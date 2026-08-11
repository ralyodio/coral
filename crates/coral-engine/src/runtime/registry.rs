//! Registers compiled backend sources into a shared `DataFusion` session.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::CatalogProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::prelude::SessionContext;
use tracing::{Instrument as _, info_span};

use crate::backends::{
    BackendRegistrationContext, CatalogColumnFetcher, CatalogPreparation, CatalogPublication,
    CatalogRegistration, CompiledBackendCatalog, RegisteredSource,
};
use crate::runtime::error::{datafusion_to_core, source_decorator_error_to_core};
use crate::{CoreError, QuerySource, SourceDecorator, SourceFailurePolicy};

/// Source SQL names the runtime owns. Mirrored by `RESERVED_SOURCE_SCHEMA_NAMES`
/// in `coral-spec`, which rejects the same names during manifest validation so a
/// source cannot pass validation and then fail here.
const RESERVED_SCHEMA_NAMES: &[&str] = &["coral", "coral_admin", "datafusion", "public"];

/// One selected query source together with its compiled backend artifact.
///
/// The registry needs both values at once: the compiled backend source drives
/// registration, while the original `QuerySource` is what source decorators
/// reason about during prepare, decoration, and failure handling.
pub(crate) struct CompiledQuerySource {
    pub(crate) source: QuerySource,
    pub(crate) compiled: Box<dyn CompiledBackendCatalog>,
}

/// One selected source's readiness for runtime registration.
pub(crate) enum SourceRegistrationCandidate {
    Compiled(CompiledQuerySource),
    CompileFailed {
        source: QuerySource,
        error: CoreError,
    },
}

impl SourceRegistrationCandidate {
    fn source(&self) -> &QuerySource {
        match self {
            Self::Compiled(compiled) => &compiled.source,
            Self::CompileFailed { source, .. } => source,
        }
    }
}

/// Captures one source manifest that failed to initialize during registration.
#[derive(Debug, Clone)]
pub(crate) struct SourceRegistrationFailure {
    /// Schema name whose registration failed.
    pub schema_name: String,
    /// Human-readable failure detail.
    pub detail: String,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SourceRegistrationResult {
    pub(crate) active_sources: Vec<RegisteredSource>,
    pub(crate) column_fetchers: Vec<CatalogColumnFetcher>,
    pub(crate) failures: Vec<SourceRegistrationFailure>,
}

fn check_reserved_schema(schema: &str) -> DataFusionResult<()> {
    if is_reserved_schema(schema) {
        return Err(DataFusionError::Execution(reserved_schema_detail(schema)));
    }
    Ok(())
}

fn is_reserved_schema(schema: &str) -> bool {
    // Case-insensitive so this agrees with `non_default_catalog_name`, which
    // folds the default catalog the same way: `DataFusion` must not register as
    // a source while catalog filters read that spelling as the default catalog.
    RESERVED_SCHEMA_NAMES
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(schema))
}

fn reserved_schema_detail(schema: &str) -> String {
    format!("source SQL name '{schema}' is reserved and cannot be used by manifests")
}

/// Register all configured source manifests into the active `SessionContext`.
///
/// # Errors
///
/// Returns a `DataFusionError` if the catalog is missing or if the source list
/// itself cannot be processed. Individual source registration failures are
/// logged and skipped so the remaining sources can still be registered.
pub(crate) async fn register_sources(
    ctx: &SessionContext,
    sources: Vec<SourceRegistrationCandidate>,
    source_decorators: &mut [Box<dyn SourceDecorator>],
) -> std::result::Result<SourceRegistrationResult, CoreError> {
    let span = info_span!(
        "coral.engine.sources.register",
        source.count = sources.len(),
    );
    register_sources_inner(ctx, sources, source_decorators)
        .instrument(span)
        .await
}

async fn register_sources_inner(
    ctx: &SessionContext,
    sources: Vec<SourceRegistrationCandidate>,
    source_decorators: &mut [Box<dyn SourceDecorator>],
) -> std::result::Result<SourceRegistrationResult, CoreError> {
    let catalog = ctx.catalog("datafusion").ok_or_else(|| {
        let plan_err = DataFusionError::Plan("catalog 'datafusion' not found".to_string());
        datafusion_to_core(&plan_err, &[])
    })?;

    let selected_sources = sources
        .iter()
        .map(|selected| selected.source().clone())
        .collect::<Vec<_>>();
    validate_selected_source_names(&selected_sources)?;
    prepare_source_decorators(source_decorators, &selected_sources)?;

    let mut result = SourceRegistrationResult::default();
    let mut seen_schemas = catalog.schema_names().into_iter().collect();
    let mut seen_catalogs = ctx.catalog_names().into_iter().collect();
    let registration_context = BackendRegistrationContext::default();

    for source in sources {
        match source {
            SourceRegistrationCandidate::Compiled(selected_source) => {
                let query_source = &selected_source.source;
                let compiled_source = selected_source.compiled;
                let source_name = query_source.source_name().to_string();
                let mut failed_sql_name = source_name.clone();

                let preparation = prepare_source(
                    ctx,
                    &registration_context,
                    compiled_source.as_ref(),
                    query_source,
                    source_decorators,
                )
                .await;
                let registration = match preparation {
                    Ok(registration) => {
                        failed_sql_name = registration.source.qualified_name.name().to_string();
                        publish_catalog_registrations(
                            ctx,
                            &mut seen_schemas,
                            &mut seen_catalogs,
                            vec![registration],
                            &mut result,
                        )
                    }
                    Err(error) => Err(error),
                };
                if let Err(core_error) = registration {
                    if handle_source_registration_failure(
                        source_decorators,
                        query_source,
                        &core_error,
                    )? {
                        return Err(core_error);
                    }
                    push_source_failure(
                        &mut result,
                        &source_name,
                        &failed_sql_name,
                        core_error.to_string(),
                    );
                }
            }
            SourceRegistrationCandidate::CompileFailed { source, error } => {
                if handle_source_registration_failure(source_decorators, &source, &error)? {
                    return Err(error);
                }
                push_source_failure(
                    &mut result,
                    source.source_name(),
                    source.source_name(),
                    error.to_string(),
                );
            }
        }
    }

    finish_source_decorators(source_decorators)?;

    Ok(result)
}

fn validate_selected_source_names(sources: &[QuerySource]) -> std::result::Result<(), CoreError> {
    let mut owner_by_name = HashMap::new();
    for source in sources {
        let names = source
            .schema_names()
            .into_iter()
            .map(|name| (name, "schema"))
            .chain(
                source
                    .catalog_names()
                    .into_iter()
                    .map(|name| (name, "catalog")),
            );
        for (name, kind) in names {
            if is_reserved_schema(name) {
                return Err(CoreError::InvalidInput(reserved_schema_detail(name)));
            }
            if let Some(existing_source) =
                owner_by_name.insert(name.to_string(), source.source_name().to_string())
            {
                return Err(CoreError::InvalidInput(format!(
                    "source '{}' runtime {kind} name '{name}' conflicts with selected source '{existing_source}'",
                    source.source_name()
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn register_sources_blocking(
    ctx: &SessionContext,
    sources: Vec<CompiledQuerySource>,
) -> std::result::Result<SourceRegistrationResult, CoreError> {
    let mut source_decorators: Vec<Box<dyn SourceDecorator>> = Vec::new();
    futures::executor::block_on(register_sources(
        ctx,
        sources
            .into_iter()
            .map(SourceRegistrationCandidate::Compiled)
            .collect(),
        source_decorators.as_mut_slice(),
    ))
}

async fn prepare_source(
    ctx: &SessionContext,
    registration_context: &BackendRegistrationContext,
    compiled: &dyn CompiledBackendCatalog,
    source: &QuerySource,
    source_decorators: &mut [Box<dyn SourceDecorator>],
) -> Result<CatalogRegistration, CoreError> {
    let mut preparation = CatalogPreparation::new(source, source_decorators);
    compiled
        .stage(ctx, registration_context, &mut preparation)
        .await?;
    preparation.finish()
}

fn publish_catalog_registrations(
    ctx: &SessionContext,
    seen_schemas: &mut std::collections::HashSet<String>,
    seen_catalogs: &mut std::collections::HashSet<String>,
    registrations: Vec<CatalogRegistration>,
    result: &mut SourceRegistrationResult,
) -> Result<(), CoreError> {
    let (new_schemas, new_catalogs) =
        validate_catalog_registrations(ctx, seen_schemas, seen_catalogs, &registrations)?;

    let mut published_schemas = Vec::<(Arc<dyn CatalogProvider>, String)>::new();
    for registration in registrations
        .iter()
        .filter(|registration| registration.publication == CatalogPublication::ExtendExisting)
    {
        let target = ctx.catalog(&registration.catalog_name).ok_or_else(|| {
            CoreError::FailedPrecondition(format!(
                "catalog '{}' is not installed",
                registration.catalog_name
            ))
        })?;
        for schema_name in registration.provider.schema_names() {
            let schema = registration.provider.schema(&schema_name).ok_or_else(|| {
                CoreError::internal(format!(
                    "prepared catalog '{}' omitted schema '{schema_name}'",
                    registration.catalog_name
                ))
            })?;
            if let Err(error) = target.register_schema(&schema_name, schema) {
                rollback_registered_schemas(&published_schemas);
                return Err(datafusion_to_core(&error, &[]));
            }
            published_schemas.push((Arc::clone(&target), schema_name));
        }
    }
    for registration in registrations
        .iter()
        .filter(|registration| registration.publication == CatalogPublication::InstallNew)
    {
        ctx.register_catalog(
            &registration.catalog_name,
            Arc::clone(&registration.provider),
        );
    }

    seen_schemas.extend(new_schemas);
    seen_catalogs.extend(new_catalogs);
    for registration in registrations {
        if let Some(column_fetcher) = registration.column_fetcher {
            result.column_fetchers.push(CatalogColumnFetcher {
                catalog_name: registration.catalog_name,
                relation_names: registration
                    .source
                    .tables
                    .iter()
                    .map(|table| {
                        (
                            table.sql_name.schema_name().to_string(),
                            table.sql_name.name().to_string(),
                        )
                    })
                    .collect(),
                fetcher: column_fetcher,
            });
        }
        result.active_sources.push(registration.source);
    }
    Ok(())
}

fn validate_catalog_registrations(
    ctx: &SessionContext,
    seen_schemas: &std::collections::HashSet<String>,
    seen_catalogs: &std::collections::HashSet<String>,
    registrations: &[CatalogRegistration],
) -> Result<
    (
        std::collections::HashSet<String>,
        std::collections::HashSet<String>,
    ),
    CoreError,
> {
    let mut new_schemas = std::collections::HashSet::new();
    let mut new_catalogs = std::collections::HashSet::new();
    for registration in registrations {
        match registration.publication {
            CatalogPublication::ExtendExisting => {
                if ctx.catalog(&registration.catalog_name).is_none() {
                    return Err(CoreError::FailedPrecondition(format!(
                        "catalog '{}' is not installed",
                        registration.catalog_name
                    )));
                }
                for schema_name in registration.provider.schema_names() {
                    check_reserved_schema(&schema_name)
                        .map_err(|error| datafusion_to_core(&error, &[]))?;
                    if seen_schemas.contains(&schema_name)
                        || !new_schemas.insert(schema_name.clone())
                    {
                        return Err(CoreError::FailedPrecondition(format!(
                            "duplicate source schema '{schema_name}'"
                        )));
                    }
                }
            }
            CatalogPublication::InstallNew => {
                check_reserved_schema(&registration.catalog_name)
                    .map_err(|error| datafusion_to_core(&error, &[]))?;
                if seen_catalogs.contains(&registration.catalog_name)
                    || !new_catalogs.insert(registration.catalog_name.clone())
                {
                    return Err(CoreError::FailedPrecondition(format!(
                        "duplicate source catalog '{}'",
                        registration.catalog_name
                    )));
                }
            }
        }
    }
    Ok((new_schemas, new_catalogs))
}

fn rollback_registered_schemas(published: &[(Arc<dyn CatalogProvider>, String)]) {
    for (catalog, schema_name) in published.iter().rev() {
        if let Err(error) = catalog.deregister_schema(schema_name, true) {
            tracing::warn!(
                schema_name,
                detail = %error,
                "failed to roll back source schema registration"
            );
        }
    }
}

fn push_source_failure(
    result: &mut SourceRegistrationResult,
    source_name: &str,
    schema_name: &str,
    detail: String,
) {
    let failure = SourceRegistrationFailure {
        schema_name: schema_name.to_string(),
        detail,
    };
    tracing::warn!(
        source = %source_name,
        schema_name = %failure.schema_name,
        detail = %failure.detail,
        "skipping source"
    );
    result.failures.push(failure);
}

fn prepare_source_decorators(
    source_decorators: &mut [Box<dyn SourceDecorator>],
    selected_sources: &[QuerySource],
) -> std::result::Result<(), CoreError> {
    for decorator in source_decorators {
        decorator
            .prepare(selected_sources)
            .map_err(|error| source_decorator_error(decorator.name(), &error))?;
    }
    Ok(())
}

fn handle_source_registration_failure(
    source_decorators: &mut [Box<dyn SourceDecorator>],
    source: &QuerySource,
    error: &CoreError,
) -> std::result::Result<bool, CoreError> {
    for decorator in source_decorators {
        let policy = decorator
            .source_failed(source, error)
            .map_err(|decorator_error| {
                source_decorator_error(decorator.name(), &decorator_error)
            })?;
        if policy == SourceFailurePolicy::Abort {
            return Ok(true);
        }
    }
    Ok(false)
}

fn finish_source_decorators(
    source_decorators: &mut [Box<dyn SourceDecorator>],
) -> std::result::Result<(), CoreError> {
    for decorator in source_decorators {
        decorator
            .finish()
            .map_err(|error| source_decorator_error(decorator.name(), &error))?;
    }
    Ok(())
}

fn source_decorator_error(name: &str, error: &crate::SourceDecoratorError) -> CoreError {
    let core = source_decorator_error_to_core(error);
    match core {
        CoreError::InvalidInput(detail) => {
            CoreError::InvalidInput(format!("source decorator '{name}': {detail}"))
        }
        CoreError::FailedPrecondition(detail) => {
            CoreError::FailedPrecondition(format!("source decorator '{name}': {detail}"))
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use coral_spec::{SqlObjectName, parse_source_manifest_yaml};

    use crate::{
        CoreError, HttpRuntimeBackend, HttpRuntimeCatalog, HttpRuntimeRelation, QuerySource,
        RuntimeSourcePackage,
    };

    use super::{check_reserved_schema, validate_selected_source_names};

    #[test]
    fn reserved_schema_coral_is_rejected() {
        let result = check_reserved_schema("coral");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("coral"),
            "error message should mention the schema name"
        );
    }

    #[test]
    fn reserved_schema_public_is_rejected() {
        let result = check_reserved_schema("public");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("public"),
            "error message should mention the schema name"
        );
    }

    #[test]
    fn reserved_schema_datafusion_is_rejected() {
        let result = check_reserved_schema("datafusion");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("datafusion"),
            "error message should mention the schema name"
        );
    }

    #[test]
    fn reserved_schema_rejection_ignores_case() {
        // `non_default_catalog_name` folds the default catalog
        // case-insensitively, so an exact-match reservation would let
        // `DataFusion` register and then be read as the default catalog by
        // every catalog filter. Legacy manifests are not restricted to
        // lowercase names, so this is reachable.
        check_reserved_schema("DataFusion").expect_err("DataFusion is reserved");
        check_reserved_schema("Coral").expect_err("Coral is reserved");
        check_reserved_schema("PUBLIC").expect_err("PUBLIC is reserved");
    }

    #[test]
    fn non_reserved_schema_is_accepted() {
        check_reserved_schema("github").expect("github is not reserved");
        check_reserved_schema("pagerduty").expect("pagerduty is not reserved");
        check_reserved_schema("slack").expect("slack is not reserved");
    }

    #[test]
    fn selected_sources_reject_reserved_schema_before_backend_registration() {
        let manifest = parse_source_manifest_yaml(
            "name: github\nversion: 0.1.0\ndsl_version: 3\nbackend: http\nbase_url: https://example.com\ntables:\n  - name: users\n    description: Users\n    request:\n      path: /users\n    columns:\n      - name: id\n        type: Int64\n",
        )
        .expect("source manifest");
        let manifest = manifest.as_http().expect("HTTP source");
        let relation = HttpRuntimeRelation::try_table(
            SqlObjectName::new("datafusion", "public", "users"),
            manifest.tables.first().expect("one table").clone(),
        )
        .expect("runtime relation");
        let catalog = HttpRuntimeCatalog::try_new(
            "datafusion",
            HttpRuntimeBackend::from_manifest(manifest),
            vec![relation],
        )
        .expect("runtime catalog");
        let source = QuerySource::from_runtime_catalog(
            RuntimeSourcePackage {
                source_name: "github".to_string(),
                authored_version: None,
                description: String::new(),
                declared_inputs: Vec::new(),
                test_queries: Vec::new(),
                identity_requirements: None,
                catalog: Some(catalog.into()),
            },
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .expect("query source");

        let error = validate_selected_source_names(&[source])
            .expect_err("reserved source schema should fail selected-source preflight");

        let CoreError::InvalidInput(detail) = error else {
            panic!("expected invalid input, got {error:?}");
        };
        assert!(
            detail.contains("source SQL name 'public' is reserved"),
            "unexpected error: {detail}"
        );
    }
}
