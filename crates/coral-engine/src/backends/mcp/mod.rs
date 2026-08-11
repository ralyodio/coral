//! MCP-backed source runtime pieces.

mod catalog;
mod client;
pub(crate) mod error;
mod fetch;
mod function;
mod provider;
mod response;
mod trace;
mod transport;

pub(crate) use error::McpProviderQueryError;

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use coral_spec::backends::mcp::McpServerSpec;
#[cfg(test)]
use coral_spec::backends::mcp::McpSourceManifest;
use coral_spec::v4::McpToolCatalog;
use coral_spec::{ManifestInputSpec, SourceBackend, resolve_inputs};
use datafusion::datasource::TableProvider;
use datafusion::error::Result;

use self::client::{McpSourceClient, McpToolCaller};
use self::function::McpSourceTableFunction;
use self::provider::McpTableProvider;
use self::transport::{StdioMcpToolCaller, StreamableHttpMcpToolCaller};
use crate::backends::shared::source_observation::{
    SourceObservationPublishers, source_observation_publishers,
};
use crate::backends::{
    BackendCompileRequest, BackendRegistrationContext, CatalogPreparation, CatalogTarget,
    CompiledBackendCatalog, RegisteredSource, SourceFunctionProviderFactory, StaticCatalogDraft,
    build_registered_inputs, build_registered_table, build_registered_table_function,
    registered_columns_from_specs, required_filter_names,
    validate_lookup_key_filter_backend_support,
};
use crate::runtime::error::datafusion_to_core;
use crate::{
    CoreError, McpRuntimeCatalog, SourceInputResolutionContext, SourceInputResolver,
    SourceInputResolverError,
};

#[derive(Clone)]
struct McpCompiledSource {
    source_name: String,
    catalog: McpRuntimeCatalog,
    source_input_resolution: SourceInputResolutionContext,
    source_inputs: Arc<McpSourceInputs>,
    caller: McpSourceClient,
    source_observation_publishers: SourceObservationPublishers,
}

struct McpCompiledSourceConfig {
    source_name: String,
    catalog: McpRuntimeCatalog,
    source_input_resolution: SourceInputResolutionContext,
    source_inputs: Arc<McpSourceInputs>,
}

#[derive(Debug, Clone)]
struct McpSourceInputs {
    fallback: Arc<BTreeMap<String, String>>,
    source: Option<SourceInputResolutionContext>,
    resolver: Option<Arc<dyn SourceInputResolver>>,
}

impl McpSourceInputs {
    fn with_resolver(
        fallback: Arc<BTreeMap<String, String>>,
        source: SourceInputResolutionContext,
        resolver: Arc<dyn SourceInputResolver>,
    ) -> Self {
        Self {
            fallback,
            source: Some(source),
            resolver: Some(resolver),
        }
    }

    pub(super) fn static_inputs(fallback: Arc<BTreeMap<String, String>>) -> Self {
        Self {
            fallback,
            source: None,
            resolver: None,
        }
    }

    async fn resolve_for_request(&self) -> Result<Arc<BTreeMap<String, String>>> {
        let (Some(resolver), Some(source)) = (&self.resolver, &self.source) else {
            return Ok(Arc::clone(&self.fallback));
        };
        resolver
            .resolve_inputs(source)
            .await
            .map(Arc::new)
            .map_err(source_input_error)
    }
}

#[cfg(test)]
pub(crate) fn compile_manifest(
    manifest: &McpSourceManifest,
    request: &BackendCompileRequest<'_>,
) -> Box<dyn CompiledBackendCatalog> {
    let catalog = McpRuntimeCatalog::try_from_default_catalog_manifest(manifest.clone())
        .expect("validated MCP manifest produces a valid runtime catalog");
    compile_runtime_catalog(&catalog, request)
}

pub(crate) fn compile_runtime_catalog(
    catalog: &McpRuntimeCatalog,
    request: &BackendCompileRequest<'_>,
) -> Box<dyn CompiledBackendCatalog> {
    let source_input_resolution = SourceInputResolutionContext::from_query_source(request.source);
    let resolved_inputs = Arc::new(coral_spec::resolve_inputs(
        request.source.declared_inputs(),
        source_input_resolution.secrets(),
        source_input_resolution.variables(),
    ));
    let source_inputs = Arc::new(match request.source_input_resolver.clone() {
        Some(resolver) => McpSourceInputs::with_resolver(
            Arc::clone(&resolved_inputs),
            source_input_resolution.clone(),
            resolver,
        ),
        None => McpSourceInputs::static_inputs(Arc::clone(&resolved_inputs)),
    });
    let body_capture =
        self::trace::McpBodyCapture::new(request.runtime_context.body_capture_max_bytes);
    let server = &catalog.backend().server;
    let caller: Arc<dyn McpToolCaller> = match server {
        McpServerSpec::Stdio { .. } => Arc::new(StdioMcpToolCaller {
            source_name: request.source.source_name().to_string(),
            server: server.clone(),
            source_inputs: Arc::clone(&source_inputs),
            body_capture,
        }),
        McpServerSpec::StreamableHttp { .. } => Arc::new(StreamableHttpMcpToolCaller {
            source_name: request.source.source_name().to_string(),
            server: server.clone(),
            source_inputs: Arc::clone(&source_inputs),
            body_capture,
        }),
    };
    compile_source_with_caller(
        McpCompiledSourceConfig {
            source_name: request.source.source_name().to_string(),
            catalog: catalog.clone(),
            source_input_resolution,
            source_inputs,
        },
        caller,
        source_observation_publishers(request.source_observation_publishers),
    )
}

/// Connects to an MCP server and returns its declared tool catalog.
///
/// This is used by DSL v4 materialization to snapshot MCP `tools/list`
/// metadata into app-owned artifacts before query runtime assembly.
///
/// # Errors
///
/// Returns [`CoreError`] when source inputs cannot be resolved, the MCP server
/// cannot be initialized, or tool catalog discovery fails.
pub async fn discover_tool_catalog(
    source_name: &str,
    server: McpServerSpec,
    declared_inputs: &[ManifestInputSpec],
    source_variables: BTreeMap<String, String>,
    source_secrets: BTreeMap<String, String>,
) -> std::result::Result<McpToolCatalog, CoreError> {
    let resolved_inputs = Arc::new(resolve_inputs(
        declared_inputs,
        &source_secrets,
        &source_variables,
    ));
    let source_inputs = Arc::new(McpSourceInputs::static_inputs(resolved_inputs));
    catalog::inspect_tools(source_name.to_string(), server, source_inputs)
        .await
        .map_err(|error| datafusion_to_core(&error, &[]))
}

fn compile_source_with_caller(
    config: McpCompiledSourceConfig,
    caller: Arc<dyn McpToolCaller>,
    source_observation_publishers: SourceObservationPublishers,
) -> Box<dyn CompiledBackendCatalog> {
    Box::new(McpCompiledSource {
        source_name: config.source_name,
        catalog: config.catalog,
        source_input_resolution: config.source_input_resolution,
        source_inputs: config.source_inputs,
        caller: McpSourceClient::new(caller),
        source_observation_publishers,
    })
}

#[async_trait]
impl CompiledBackendCatalog for McpCompiledSource {
    async fn stage(
        &self,
        _ctx: &datafusion::prelude::SessionContext,
        _registration: &BackendRegistrationContext,
        preparation: &mut CatalogPreparation<'_>,
    ) -> std::result::Result<(), CoreError> {
        let draft = self
            .build_static_draft()
            .map_err(|error| datafusion_to_core(&error, &[]))?;
        preparation.stage_static(draft)
    }
}

impl McpCompiledSource {
    fn build_static_draft(&self) -> Result<StaticCatalogDraft> {
        validate_lookup_key_filter_backend_support(
            &self.source_name,
            SourceBackend::Mcp,
            self.catalog
                .table_relations()
                .flat_map(|(_, table)| table.filters())
                .any(|filter| filter.lookup_key),
        )?;
        let mut table_function_infos = Vec::new();

        for (sql_name, function) in self.catalog.function_relations() {
            let factory: Arc<dyn SourceFunctionProviderFactory> =
                Arc::new(McpSourceTableFunction::new(
                    self.caller.clone(),
                    sql_name.clone(),
                    function.clone(),
                    Arc::clone(&self.source_observation_publishers),
                )?);
            table_function_infos.push(build_registered_table_function(
                sql_name.clone(),
                &function.common,
                factory,
            ));
        }

        let mut tables: BTreeMap<coral_spec::SqlObjectName, Arc<dyn TableProvider>> =
            BTreeMap::new();
        let mut table_infos = Vec::new();
        for (sql_name, table) in self.catalog.table_relations() {
            let provider: Arc<dyn TableProvider> = Arc::new(McpTableProvider::new(
                self.caller.clone(),
                sql_name.clone(),
                Arc::clone(&self.source_inputs),
                table.clone(),
                Arc::clone(&self.source_observation_publishers),
            )?);
            tables.insert(sql_name.clone(), provider);
            let required_filters = required_filter_names(table.filters());
            let columns = registered_columns_from_specs(table.columns(), table.filters());
            table_infos.push(build_registered_table(
                sql_name.clone(),
                &table.common,
                columns,
                required_filters,
            ));
        }

        let secret_keys = self
            .source_input_resolution
            .secrets()
            .keys()
            .cloned()
            .collect();
        let inputs = build_registered_inputs(
            self.source_input_resolution.declared_inputs(),
            self.source_input_resolution.variables(),
            &secret_keys,
        );

        let target = CatalogTarget::new(self.catalog.catalog_name());
        let qualified_name = target.source_qualified_name(&self.source_name);
        Ok(StaticCatalogDraft {
            target,
            tables,
            source: RegisteredSource {
                source_name: self.source_name.clone(),
                qualified_name,
                tables: table_infos,
                table_functions: table_function_infos,
                inputs,
            },
        })
    }
}

fn source_input_error(error: SourceInputResolverError) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(error))
}

#[cfg(test)]
mod tests;
