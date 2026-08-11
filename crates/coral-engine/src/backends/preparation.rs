//! Source-scoped catalog staging, decoration, and assembly.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use coral_spec::SqlObjectName;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::datasource::TableProvider;

use crate::backends::{
    CatalogRegistration, DiscoveredCatalogDraft, SourceQualifiedName, StaticCatalogDraft,
};
use crate::runtime::error::{datafusion_to_core, source_decorator_error_to_core};
use crate::runtime::schema_provider::StaticSchemaProvider;
use crate::{CoreError, QuerySource, SourceDecorator, SourceTables};

enum CatalogDraft {
    Static(StaticCatalogDraft),
    Discovered(DiscoveredCatalogDraft),
}

pub(crate) struct CatalogPreparation<'a> {
    source: &'a QuerySource,
    decorators: &'a mut [Box<dyn SourceDecorator>],
    draft: Option<CatalogDraft>,
}

impl<'a> CatalogPreparation<'a> {
    pub(crate) fn new(
        source: &'a QuerySource,
        decorators: &'a mut [Box<dyn SourceDecorator>],
    ) -> Self {
        Self {
            source,
            decorators,
            draft: None,
        }
    }

    pub(crate) fn stage_static(&mut self, draft: StaticCatalogDraft) -> Result<(), CoreError> {
        if let Some(sql_name) = draft
            .tables
            .keys()
            .find(|sql_name| sql_name.catalog_name() != draft.target.catalog_name)
        {
            return Err(CoreError::InvalidInput(format!(
                "static catalog '{}' contains table identity '{sql_name}'",
                draft.target.catalog_name
            )));
        }
        self.stage(CatalogDraft::Static(draft))
    }

    pub(crate) fn stage_discovered(
        &mut self,
        draft: DiscoveredCatalogDraft,
    ) -> Result<(), CoreError> {
        self.stage(CatalogDraft::Discovered(draft))
    }

    fn stage(&mut self, draft: CatalogDraft) -> Result<(), CoreError> {
        if self.draft.is_some() {
            return Err(CoreError::FailedPrecondition(format!(
                "source '{}' staged more than one runtime catalog",
                self.source.source_name()
            )));
        }
        self.draft = Some(draft);
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<CatalogRegistration, CoreError> {
        let draft = self.draft.take().ok_or_else(|| {
            CoreError::internal(format!(
                "source '{}' did not stage its runtime catalog",
                self.source.source_name()
            ))
        })?;
        match draft {
            CatalogDraft::Static(draft) => self.finish_static(draft),
            CatalogDraft::Discovered(draft) => self.finish_discovered(draft),
        }
    }

    fn finish_static(self, draft: StaticCatalogDraft) -> Result<CatalogRegistration, CoreError> {
        let original_identities = draft.tables.keys().cloned().collect::<BTreeSet<_>>();
        let mut tables = SourceTables::new(draft.tables);
        for decorator in self.decorators.iter_mut() {
            tables = decorator
                .decorate_source(self.source, tables)
                .map_err(|error| source_decorator_error(decorator.name(), &error))?;
            let decorated_identities = tables
                .iter()
                .map(|(name, _)| name.clone())
                .collect::<BTreeSet<_>>();
            if decorated_identities != original_identities {
                return Err(CoreError::FailedPrecondition(format!(
                    "source decorator '{}' changed source '{}' table identities",
                    decorator.name(),
                    self.source.source_name()
                )));
            }
        }

        let provider = assemble_static_provider(tables.into_inner(), &draft.source)?;
        Ok(CatalogRegistration {
            catalog_name: draft.target.catalog_name,
            publication: draft.target.publication,
            provider,
            source: draft.source,
            column_fetcher: None,
        })
    }

    fn finish_discovered(
        self,
        draft: DiscoveredCatalogDraft,
    ) -> Result<CatalogRegistration, CoreError> {
        if let Some(decorator) = self
            .decorators
            .iter()
            .find(|decorator| !decorator.supports_discovered_catalogs())
        {
            return Err(CoreError::FailedPrecondition(format!(
                "source '{}' has a discovered catalog, which source decorator '{}' does not support",
                self.source.source_name(),
                decorator.name()
            )));
        }
        Ok(CatalogRegistration {
            catalog_name: draft.target.catalog_name,
            publication: draft.target.publication,
            provider: draft.provider,
            source: draft.source,
            column_fetcher: draft.column_fetcher,
        })
    }
}

fn assemble_static_provider(
    tables: BTreeMap<SqlObjectName, Arc<dyn TableProvider>>,
    source: &crate::backends::RegisteredSource,
) -> Result<Arc<MemoryCatalogProvider>, CoreError> {
    let mut schemas = BTreeMap::<String, HashMap<String, Arc<dyn TableProvider>>>::new();
    for (sql_name, provider) in tables {
        let previous = schemas
            .entry(sql_name.schema_name().to_string())
            .or_default()
            .insert(sql_name.name().to_string(), provider);
        debug_assert!(previous.is_none(), "full SQL identities are unique");
    }
    if schemas.is_empty()
        && let SourceQualifiedName::Schema(schema_name) = &source.qualified_name
    {
        schemas.entry(schema_name.clone()).or_default();
    }

    let provider = Arc::new(MemoryCatalogProvider::new());
    for (schema_name, tables) in schemas {
        provider
            .register_schema(&schema_name, Arc::new(StaticSchemaProvider::new(tables)))
            .map_err(|error| datafusion_to_core(&error, &[]))?;
    }
    Ok(provider)
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
