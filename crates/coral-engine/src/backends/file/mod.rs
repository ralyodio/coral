//! Native file table provider backed by local files or object-store URLs.

mod error;
mod file_groups;
mod json;
mod listing;
mod metadata;
mod parquet_schema;
mod partitions;
mod provider;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;

use crate::FileRuntimeCatalog;
use crate::backends::{
    BackendCompileRequest, BackendRegistrationContext, CatalogPreparation, CatalogTarget,
    CompiledBackendCatalog, RegisteredSource, RegisteredTable, StaticCatalogDraft,
    build_registered_inputs, build_registered_table, registered_columns_from_schema,
    registered_columns_from_specs, required_filter_names,
    validate_lookup_key_filter_backend_support,
};
use crate::runtime::error::datafusion_to_core;
use coral_spec::SourceBackend;
#[cfg(test)]
use coral_spec::backends::file::FileSourceManifest;
use coral_spec::backends::file::{FileFormat, FileTableSpec};

use self::json::JsonFileTableProvider;
use self::provider::FileTableProvider;

#[derive(Debug, Clone)]
struct FileCompiledSource {
    source_name: String,
    catalog: FileRuntimeCatalog,
    declared_inputs: Vec<coral_spec::ManifestInputSpec>,
    home_dir: Option<PathBuf>,
    source_secrets: BTreeMap<String, String>,
    source_variables: BTreeMap<String, String>,
}

pub(crate) fn compile_runtime_catalog(
    catalog: &FileRuntimeCatalog,
    request: &BackendCompileRequest<'_>,
) -> Box<dyn CompiledBackendCatalog> {
    Box::new(FileCompiledSource {
        source_name: request.source.source_name().to_string(),
        catalog: catalog.clone(),
        declared_inputs: request.source.declared_inputs().to_vec(),
        home_dir: request.runtime_context.home_dir.clone(),
        source_secrets: request.source_secrets.clone(),
        source_variables: request.source_variables.clone(),
    })
}

#[cfg(test)]
pub(crate) fn compile_manifest(
    manifest: &FileSourceManifest,
    request: &BackendCompileRequest<'_>,
) -> Box<dyn CompiledBackendCatalog> {
    let catalog = FileRuntimeCatalog::try_from_default_catalog_manifest(manifest.clone())
        .expect("validated file manifest produces a valid runtime catalog");
    compile_runtime_catalog(&catalog, request)
}

#[async_trait]
impl CompiledBackendCatalog for FileCompiledSource {
    async fn stage(
        &self,
        ctx: &SessionContext,
        _registration: &BackendRegistrationContext,
        preparation: &mut CatalogPreparation<'_>,
    ) -> std::result::Result<(), crate::CoreError> {
        let draft = self
            .build_static_draft(ctx)
            .await
            .map_err(|error| datafusion_to_core(&error, &[]))?;
        preparation.stage_static(draft)
    }
}

impl FileCompiledSource {
    async fn build_static_draft(&self, ctx: &SessionContext) -> Result<StaticCatalogDraft> {
        validate_lookup_key_filter_backend_support(
            &self.source_name,
            SourceBackend::File,
            self.catalog
                .relations()
                .iter()
                .map(crate::FileRuntimeRelation::definition)
                .flat_map(FileTableSpec::filters)
                .any(|filter| filter.lookup_key),
        )?;
        let mut tables: BTreeMap<coral_spec::SqlObjectName, Arc<dyn TableProvider>> =
            BTreeMap::new();
        let mut table_infos = Vec::with_capacity(self.catalog.relations().len());
        let resolved_inputs = coral_spec::resolve_inputs(
            &self.declared_inputs,
            &self.source_secrets,
            &self.source_variables,
        );

        for relation in self.catalog.relations() {
            let table = relation.definition();
            let provider: Arc<dyn TableProvider> = match table.format {
                FileFormat::Jsonl | FileFormat::Json if json::requires_custom_provider(table)? => {
                    Arc::new(
                        JsonFileTableProvider::try_new_async(
                            ctx,
                            &self.source_name,
                            table.clone(),
                            self.home_dir.as_deref(),
                            &resolved_inputs,
                        )
                        .await?,
                    )
                }
                FileFormat::Parquet | FileFormat::Csv | FileFormat::Jsonl | FileFormat::Json => {
                    Arc::new(
                        FileTableProvider::try_new_async(
                            ctx,
                            &self.source_name,
                            table.clone(),
                            self.home_dir.as_deref(),
                            &resolved_inputs,
                        )
                        .await?,
                    )
                }
            };
            let schema = provider.schema();
            let metadata = registered_table(relation.sql_name().clone(), table, &schema);
            tables.insert(relation.sql_name().clone(), provider);
            table_infos.push(metadata);
        }

        let secret_keys = self.source_secrets.keys().cloned().collect();
        let inputs =
            build_registered_inputs(&self.declared_inputs, &self.source_variables, &secret_keys);

        let target = CatalogTarget::new(self.catalog.catalog_name());
        let qualified_name = target.source_qualified_name(&self.source_name);
        Ok(StaticCatalogDraft {
            target,
            tables,
            source: RegisteredSource {
                source_name: self.source_name.clone(),
                qualified_name,
                tables: table_infos,
                table_functions: vec![],
                inputs,
            },
        })
    }
}

fn registered_table(
    sql_name: coral_spec::SqlObjectName,
    table: &FileTableSpec,
    inferred_schema: &SchemaRef,
) -> RegisteredTable {
    let filters = table.filters();
    let required_filters = required_filter_names(filters);
    let columns = if table.columns().is_empty() {
        registered_columns_from_schema(inferred_schema, filters)
    } else {
        let mut columns = registered_columns_from_specs(table.columns(), filters);
        let declared_names = table
            .columns()
            .iter()
            .map(|column| column.name.as_str())
            .collect::<HashSet<_>>();
        columns.extend(
            registered_columns_from_schema(inferred_schema, filters)
                .into_iter()
                .filter(|column| !declared_names.contains(column.name.as_str())),
        );
        columns
    };

    build_registered_table(sql_name, &table.common, columns, required_filters)
}
