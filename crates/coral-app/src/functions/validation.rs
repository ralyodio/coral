use std::collections::{BTreeSet, HashSet};

use coral_engine::{QuerySource, RuntimeCatalog, StaticRuntimeCatalog, UdfRuntimeDefinition};

use crate::bootstrap::AppError;

pub(crate) type SqlPublishTargets = HashSet<SqlPublishTarget>;

pub(crate) fn initial_sql_publish_targets(selected_sources: &[QuerySource]) -> SqlPublishTargets {
    source_sql_publish_targets(selected_sources)
}

pub(crate) fn record_sql_publish_target(
    function: &UdfRuntimeDefinition,
    publish_targets: &mut SqlPublishTargets,
) -> Result<(), AppError> {
    let target = SqlPublishTarget::new(
        &function.publish.table_function.schema,
        &function.publish.table_function.name,
    );
    if !publish_targets.insert(target.clone()) {
        return Err(AppError::FailedPrecondition(format!(
            "function publish target '{}' is installed more than once",
            target.display_name()
        )));
    }
    Ok(())
}

pub(crate) fn source_sql_publish_targets_for_schemas(
    selected_sources: &[QuerySource],
    schemas: &BTreeSet<String>,
) -> SqlPublishTargets {
    let mut targets = HashSet::new();
    for source in selected_sources {
        if let Some(catalog) = source.catalog() {
            record_source_catalog_sql_targets(catalog, Some(schemas), &mut targets);
        }
    }
    targets
}

pub(crate) fn unchecked_source_publish_schemas(
    function: &UdfRuntimeDefinition,
    checked: &BTreeSet<String>,
) -> BTreeSet<String> {
    let schema = &function.publish.table_function.schema;
    if checked.contains(schema) {
        BTreeSet::new()
    } else {
        BTreeSet::from([schema.clone()])
    }
}

fn source_sql_publish_targets(selected_sources: &[QuerySource]) -> SqlPublishTargets {
    let mut targets = HashSet::new();
    for source in selected_sources {
        if let Some(catalog) = source.catalog() {
            record_source_catalog_sql_targets(catalog, None, &mut targets);
        }
    }
    targets
}

fn record_source_catalog_sql_targets(
    catalog: &RuntimeCatalog,
    schemas: Option<&BTreeSet<String>>,
    targets: &mut SqlPublishTargets,
) {
    let mut record = |schema: &str, name: &str| {
        if schemas.is_none_or(|schemas| schemas.contains(schema)) {
            targets.insert(SqlPublishTarget::new(schema, name));
        }
    };
    match catalog {
        RuntimeCatalog::Discovered(_) => {
            // Database tables are discovered at registration and have no static publish targets.
        }
        RuntimeCatalog::Static(StaticRuntimeCatalog::Http(catalog)) => {
            for relation in catalog.relations() {
                let sql_name = relation.sql_name();
                record(sql_name.schema_name(), sql_name.name());
            }
        }
        RuntimeCatalog::Static(StaticRuntimeCatalog::File(catalog)) => {
            for relation in catalog.relations() {
                let sql_name = relation.sql_name();
                record(sql_name.schema_name(), sql_name.name());
            }
        }
        RuntimeCatalog::Static(StaticRuntimeCatalog::Mcp(catalog)) => {
            for relation in catalog.relations() {
                let sql_name = relation.sql_name();
                record(sql_name.schema_name(), sql_name.name());
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SqlPublishTarget {
    schema: String,
    name: String,
}

impl SqlPublishTarget {
    fn new(schema: &str, name: &str) -> Self {
        Self {
            schema: schema.to_ascii_lowercase(),
            name: name.to_ascii_lowercase(),
        }
    }

    fn display_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use coral_engine::{
        UdfRuntimeImplementation, UdfRuntimePublish, UdfRuntimeTableFunctionPublish,
    };
    use coral_spec::parse_source_manifest_yaml;

    use super::*;

    fn functions_source() -> QuerySource {
        http_source("functions", "review_queue")
    }

    fn http_source(schema: &str, table: &str) -> QuerySource {
        let manifest = parse_source_manifest_yaml(&format!(
            r"
name: {schema}
version: 0.1.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: {table}
    description: Existing table
    request:
      method: GET
      path: /{table}
    response: {{}}
    columns:
      - name: id
        type: Int64
"
        ))
        .expect("source manifest");
        QuerySource::new(manifest, BTreeMap::new(), BTreeMap::new())
    }

    fn runtime_function() -> UdfRuntimeDefinition {
        UdfRuntimeDefinition {
            name: "review_queue".to_string(),
            description: String::new(),
            arguments: Vec::new(),
            implementation: UdfRuntimeImplementation::CoralSql {
                query: "select 1 as id".to_string(),
            },
            publish: UdfRuntimePublish {
                table_function: UdfRuntimeTableFunctionPublish {
                    schema: "functions".to_string(),
                    name: "review_queue".to_string(),
                    description: String::new(),
                    guide: String::new(),
                },
            },
            result_columns: Vec::new(),
            source_names: Vec::new(),
        }
    }

    #[test]
    fn functions_schema_still_checks_source_publish_targets() {
        let targets = initial_sql_publish_targets(&[functions_source()]);

        assert!(targets.contains(&SqlPublishTarget::new("functions", "review_queue")));
    }

    #[test]
    fn functions_schema_is_not_treated_as_prechecked() {
        assert_eq!(
            unchecked_source_publish_schemas(&runtime_function(), &BTreeSet::new()),
            BTreeSet::from(["functions".to_string()])
        );
    }
}
