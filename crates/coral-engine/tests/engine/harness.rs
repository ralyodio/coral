use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use coral_engine::{
    CoreError, HttpRuntimeBackend, HttpRuntimeCatalog, HttpRuntimeRelation, QueryExecution,
    QueryRuntimeConfig, QuerySource, RuntimeSourcePackage, StatusCode,
};
use coral_spec::backends::http::HttpSourceManifest;
use coral_spec::{SqlObjectName, parse_source_manifest_value};
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};

pub(crate) fn test_runtime() -> QueryRuntimeConfig {
    QueryRuntimeConfig::default()
}

pub(crate) fn build_source(value: Value) -> QuerySource {
    build_source_with_inputs(value, BTreeMap::new(), BTreeMap::new())
}

pub(crate) fn build_v4_http_function_source(
    value: Value,
    catalog_name: &str,
    schema_name: &str,
    sql_function_name: Option<&str>,
    authored_version: Option<&str>,
) -> QuerySource {
    let manifest = v4_http_manifest(value);
    let function = manifest.functions.first().expect("HTTP function").clone();
    let relation = HttpRuntimeRelation::try_table_function(
        SqlObjectName::new(
            catalog_name,
            schema_name,
            sql_function_name.unwrap_or(&function.name),
        ),
        function,
    )
    .expect("runtime function");
    build_v4_http_source(&manifest, catalog_name, relation, authored_version)
}

pub(crate) fn build_v4_http_table_source(
    value: Value,
    catalog_name: &str,
    schema_name: &str,
) -> QuerySource {
    let manifest = v4_http_manifest(value);
    let table = manifest.tables.first().expect("HTTP table").clone();
    let relation = HttpRuntimeRelation::try_table(
        SqlObjectName::new(catalog_name, schema_name, table.name()),
        table,
    )
    .expect("runtime table");
    build_v4_http_source(&manifest, catalog_name, relation, None)
}

fn v4_http_manifest(value: Value) -> HttpSourceManifest {
    let mut manifest = parse_source_manifest_value(value)
        .expect("HTTP manifest")
        .as_http()
        .expect("HTTP source")
        .clone();
    manifest.common.dsl_version = 4;
    manifest
}

fn build_v4_http_source(
    manifest: &HttpSourceManifest,
    catalog_name: &str,
    relation: HttpRuntimeRelation,
    authored_version: Option<&str>,
) -> QuerySource {
    let catalog = HttpRuntimeCatalog::try_new(
        catalog_name,
        HttpRuntimeBackend::from_manifest(manifest),
        vec![relation],
    )
    .expect("runtime catalog");
    QuerySource::from_runtime_catalog(
        RuntimeSourcePackage {
            source_name: catalog_name.to_string(),
            authored_version: authored_version.map(ToString::to_string),
            description: String::new(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            identity_requirements: None,
            catalog: Some(catalog.into()),
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("query source")
}

pub(crate) fn build_source_with_secrets(
    value: Value,
    secrets: impl IntoIterator<Item = (&'static str, &'static str)>,
) -> QuerySource {
    build_source_with_inputs(value, BTreeMap::new(), string_map(secrets))
}

pub(crate) fn build_source_with_inputs(
    value: Value,
    variables: BTreeMap<String, String>,
    secrets: BTreeMap<String, String>,
) -> QuerySource {
    let manifest = parse_source_manifest_value(value).expect("manifest should parse");
    QuerySource::new(manifest, variables, secrets)
}

pub(crate) fn execution_to_rows(execution: &QueryExecution) -> Vec<Value> {
    let mut bytes = Vec::new();
    {
        let mut writer = arrow::json::ArrayWriter::new(&mut bytes);
        for batch in execution.batches() {
            writer.write(batch).expect("batch should encode to json");
        }
        writer.finish().expect("json writer should finish");
    }
    serde_json::from_slice(&bytes).expect("json rows should decode")
}

pub(crate) fn assert_row_count(execution: &QueryExecution, expected: usize) {
    assert_eq!(execution.row_count(), expected);
    assert_eq!(execution_to_rows(execution).len(), expected);
}

pub(crate) fn assert_table_not_found(
    error: CoreError,
    expected_schema: &str,
    expected_table: &str,
) {
    assert_eq!(error.status_code(), StatusCode::NotFound);
    match error {
        CoreError::QueryFailure(sqe) => {
            assert_eq!(sqe.reason(), "TABLE_NOT_FOUND");
            assert_eq!(
                sqe.metadata().get("schema").map(String::as_str),
                Some(expected_schema)
            );
            assert_eq!(
                sqe.metadata().get("table").map(String::as_str),
                Some(expected_table)
            );
        }
        other => panic!("expected CoreError::QueryFailure, got {other:?}"),
    }
}

pub(crate) fn write_jsonl_file(dir: &Path, filename: &str, rows: &[Value]) {
    let path = dir.join(filename);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("jsonl parent directory should exist");
    }
    let mut data = String::new();
    for row in rows {
        data.push_str(&serde_json::to_string(row).expect("json row should serialize for fixture"));
        data.push('\n');
    }
    fs::write(path, data).expect("jsonl fixture should write");
}

pub(crate) fn write_parquet_file(dir: &Path, filename: &str, batch: &RecordBatch) {
    let path = dir.join(filename);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("parquet parent directory should exist");
    }
    let file = fs::File::create(path).expect("parquet fixture should open");
    let mut writer =
        ArrowWriter::try_new(file, batch.schema(), None).expect("parquet writer should start");
    writer.write(batch).expect("parquet batch should write");
    writer.close().expect("parquet writer should close");
}

pub(crate) fn dir_url(path: &Path) -> String {
    format!("file://{}/", path.display())
}

pub(crate) fn users_rows() -> Vec<Value> {
    vec![
        json!({"id": 1, "name": "Ada", "email": "ada@example.com"}),
        json!({"id": 2, "name": "Grace", "email": "grace@example.com"}),
        json!({"id": 3, "name": "Linus", "email": "linus@example.com"}),
    ]
}

pub(crate) fn users_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            Arc::new(StringArray::from(vec!["Ada", "Grace", "Linus"])),
            Arc::new(StringArray::from(vec![
                "ada@example.com",
                "grace@example.com",
                "linus@example.com",
            ])),
        ],
    )
    .expect("user batch should build")
}

fn string_map(
    items: impl IntoIterator<Item = (&'static str, &'static str)>,
) -> BTreeMap<String, String> {
    items
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}
