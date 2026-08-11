use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;
use coral_engine::{
    BoundRequestIdentityHttpAuthenticator, CoralQuery, DatabaseRuntimeBackend,
    DatabaseRuntimeCatalog, EngineExtensions, FileRuntimeCatalog, HttpRuntimeBackend,
    HttpRuntimeCatalog, HttpRuntimeRelation, McpRuntimeCatalog, QueryRuntimeConfig,
    QueryRuntimeContext, QuerySource, RequestIdentityHttpAuthenticatorError,
    RequestIdentityHttpAuthenticatorFactory, RequestIdentitySelectionContext,
    RequestIdentitySelectionError, RequestIdentitySelector, RuntimeSourcePackage,
    SelectedRequestIdentity, SourceDecorator, SourceDecoratorError, SourceFailurePolicy,
    SourceTables,
};
use coral_spec::parse_source_manifest_yaml;
use coral_spec::v4::{AcceptedIdentityRequirement, IdentityRequirements};
use coral_spec::{
    DatabaseConnectionSpec, DatabaseSourceManifest, FilterMode, FilterSpec, ManifestDataType,
    ParsedTemplate, SourceManifestCommon, SqliteConnectionSpec,
};
use reqwest::header::{HeaderName, HeaderValue};
use serde_json::json;
use tempfile::tempdir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::harness::{execution_to_rows, test_runtime};

struct DecorationCountingDecorator {
    calls: Arc<AtomicUsize>,
    identities: Arc<Mutex<BTreeSet<String>>>,
}

#[tokio::test]
async fn source_without_runtime_catalog_registers_nothing() {
    let source = QuerySource::from_runtime_catalog(
        RuntimeSourcePackage {
            source_name: "empty_v4".to_string(),
            authored_version: Some("1.0.0".to_string()),
            description: String::new(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            identity_requirements: None,
            catalog: None,
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("runtime source without a catalog");

    let catalog = CoralQuery::list_catalog(&[source], test_runtime(), None, None)
        .await
        .expect("catalog discovery");

    assert!(catalog.tables.iter().all(|table| {
        table.catalog_name.as_deref() != Some("empty_v4") && table.schema_name != "empty_v4"
    }));
}

impl SourceDecorator for DecorationCountingDecorator {
    fn name(&self) -> &'static str {
        "decoration-counter"
    }

    fn decorate_source(
        &mut self,
        _source: &QuerySource,
        tables: SourceTables,
    ) -> Result<SourceTables, SourceDecoratorError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut identities = self.identities.lock().map_err(|_poisoned| {
            SourceDecoratorError::failed_precondition("identity recorder poisoned")
        })?;
        identities.extend(tables.iter().map(|(sql_name, _)| sql_name.to_string()));
        Ok(tables)
    }
}

#[tokio::test]
async fn static_catalog_decorates_once_with_complete_inventory() {
    let source = QuerySource::from_runtime_catalog(
        RuntimeSourcePackage {
            source_name: "github".to_string(),
            authored_version: None,
            description: String::new(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            identity_requirements: None,
            catalog: Some(combined_http_catalog(vec![
                http_component(
                    "https://api.example.com",
                    "github_rest",
                    "issues",
                    "/issues",
                ),
                http_component("https://api.example.com", "github_mcp", "pulls", "/pulls"),
            ])),
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("runtime package");
    let calls = Arc::new(AtomicUsize::new(0));
    let identities = Arc::new(Mutex::new(BTreeSet::new()));
    let mut extensions = EngineExtensions::default();
    extensions
        .source_decorators
        .push(Box::new(DecorationCountingDecorator {
            calls: Arc::clone(&calls),
            identities: Arc::clone(&identities),
        }));

    CoralQuery::list_catalog(
        &[source],
        QueryRuntimeConfig::new(QueryRuntimeContext::default(), extensions),
        None,
        None,
    )
    .await
    .expect("catalog discovery");

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        *identities.lock().expect("identity recorder"),
        BTreeSet::from([
            "datafusion.github_mcp.pulls".to_string(),
            "datafusion.github_rest.issues".to_string(),
        ])
    );
}

struct RejectDiscoveredDecorator {
    decoration_calls: Arc<AtomicUsize>,
}

impl SourceDecorator for RejectDiscoveredDecorator {
    fn name(&self) -> &'static str {
        "reject-discovered"
    }

    fn decorate_source(
        &mut self,
        _source: &QuerySource,
        tables: SourceTables,
    ) -> Result<SourceTables, SourceDecoratorError> {
        self.decoration_calls.fetch_add(1, Ordering::SeqCst);
        Ok(tables)
    }

    fn source_failed(
        &mut self,
        _source: &QuerySource,
        _error: &coral_engine::CoreError,
    ) -> Result<SourceFailurePolicy, SourceDecoratorError> {
        Ok(SourceFailurePolicy::Abort)
    }
}

#[tokio::test]
async fn discovered_catalog_support_fails_before_static_decoration() {
    let directory = tempdir().expect("database directory");
    let database_path = directory.path().join("catalog.sqlite");
    drop(rusqlite::Connection::open(&database_path).expect("sqlite database"));
    let database = DatabaseSourceManifest {
        common: SourceManifestCommon {
            dsl_version: 4,
            name: "github_db".to_string(),
            version: String::new(),
            description: String::new(),
            test_queries: Vec::new(),
        },
        connection: DatabaseConnectionSpec::Sqlite(SqliteConnectionSpec {
            path: ParsedTemplate::parse(database_path.to_string_lossy().into_owned())
                .expect("sqlite path"),
        }),
        declared_inputs: Vec::new(),
    };
    let source = QuerySource::from_runtime_catalog(
        RuntimeSourcePackage {
            source_name: "github".to_string(),
            authored_version: None,
            description: String::new(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            identity_requirements: None,
            catalog: Some(
                DatabaseRuntimeCatalog::try_new(
                    "github_db",
                    DatabaseRuntimeBackend::from_manifest(database),
                )
                .expect("database runtime catalog")
                .into(),
            ),
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("runtime package");
    let decoration_calls = Arc::new(AtomicUsize::new(0));
    let mut extensions = EngineExtensions::default();
    extensions
        .source_decorators
        .push(Box::new(RejectDiscoveredDecorator {
            decoration_calls: Arc::clone(&decoration_calls),
        }));

    let error = CoralQuery::list_catalog(
        &[source],
        QueryRuntimeConfig::new(QueryRuntimeContext::default(), extensions),
        None,
        None,
    )
    .await
    .expect_err("unsupported discovered catalog");

    assert!(
        error.to_string().contains(
            "source 'github' has a discovered catalog, which source decorator 'reject-discovered' does not support"
        ),
        "unexpected error: {error}"
    );
    assert_eq!(decoration_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn default_http_runtime_catalog_keeps_two_part_sql_identity() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/issues"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": 1, "title": "Issue"}
        ])))
        .mount(&server)
        .await;

    let source = QuerySource::from_runtime_catalog(
        RuntimeSourcePackage {
            source_name: "github".to_string(),
            authored_version: None,
            description: "GitHub runtime package".to_string(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            identity_requirements: None,
            catalog: Some(http_catalog(http_component(
                &server.uri(),
                "github",
                "issues",
                "/issues",
            ))),
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("runtime package");

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT id, title FROM github.issues",
        )
        .await
        .expect("v3 two-part query should execute"),
    );

    assert_eq!(rows, vec![json!({"id": 1, "title": "Issue"})]);
}

#[tokio::test]
async fn default_mcp_runtime_catalog_keeps_two_part_catalog_identity() {
    let manifest = parse_source_manifest_yaml(
        r"
name: test_mcp
version: 1.0.0
dsl_version: 3
backend: mcp
server:
  transport: stdio
  command: unused
tables:
  - name: issues
    description: Issues
    tool: list_issues
    columns:
      - name: id
        type: Utf8
",
    )
    .expect("MCP manifest");
    let source = QuerySource::from_runtime_catalog(
        RuntimeSourcePackage {
            source_name: "test_mcp".to_string(),
            authored_version: None,
            description: String::new(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            identity_requirements: None,
            catalog: Some(
                McpRuntimeCatalog::try_from_default_catalog_manifest(
                    manifest.as_mcp().expect("MCP source").clone(),
                )
                .expect("default MCP runtime catalog")
                .into(),
            ),
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("runtime package");

    let catalog = CoralQuery::list_catalog(&[source], test_runtime(), None, None)
        .await
        .expect("MCP catalog discovery");

    assert!(catalog.tables.iter().any(|table| {
        table.catalog_name.is_none()
            && table.schema_name == "test_mcp"
            && table.table_name == "issues"
    }));
}

#[tokio::test]
async fn default_file_runtime_catalog_keeps_two_part_sql_identity() {
    let directory = tempdir().expect("file fixture directory");
    std::fs::write(
        directory.path().join("issues.jsonl"),
        "{\"id\":1,\"title\":\"Issue\"}\n",
    )
    .expect("file fixture");
    let location = url::Url::from_directory_path(directory.path())
        .expect("file fixture URL")
        .to_string();
    let manifest = parse_source_manifest_yaml(&format!(
        r"
name: files
version: 1.0.0
dsl_version: 3
backend: file
tables:
  - name: issues
    description: Issues
    format: jsonl
    source:
      location: {location}
    columns:
      - name: id
        type: Int64
      - name: title
        type: Utf8
"
    ))
    .expect("file manifest");
    let source = QuerySource::from_runtime_catalog(
        RuntimeSourcePackage {
            source_name: "files".to_string(),
            authored_version: None,
            description: String::new(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            identity_requirements: None,
            catalog: Some(file_catalog(
                manifest.as_file().expect("file source").clone(),
            )),
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("runtime package");

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT id, title FROM files.issues",
        )
        .await
        .expect("v3 two-part file query should execute"),
    );

    assert_eq!(rows, vec![json!({"id": 1, "title": "Issue"})]);
}

#[tokio::test]
async fn static_catalog_executes_across_declared_tables() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/issues"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": 1, "title": "Issue"}
        ])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/pulls"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": 2, "title": "Pull"}
        ])))
        .mount(&server)
        .await;

    let issues = http_component(&server.uri(), "github", "issues", "/issues");
    let pulls = http_component(&server.uri(), "github", "pulls", "/pulls");
    let source = QuerySource::from_runtime_catalog(
        RuntimeSourcePackage {
            source_name: "github".to_string(),
            authored_version: None,
            description: "GitHub runtime package".to_string(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            identity_requirements: None,
            catalog: Some(combined_http_catalog(vec![issues, pulls])),
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("runtime package");

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT 'issue' AS kind, id, title FROM github.issues UNION ALL SELECT 'pull' AS kind, id, title FROM github.pulls ORDER BY kind",
        )
        .await
        .expect("query should execute"),
    );

    assert_eq!(
        rows,
        vec![
            json!({"kind": "issue", "id": 1, "title": "Issue"}),
            json!({"kind": "pull", "id": 2, "title": "Pull"}),
        ]
    );
}

#[tokio::test]
async fn file_catalog_rejects_unsupported_lookup_key_backend() {
    let source = QuerySource::from_runtime_catalog(
        RuntimeSourcePackage {
            source_name: "demo".to_string(),
            authored_version: None,
            description: "File runtime package".to_string(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            identity_requirements: None,
            catalog: Some(file_catalog(file_component_with_lookup_key_filter())),
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("runtime package");

    let error = CoralQuery::validate_source(&source, test_runtime(), &[])
        .await
        .expect_err("file validation should reject unsupported lookup_key backend");

    assert!(
        error.to_string().contains(
            "source 'demo': lookup_key filters are not supported by the current engine for backend 'file'"
        ),
        "{error}"
    );
}

#[tokio::test]
async fn static_catalog_can_register_multiple_schemas() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/issues"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": 1, "title": "Issue"}
        ])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/pulls"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": 2, "title": "Pull"}
        ])))
        .mount(&server)
        .await;

    let issues = http_component(&server.uri(), "github_rest", "issues", "/issues");
    let pulls = http_component(&server.uri(), "github_mcp", "pulls", "/pulls");
    let source = QuerySource::from_runtime_catalog(
        RuntimeSourcePackage {
            source_name: "github".to_string(),
            authored_version: None,
            description: "GitHub runtime package".to_string(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            identity_requirements: None,
            catalog: Some(combined_http_catalog(vec![issues, pulls])),
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("runtime package");

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT 'issue' AS kind, id, title FROM github_rest.issues UNION ALL SELECT 'pull' AS kind, id, title FROM github_mcp.pulls ORDER BY kind",
        )
        .await
        .expect("query should execute"),
    );

    assert_eq!(
        rows,
        vec![
            json!({"kind": "issue", "id": 1, "title": "Issue"}),
            json!({"kind": "pull", "id": 2, "title": "Pull"}),
        ]
    );
}

#[tokio::test]
async fn selected_sources_reject_runtime_schema_collisions() {
    let server = MockServer::start().await;
    let first = QuerySource::from_runtime_catalog(
        RuntimeSourcePackage {
            source_name: "github_v4".to_string(),
            authored_version: None,
            description: "GitHub runtime package".to_string(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            identity_requirements: None,
            catalog: Some(http_catalog(http_component(
                &server.uri(),
                "github_v4_rest",
                "issues",
                "/issues",
            ))),
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("first runtime package");
    let second = QuerySource::from_runtime_catalog(
        RuntimeSourcePackage {
            source_name: "github_v4_rest".to_string(),
            authored_version: None,
            description: "Conflicting runtime package".to_string(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            identity_requirements: None,
            catalog: Some(http_catalog(http_component(
                &server.uri(),
                "github_v4_rest",
                "pulls",
                "/pulls",
            ))),
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("second runtime package");

    let error = CoralQuery::list_catalog(&[first, second], test_runtime(), None, None)
        .await
        .expect_err("duplicate selected schemas should fail");

    assert!(
        error
            .to_string()
            .contains("runtime schema name 'github_v4_rest' conflicts"),
        "{error}"
    );
}

#[tokio::test]
async fn identity_gated_source_requires_request_identity_selector() {
    let source = identity_runtime_source(identity_http_component());

    let error = CoralQuery::list_catalog(&[source], test_runtime(), None, None)
        .await
        .expect_err("identity-gated source should require a selector");

    assert!(
        error.to_string().contains(
            "source 'github_v4' declares identity_requirements but no request identity selector is installed"
        ),
        "{error}"
    );
}

#[tokio::test]
async fn identity_gated_source_requires_request_identity_authenticator_factory() {
    let source = identity_runtime_source(identity_http_component());
    let runtime =
        test_runtime().with_request_identity_selector(Some(Arc::new(UnexpectedIdentitySelector)));

    let error = CoralQuery::list_catalog(&[source], runtime, None, None)
        .await
        .expect_err("identity-gated source should require an authenticator factory");

    assert!(
        error.to_string().contains(
            "source 'github_v4' declares identity_requirements but no request identity HTTP authenticator factory is installed"
        ),
        "{error}"
    );
}

#[tokio::test]
async fn identity_gated_sources_reject_duplicate_source_names_before_binding() {
    let sources = [
        identity_runtime_source(identity_http_component()),
        identity_runtime_source(identity_http_component()),
    ];
    let factory_called = Arc::new(AtomicBool::new(false));
    let runtime = test_runtime()
        .with_request_identity_selector(Some(Arc::new(UnexpectedIdentitySelector)))
        .with_request_identity_http_authenticator_factory(Some(identity_factory(Arc::clone(
            &factory_called,
        ))));

    let error = CoralQuery::list_catalog(&sources, runtime, None, None)
        .await
        .expect_err("duplicate gated source names should fail before identity binding");

    assert!(
        error
            .to_string()
            .contains("source 'github_v4' appears more than once with identity_requirements"),
        "{error}"
    );
    assert!(!factory_called.load(Ordering::Relaxed));
}

#[tokio::test]
async fn identity_gated_source_binds_identity_once_for_http_catalog() {
    let first = identity_http_component();
    let mut second = identity_http_component();
    second.common.name = "github_v4_graphql".to_string();
    let source = identity_runtime_source_with_components(vec![first, second]);
    let factory_called = Arc::new(AtomicBool::new(false));
    let runtime = identity_runtime(
        SelectedRequestIdentity::new(
            "identity-1",
            "github_oauth",
            BTreeMap::from([
                ("host".to_string(), json!("api.github.com")),
                ("port".to_string(), json!(443)),
            ]),
        ),
        Arc::clone(&factory_called),
    );

    let catalog = CoralQuery::list_catalog(&[source], runtime, None, None)
        .await
        .expect("one source identity should authenticate every HTTP component");

    assert!(
        catalog
            .tables
            .iter()
            .any(|table| table.schema_name == "github_v4_rest")
    );
    assert!(
        catalog
            .tables
            .iter()
            .any(|table| table.schema_name == "github_v4_graphql")
    );
    assert!(factory_called.load(Ordering::Relaxed));
}

#[tokio::test]
async fn identity_gated_source_rejects_selected_identity_type_mismatch() {
    let source = identity_runtime_source(identity_http_component());
    let factory_called = Arc::new(AtomicBool::new(false));
    let runtime = identity_runtime(
        SelectedRequestIdentity::new(
            "identity-1",
            "github_oauth",
            BTreeMap::from([
                ("host".to_string(), json!("api.github.com")),
                ("port".to_string(), json!(443.0)),
            ]),
        ),
        Arc::clone(&factory_called),
    );

    let error = CoralQuery::list_catalog(&[source], runtime, None, None)
        .await
        .expect_err("JSON type mismatch should reject selected identity");

    assert!(
        error.to_string().contains(
            "selected identity 'identity-1' with spec 'github_oauth' that does not satisfy identity_requirements"
        ),
        "{error}"
    );
    assert!(!factory_called.load(Ordering::Relaxed));
}

#[tokio::test]
async fn identity_gated_source_accepts_spec_id_and_audience_subset() {
    let source = identity_runtime_source(identity_http_component());
    let factory_called = Arc::new(AtomicBool::new(false));
    let runtime = identity_runtime(
        SelectedRequestIdentity::new(
            "identity-1",
            "github_oauth",
            BTreeMap::from([
                ("host".to_string(), json!("api.github.com")),
                ("port".to_string(), json!(443)),
                ("tenant".to_string(), json!("acme")),
            ]),
        ),
        Arc::clone(&factory_called),
    );

    let catalog = CoralQuery::list_catalog(&[source], runtime, None, None)
        .await
        .expect("matching identity should build runtime");

    assert!(
        catalog
            .tables
            .iter()
            .any(|table| { table.schema_name == "github_v4_rest" && table.table_name == "issues" })
    );
    assert!(factory_called.load(Ordering::Relaxed));
}

fn http_component(
    base_url: &str,
    schema_name: &str,
    table_name: &str,
    path: &str,
) -> coral_spec::backends::http::HttpSourceManifest {
    let manifest = parse_source_manifest_yaml(&format!(
        r"
name: {schema_name}
version: 1.0.0
dsl_version: 3
backend: http
base_url: {base_url}
tables:
  - name: {table_name}
    description: {table_name}
    request:
      method: GET
      path: {path}
    response: {{}}
    columns:
      - name: id
        type: Int64
      - name: title
        type: Utf8
"
    ))
    .expect("manifest");
    manifest.as_http().expect("http manifest").clone()
}

fn http_catalog(
    manifest: coral_spec::backends::http::HttpSourceManifest,
) -> coral_engine::RuntimeCatalog {
    combined_http_catalog(vec![manifest])
}

fn combined_http_catalog(
    manifests: Vec<coral_spec::backends::http::HttpSourceManifest>,
) -> coral_engine::RuntimeCatalog {
    combined_http_catalog_named("datafusion", manifests)
}

fn combined_http_catalog_named(
    catalog_name: &str,
    manifests: Vec<coral_spec::backends::http::HttpSourceManifest>,
) -> coral_engine::RuntimeCatalog {
    let backend =
        HttpRuntimeBackend::from_manifest(manifests.first().expect("at least one HTTP manifest"));
    let mut relations = Vec::new();
    for manifest in manifests {
        let schema_name = manifest.common.name;
        relations.extend(manifest.tables.into_iter().map(|table| {
            HttpRuntimeRelation::try_table(
                coral_spec::SqlObjectName::new(catalog_name, &schema_name, table.name()),
                table,
            )
            .expect("HTTP table relation")
        }));
        relations.extend(manifest.functions.into_iter().map(|function| {
            HttpRuntimeRelation::try_table_function(
                coral_spec::SqlObjectName::new(catalog_name, &schema_name, &function.name),
                function,
            )
            .expect("HTTP table function relation")
        }));
    }
    HttpRuntimeCatalog::try_new(catalog_name, backend, relations)
        .expect("default HTTP runtime catalog")
        .into()
}

fn file_catalog(
    manifest: coral_spec::backends::file::FileSourceManifest,
) -> coral_engine::RuntimeCatalog {
    FileRuntimeCatalog::try_from_default_catalog_manifest(manifest)
        .expect("default file runtime catalog")
        .into()
}

fn identity_http_component() -> coral_spec::backends::http::HttpSourceManifest {
    let mut manifest = http_component(
        "https://api.example.com",
        "github_v4_rest",
        "issues",
        "/issues",
    );
    manifest.common.dsl_version = 4;
    manifest
}

fn identity_runtime_source(
    component: coral_spec::backends::http::HttpSourceManifest,
) -> QuerySource {
    identity_runtime_source_with_components(vec![component])
}

fn identity_runtime_source_with_components(
    components: Vec<coral_spec::backends::http::HttpSourceManifest>,
) -> QuerySource {
    QuerySource::from_runtime_catalog(
        RuntimeSourcePackage {
            source_name: "github_v4".to_string(),
            authored_version: None,
            description: "GitHub v4 runtime package".to_string(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            identity_requirements: Some(identity_requirements()),
            catalog: Some(combined_http_catalog_named("github_v4", components)),
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("identity runtime package")
}

fn identity_requirements() -> IdentityRequirements {
    IdentityRequirements {
        accepts: vec![AcceptedIdentityRequirement {
            id: "github_rest_read".to_string(),
            identity_specs: vec!["github_oauth".to_string()],
            audience: BTreeMap::from([
                ("host".to_string(), json!("api.github.com")),
                ("port".to_string(), json!(443)),
            ]),
        }],
    }
}

fn identity_runtime(
    identity: SelectedRequestIdentity,
    factory_called: Arc<AtomicBool>,
) -> QueryRuntimeConfig {
    QueryRuntimeConfig::default()
        .with_request_identity_selector(Some(Arc::new(FixedIdentitySelector { identity })))
        .with_request_identity_http_authenticator_factory(Some(identity_factory(factory_called)))
}

fn identity_factory(factory_called: Arc<AtomicBool>) -> RequestIdentityHttpAuthenticatorFactory {
    Arc::new(move |_identity| {
        assert!(
            !factory_called.swap(true, Ordering::Relaxed),
            "identity authenticator factory should run once per source"
        );
        Ok(empty_identity_authenticator())
    })
}

fn empty_identity_authenticator() -> BoundRequestIdentityHttpAuthenticator {
    Arc::new(|_request, _resolved_inputs| {
        Box::pin(async {
            Ok::<Vec<(HeaderName, HeaderValue)>, RequestIdentityHttpAuthenticatorError>(Vec::new())
        })
    })
}

#[derive(Debug)]
struct FixedIdentitySelector {
    identity: SelectedRequestIdentity,
}

#[async_trait]
impl RequestIdentitySelector for FixedIdentitySelector {
    async fn select_identity(
        &self,
        _identity: &RequestIdentitySelectionContext,
    ) -> Result<SelectedRequestIdentity, RequestIdentitySelectionError> {
        Ok(self.identity.clone())
    }
}

#[derive(Debug)]
struct UnexpectedIdentitySelector;

#[async_trait]
impl RequestIdentitySelector for UnexpectedIdentitySelector {
    async fn select_identity(
        &self,
        _identity: &RequestIdentitySelectionContext,
    ) -> Result<SelectedRequestIdentity, RequestIdentitySelectionError> {
        panic!("identity selection should not run")
    }
}

fn file_component_with_lookup_key_filter() -> coral_spec::backends::file::FileSourceManifest {
    let manifest = parse_source_manifest_yaml(
        r"
name: demo
version: 1.0.0
dsl_version: 3
backend: file
tables:
  - name: items
    description: Items
    format: jsonl
    source:
      location: file:///tmp/coral-composite-lookup-key/
    columns:
      - name: id
        type: Utf8
",
    )
    .expect("manifest");
    let mut manifest = manifest.as_file().expect("file manifest").clone();
    let table = manifest.tables.first_mut().expect("file manifest table");
    table.common.filters.push(FilterSpec {
        name: "id".to_string(),
        data_type: ManifestDataType::Utf8,
        required: false,
        mode: FilterMode::Equality,
        description: String::new(),
        lookup_key: true,
    });
    manifest
}
