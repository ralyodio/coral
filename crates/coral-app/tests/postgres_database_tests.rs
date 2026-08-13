//! Pins gRPC server startup behavior for configured Postgres storage.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set and intentionally exercise only a subset of it."
)]

use std::collections::BTreeMap;
use std::fs;

use coral_client::local::ServerBuilder;
use coral_engine::{
    CoralQuery, DatabaseRuntimeBackend, DatabaseRuntimeCatalog, QueryRuntimeConfig, QuerySource,
    RuntimeSourcePackage,
};
use coral_spec::{
    DatabaseConnectionSpec, DatabaseSourceManifest, ParsedTemplate, PostgresConnectionSpec,
    SourceManifestCommon,
};
use sqlx::postgres::PgPoolOptions;
use tempfile::TempDir;

#[tokio::test]
#[ignore = "set CORAL_TEST_POSTGRES_URL to run configured Postgres startup coverage"]
async fn server_lifecycle_can_start_with_postgres_database_config() {
    let Some(database_url) = postgres_test_url() else {
        return;
    };
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(
        config_dir.join("config.toml"),
        "[database]\nbackend = \"postgres\"\nurl_env = \"CORAL_TEST_POSTGRES_URL\"\n",
    )
    .expect("write config");

    let server = ServerBuilder::new()
        .with_config_dir(&config_dir)
        .start()
        .await
        .expect("start server with Postgres config");
    assert_postgres_db_is_migrated(&database_url).await;

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
#[ignore = "set CORAL_TEST_POSTGRES_URL to run canonical Postgres catalog coverage"]
async fn postgres_discovered_catalog_uses_canonical_sql_and_remote_columns_with_failure_isolation()
{
    let Some(database_url) = postgres_test_url() else {
        return;
    };
    let pool = PgPoolOptions::new()
        .connect(&database_url)
        .await
        .expect("open Postgres database");
    sqlx::query("CREATE SCHEMA IF NOT EXISTS coral_inventory")
        .execute(&pool)
        .await
        .expect("create inventory fixture schema");
    sqlx::query("DROP TABLE IF EXISTS coral_inventory.column_types")
        .execute(&pool)
        .await
        .expect("reset inventory fixture table");
    sqlx::query(
        "CREATE TABLE coral_inventory.column_types (
            id BIGINT NOT NULL,
            display_name CHARACTER VARYING(64),
            note TEXT
        )",
    )
    .execute(&pool)
    .await
    .expect("create inventory fixture table");
    sqlx::query(
        "INSERT INTO coral_inventory.column_types (id, display_name, note)
         VALUES (1, 'Ada', 'canonical catalog fixture')",
    )
    .execute(&pool)
    .await
    .expect("populate inventory fixture table");

    let mut unreachable_url = url::Url::parse(&database_url).expect("parse Postgres test URL");
    unreachable_url
        .set_port(Some(1))
        .expect("set unreachable Postgres port");
    let sources = vec![
        postgres_source_named(unreachable_url.as_str(), "broken_postgres"),
        postgres_source(&database_url),
    ];

    let runtime = CoralQuery::prepare(&sources, QueryRuntimeConfig::default())
        .await
        .expect("prepare one runtime while isolating the broken source");
    let broken_error = runtime
        .execute_sql("SELECT * FROM broken_postgres.public.unreachable")
        .await
        .expect_err("broken Postgres catalog should remain unavailable");
    assert!(
        broken_error.to_string().contains("broken_postgres"),
        "broken source error should preserve its identity: {broken_error}"
    );

    let result = runtime
        .execute_sql(
            "SELECT display_name FROM postgres_inventory.coral_inventory.column_types WHERE id = 1",
        )
        .await
        .expect("query canonical Postgres table in the same runtime");
    assert_eq!(result.row_count(), 1);

    let tables = runtime
        .list_tables(
            Some("postgres_inventory"),
            Some("coral_inventory"),
            Some("column_types"),
        )
        .await
        .expect("read Postgres column inventory through the same runtime");

    assert_eq!(tables.len(), 1);
    let table = tables.first().expect("inventory fixture table");
    assert_eq!(table.catalog_name.as_deref(), Some("postgres_inventory"));
    assert_eq!(table.schema_name, "coral_inventory");
    assert_eq!(table.table_name, "column_types");
    let columns = &table.columns;
    assert_eq!(columns.len(), 3);
    let id = columns.first().expect("id column metadata");
    assert_eq!(id.name, "id");
    assert_eq!(id.data_type, "bigint");
    assert!(!id.nullable);
    assert_eq!(id.ordinal_position, 0);
    let display_name = columns.get(1).expect("display_name column metadata");
    assert_eq!(display_name.name, "display_name");
    assert_eq!(display_name.data_type, "character varying");
    assert!(display_name.nullable);
    assert_eq!(display_name.ordinal_position, 1);

    sqlx::query("DROP SCHEMA coral_inventory CASCADE")
        .execute(&pool)
        .await
        .expect("remove inventory fixture schema");
}

fn postgres_source(database_url: &str) -> QuerySource {
    postgres_source_named(database_url, "postgres_inventory")
}

fn postgres_source_named(database_url: &str, source_name: &str) -> QuerySource {
    let url = url::Url::parse(database_url).expect("parse Postgres test URL");
    let host = url.host_str().expect("Postgres test URL host");
    let port = url.port_or_known_default().expect("Postgres test URL port");
    let database = url.path().trim_start_matches('/');
    let sslmode = url
        .query_pairs()
        .find_map(|(key, value)| (key == "sslmode").then(|| value.into_owned()))
        .unwrap_or_else(|| {
            if matches!(host, "127.0.0.1" | "localhost" | "::1") {
                "disable".to_string()
            } else {
                "verify-full".to_string()
            }
        });
    let template = |value: &str| ParsedTemplate::parse(value).expect("literal template");
    let manifest = DatabaseSourceManifest {
        common: SourceManifestCommon {
            dsl_version: 4,
            name: source_name.to_string(),
            version: String::new(),
            description: "Postgres inventory integration fixture".to_string(),
            test_queries: Vec::new(),
        },
        connection: DatabaseConnectionSpec::Postgres(PostgresConnectionSpec {
            host: template(host),
            port: template(&port.to_string()),
            database: template(database),
            user: template(url.username()),
            password: template(url.password().unwrap_or_default()),
            sslmode: Some(template(&sslmode)),
        }),
        declared_inputs: Vec::new(),
    };
    QuerySource::from_runtime_catalog(
        RuntimeSourcePackage {
            source_name: source_name.to_string(),
            authored_version: None,
            description: String::new(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            identity_requirements: None,
            catalog: Some(
                DatabaseRuntimeCatalog::try_new(
                    source_name,
                    DatabaseRuntimeBackend::from_manifest(manifest),
                )
                .expect("database runtime catalog")
                .into(),
            ),
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("build Postgres inventory source")
}

async fn assert_postgres_db_is_migrated(database_url: &str) {
    let pool = PgPoolOptions::new()
        .connect(database_url)
        .await
        .expect("open Postgres database");
    let table_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('public.workspaces') IS NOT NULL")
            .fetch_one(&pool)
            .await
            .expect("inspect migrated Postgres schema");
    assert!(table_exists, "workspaces table should be migrated");
}

#[expect(
    clippy::disallowed_methods,
    reason = "The ignored Postgres integration test is explicitly gated by this CI/test-only variable."
)]
fn postgres_test_url() -> Option<String> {
    std::env::var("CORAL_TEST_POSTGRES_URL")
        .ok()
        .filter(|value| !value.is_empty())
}
