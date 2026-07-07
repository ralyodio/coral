use std::collections::BTreeSet;

use super::session::DbRepos;
use super::{CoralDb, now_unix_nanos_i64};
use crate::bootstrap::AppError;
use crate::sources::model::InstalledSource;
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::{WorkspaceName, WorkspaceRecord};

const WORKSPACE_CATALOG_CUTOVER_ID: &str = "workspace_catalog_cutover_v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkspaceCatalogCutoverReport {
    pub(crate) workspace_count: usize,
    pub(crate) source_count: usize,
    pub(crate) cutover_performed: bool,
}

pub(crate) async fn run_state_migrations(
    db: &CoralDb,
    config_store: &ConfigStore,
    layout: &AppStateLayout,
) -> Result<(), AppError> {
    cutover_legacy_workspace_catalog(db, config_store).await?;
    remove_legacy_task_jsonl(config_store, layout)?;
    Ok(())
}

fn remove_legacy_task_jsonl(
    config_store: &ConfigStore,
    layout: &AppStateLayout,
) -> Result<(), AppError> {
    let _state_lock = config_store.state_lock_exclusive()?;
    layout.remove_legacy_task_event_logs()?;
    Ok(())
}

async fn cutover_legacy_workspace_catalog(
    db: &CoralDb,
    config_store: &ConfigStore,
) -> Result<WorkspaceCatalogCutoverReport, AppError> {
    cutover_legacy_workspace_catalog_at(db, config_store, now_unix_nanos_i64()?).await
}

async fn cutover_legacy_workspace_catalog_at(
    db: &CoralDb,
    config_store: &ConfigStore,
    now_unix_nanos: i64,
) -> Result<WorkspaceCatalogCutoverReport, AppError> {
    let _state_lock = config_store.state_lock_exclusive()?;
    let mut tx = db.begin().await?;
    if !tx
        .state_migrations()
        .try_claim(WORKSPACE_CATALOG_CUTOVER_ID, now_unix_nanos)
        .await?
    {
        tx.rollback().await?;
        let mut session = db;
        let (workspace_count, source_count) = database_catalog_counts(&mut session).await?;
        return Ok(WorkspaceCatalogCutoverReport {
            workspace_count,
            source_count,
            cutover_performed: false,
        });
    }

    let config = config_store.load_config_unlocked()?;
    let workspaces = config.legacy_workspace_records();
    let source_entries = config.source_catalog_entries();

    let mut imported_workspaces = BTreeSet::new();
    import_legacy_workspaces(
        &mut tx,
        &workspaces,
        now_unix_nanos,
        &mut imported_workspaces,
    )
    .await?;
    let source_count = import_legacy_source_catalog(
        &mut tx,
        &source_entries,
        now_unix_nanos,
        &mut imported_workspaces,
    )
    .await?;
    prune_stale_workspaces(&mut tx, &imported_workspaces).await?;
    verify_workspace_parity(&mut tx, &imported_workspaces).await?;
    tx.commit().await?;

    Ok(WorkspaceCatalogCutoverReport {
        workspace_count: imported_workspaces.len(),
        source_count,
        cutover_performed: true,
    })
}

async fn import_legacy_workspaces<S>(
    session: &mut S,
    workspaces: &[WorkspaceRecord],
    now_unix_nanos: i64,
    imported_workspaces: &mut BTreeSet<WorkspaceName>,
) -> Result<(), AppError>
where
    S: DbRepos,
{
    for workspace in workspaces {
        session
            .workspaces()
            .ensure(workspace.name.as_str(), now_unix_nanos)
            .await?;
        imported_workspaces.insert(workspace.name.clone());
    }
    Ok(())
}

async fn import_legacy_source_catalog<S>(
    session: &mut S,
    entries: &[(WorkspaceName, InstalledSource)],
    now_unix_nanos: i64,
    imported_workspaces: &mut BTreeSet<WorkspaceName>,
) -> Result<usize, AppError>
where
    S: DbRepos,
{
    let mut source_count = 0;
    for (workspace_name, source) in entries {
        imported_workspaces.insert(workspace_name.clone());
        session
            .workspaces()
            .ensure(workspace_name.as_str(), now_unix_nanos)
            .await?;
        if session
            .sources()
            .get_source(workspace_name, &source.name)
            .await?
            .is_some()
        {
            continue;
        }
        session
            .sources()
            .upsert_source(workspace_name, source, now_unix_nanos)
            .await?;
        let imported = session
            .sources()
            .get_source(workspace_name, &source.name)
            .await?;
        if imported.as_ref() != Some(source) {
            return Err(AppError::Database(format!(
                "source catalog cutover failed validation for {workspace_name}:{}",
                source.name
            )));
        }
        source_count += 1;
    }
    Ok(source_count)
}

async fn prune_stale_workspaces<S>(
    session: &mut S,
    imported_workspaces: &BTreeSet<WorkspaceName>,
) -> Result<(), AppError>
where
    S: DbRepos,
{
    let expected = imported_workspaces
        .iter()
        .map(WorkspaceName::as_str)
        .collect::<BTreeSet<_>>();
    for workspace in session.workspaces().list().await? {
        if !expected.contains(workspace.id.as_str()) {
            session.workspaces().delete(&workspace.id).await?;
        }
    }
    Ok(())
}

async fn verify_workspace_parity<S>(
    session: &mut S,
    imported_workspaces: &BTreeSet<WorkspaceName>,
) -> Result<(), AppError>
where
    S: DbRepos,
{
    let expected = imported_workspaces
        .iter()
        .map(|workspace| workspace.as_str().to_string())
        .collect::<BTreeSet<_>>();
    let actual = session
        .workspaces()
        .list()
        .await?
        .into_iter()
        .map(|workspace| workspace.id)
        .collect::<BTreeSet<_>>();
    if actual == expected {
        return Ok(());
    }
    Err(AppError::Database(format!(
        "workspace catalog cutover parity validation failed: legacy={expected:?} database={actual:?}"
    )))
}

async fn database_catalog_counts<S>(session: &mut S) -> Result<(usize, usize), AppError>
where
    S: DbRepos,
{
    let workspaces = session.workspaces().list().await?;
    let mut source_count = 0;
    for workspace in &workspaces {
        let workspace_name = WorkspaceName::parse(&workspace.id).map_err(|error| {
            AppError::Database(format!(
                "workspace catalog cutover state contains invalid workspace name '{}': {error}",
                workspace.id
            ))
        })?;
        source_count += session
            .sources()
            .list_workspace_source_names(&workspace_name)
            .await?
            .len();
    }
    Ok((workspaces.len(), source_count))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tempfile::tempdir;

    use super::{
        WORKSPACE_CATALOG_CUTOVER_ID, WorkspaceCatalogCutoverReport,
        cutover_legacy_workspace_catalog, cutover_legacy_workspace_catalog_at,
        run_state_migrations,
    };
    use crate::credentials::CredentialStorageKind;
    use crate::sources::SourceName;
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::workspaces::WorkspaceName;

    #[tokio::test]
    async fn cuts_over_legacy_config_sources_into_database() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = source(
            "github",
            Some("1.2.3"),
            [("GITHUB_API_BASE", "https://api.github.com")],
            ["GITHUB_TOKEN"],
            Some(CredentialStorageKind::Keychain),
            SourceOrigin::Imported,
        );
        config_store
            .upsert_source(&workspace, source.clone())
            .expect("write config source");
        let db = open_sqlite(&layout).await;

        let report = cutover_legacy_workspace_catalog_at(&db, &config_store, 11)
            .await
            .expect("cut over legacy config");

        assert_eq!(
            report,
            WorkspaceCatalogCutoverReport {
                workspace_count: 1,
                source_count: 1,
                cutover_performed: true,
            }
        );
        let mut session = &db;
        assert!(
            session
                .workspaces()
                .get(workspace.as_str())
                .await
                .expect("get workspace")
                .is_some()
        );
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &source.name)
                .await
                .expect("get source"),
            Some(source)
        );
        assert!(
            session
                .state_migrations()
                .has_completed(WORKSPACE_CATALOG_CUTOVER_ID)
                .await
                .expect("read cutover marker")
        );
    }

    #[tokio::test]
    async fn cuts_over_legacy_workspaces_without_sources() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let analytics_workspace = WorkspaceName::parse("analytics").expect("workspace");
        config_store
            .create_legacy_workspace_entry_for_tests(&analytics_workspace)
            .expect("create legacy workspace entry");
        let db = open_sqlite(&layout).await;

        let report = cutover_legacy_workspace_catalog_at(&db, &config_store, 11)
            .await
            .expect("cut over legacy workspace catalog");

        assert_eq!(
            report,
            WorkspaceCatalogCutoverReport {
                workspace_count: 2,
                source_count: 0,
                cutover_performed: true,
            }
        );
        let mut session = &db;
        assert_eq!(
            session
                .workspaces()
                .list()
                .await
                .expect("list workspaces")
                .into_iter()
                .map(|workspace| workspace.id)
                .collect::<Vec<_>>(),
            vec![
                "analytics".to_string(),
                WorkspaceName::default().as_str().to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn cutover_preserves_existing_database_sources() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let existing = source("github", None, [], [], None, SourceOrigin::Bundled);
        let replacement = source(
            "github",
            Some("1.2.3"),
            [("OWNER", "coral")],
            [],
            Some(CredentialStorageKind::File),
            SourceOrigin::Imported,
        );
        config_store
            .upsert_source(&workspace, replacement)
            .expect("write config source");
        let db = open_sqlite(&layout).await;
        {
            let mut tx = db.begin().await.expect("begin seed tx");
            tx.workspaces()
                .ensure(workspace.as_str(), 7)
                .await
                .expect("seed workspace");
            tx.sources()
                .upsert_source(&workspace, &existing, 7)
                .await
                .expect("seed db source");
            tx.commit().await.expect("commit seed tx");
        }

        let report = cutover_legacy_config_at(&db, &config_store, 11)
            .await
            .expect("cut over legacy config");

        assert_eq!(
            report,
            LegacyConfigCutoverReport {
                workspace_count: 1,
                source_count: 0,
                cutover_performed: true,
            }
        );
        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&workspace, &existing.name)
                .await
                .expect("get preserved source"),
            Some(existing)
        );
    }

    #[tokio::test]
    async fn cutover_imports_missing_config_sources_without_overwriting_existing() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let default_workspace = WorkspaceName::parse("default").expect("workspace");
        let other_workspace = WorkspaceName::parse("other").expect("workspace");
        let existing_source = source("github", None, [], [], None, SourceOrigin::Bundled);
        let config_source = source("slack", None, [], [], None, SourceOrigin::Bundled);
        config_store
            .create_workspace(&other_workspace)
            .expect("create config workspace");
        config_store
            .upsert_source(&other_workspace, config_source.clone())
            .expect("write config source");
        let db = open_sqlite(&layout).await;
        {
            let mut tx = db.begin().await.expect("begin seed tx");
            tx.workspaces()
                .ensure(default_workspace.as_str(), 7)
                .await
                .expect("seed workspace");
            tx.sources()
                .upsert_source(&default_workspace, &existing_source, 7)
                .await
                .expect("seed db source");
            tx.commit().await.expect("commit seed tx");
        }

        let report = cutover_legacy_config_at(&db, &config_store, 11)
            .await
            .expect("cut over legacy config");

        assert_eq!(
            report,
            LegacyConfigCutoverReport {
                workspace_count: 2,
                source_count: 1,
                cutover_performed: true,
            }
        );
        let mut session = &db;
        assert_eq!(
            session
                .sources()
                .get_source(&default_workspace, &existing_source.name)
                .await
                .expect("get existing source"),
            Some(existing_source)
        );
        assert_eq!(
            session
                .sources()
                .get_source(&other_workspace, &config_source.name)
                .await
                .expect("get imported source"),
            Some(config_source)
        );
    }

    #[tokio::test]
    async fn cutover_resets_stale_shadow_database_rows() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let analytics_workspace = WorkspaceName::parse("analytics").expect("workspace");
        config_store
            .create_legacy_workspace_entry_for_tests(&analytics_workspace)
            .expect("create legacy workspace entry");
        let db = open_sqlite(&layout).await;
        let stale_workspace = WorkspaceName::parse("stale").expect("workspace");
        let stale_source = source("stale_source", None, [], [], None, SourceOrigin::Bundled);
        let mut tx = db.begin().await.expect("begin stale seed tx");
        tx.workspaces()
            .ensure(stale_workspace.as_str(), 7)
            .await
            .expect("seed stale workspace");
        tx.sources()
            .upsert_source(&stale_workspace, &stale_source, 7)
            .await
            .expect("seed stale source");
        tx.commit().await.expect("commit stale seed tx");

        cutover_legacy_workspace_catalog_at(&db, &config_store, 11)
            .await
            .expect("cut over legacy workspace catalog");

        let mut session = &db;
        assert_eq!(
            session
                .workspaces()
                .list()
                .await
                .expect("list workspaces")
                .into_iter()
                .map(|workspace| workspace.id)
                .collect::<Vec<_>>(),
            vec![
                "analytics".to_string(),
                WorkspaceName::default().as_str().to_string(),
            ]
        );
        assert_eq!(
            session
                .sources()
                .get_source(&stale_workspace, &stale_source.name)
                .await
                .expect("get stale source"),
            None
        );
    }

    #[tokio::test]
    async fn completed_cutover_does_not_reimport_legacy_config() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let db = open_sqlite(&layout).await;

        cutover_legacy_workspace_catalog_at(&db, &config_store, 11)
            .await
            .expect("initial cutover");
        std::fs::write(layout.config_file(), "[[workspaces]\n").expect("corrupt config");

        let report = cutover_legacy_workspace_catalog(&db, &config_store)
            .await
            .expect("marker should skip legacy config reload");

        assert_eq!(
            report,
            WorkspaceCatalogCutoverReport {
                workspace_count: 1,
                source_count: 0,
                cutover_performed: false,
            }
        );
    }

    #[tokio::test]
    async fn shared_database_does_not_scope_task_cleanup_to_the_first_layout() {
        let temp = tempdir().expect("temp dir");
        let first_layout =
            AppStateLayout::discover(Some(temp.path().join("first"))).expect("first layout");
        let second_layout =
            AppStateLayout::discover(Some(temp.path().join("second"))).expect("second layout");
        first_layout.ensure().expect("ensure first layout");
        second_layout.ensure().expect("ensure second layout");
        let first_config_store = ConfigStore::new(first_layout.clone());
        let second_config_store = ConfigStore::new(second_layout.clone());
        let first_legacy_file = first_layout
            .workspace_dir(&WorkspaceName::default())
            .join("tasks")
            .join("tasks.jsonl");
        let second_legacy_file = second_layout
            .workspace_dir(&WorkspaceName::default())
            .join("tasks")
            .join("tasks.jsonl");
        for path in [&first_legacy_file, &second_legacy_file] {
            std::fs::create_dir_all(path.parent().expect("legacy task dir"))
                .expect("create legacy task dir");
            std::fs::write(path, "sensitive task intent").expect("write legacy task file");
        }
        let db = open_sqlite(&first_layout).await;

        run_state_migrations(&db, &first_config_store, &first_layout)
            .await
            .expect("run migrations for first layout");
        run_state_migrations(&db, &second_config_store, &second_layout)
            .await
            .expect("run migrations for second layout");

        assert!(!first_legacy_file.exists());
        assert!(!second_legacy_file.exists());
    }

    async fn open_sqlite(layout: &AppStateLayout) -> CoralDb {
        let config = DatabaseConfig::load(layout).expect("db config");
        let DatabaseConfig::Sqlite { path } = config else {
            panic!("default test config should be sqlite");
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");
        db
    }

    fn source<const V: usize, const S: usize>(
        name: &str,
        version: Option<&str>,
        variables: [(&str, &str); V],
        secrets: [&str; S],
        credential_storage: Option<CredentialStorageKind>,
        origin: SourceOrigin,
    ) -> InstalledSource {
        InstalledSource {
            name: SourceName::parse(name).expect("source name"),
            version: version.map(str::to_string),
            variables: variables
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect::<BTreeMap<_, _>>(),
            secrets: secrets.into_iter().map(str::to_string).collect(),
            credential_storage,
            origin,
        }
    }
}
