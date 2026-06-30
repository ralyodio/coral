//! RDBMS-backed durable app-state infrastructure.

mod backend;
mod clock;
mod config;
mod coral_db;
mod error;
mod import;
mod migrations;
mod repositories;
mod schema;
mod session;
mod task_query_state;
mod task_state;
mod transaction;
mod workspace_state;

pub(crate) use clock::now_unix_nanos_i64;
pub(crate) use config::{DatabaseConfig, ResolvedDatabaseConfig};
pub(crate) use coral_db::CoralDb;
pub(crate) use error::DbError;
pub(crate) use import::{import_filesystem_feedback_reports, run_state_migrations};
pub(crate) use repositories::feedback_reports::FeedbackReportRecord;
#[expect(
    unused_imports,
    reason = "identity persistence types are not yet wired to production consumers"
)]
pub(crate) use repositories::identity_specs::{
    IdentitySpecDocumentRecord, IdentitySpecKey, IdentitySpecRecord, IdentitySpecScope,
};
pub(crate) use repositories::materializations::{
    MaterializationRecord, MaterializationSurfaceRecord,
};
pub(crate) use repositories::tasks::{TaskCompletionUpdate, TaskLifecycleState};
pub(crate) use session::{DbRepos, DbSession};
#[cfg(test)]
pub(crate) use task_query_state::TaskQueryRelationRecord;
pub(crate) use task_query_state::{TaskQueryRelationWrite, TaskQueryWrite, TaskQueryWriteResult};
#[cfg(test)]
pub(crate) use task_state::TaskMutationBarrier;
pub(crate) use task_state::{TaskCreation, TaskCreationResult};
pub(crate) use transaction::CoralTx;

#[cfg(test)]
pub(crate) async fn open_test_database(
    layout: &super::AppStateLayout,
) -> Result<std::sync::Arc<CoralDb>, crate::bootstrap::AppError> {
    let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(layout)? else {
        return Err(crate::bootstrap::AppError::FailedPrecondition(
            "default test database config should use SQLite".to_string(),
        ));
    };
    let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path }).await?;
    db.migrate().await?;
    Ok(std::sync::Arc::new(db))
}
