//! Implements the gRPC `WorkspaceService` for workspace lifecycle APIs.

use coral_api::v1::workspace_service_server::WorkspaceService as WorkspaceServiceApi;
use coral_api::v1::{
    CreateWorkspaceRequest, CreateWorkspaceResponse, DeleteWorkspaceRequest,
    DeleteWorkspaceResponse, ListWorkspacesRequest, ListWorkspacesResponse, Workspace,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::transport::{
    grpc_span, instrument_grpc, run_state_locked_mutation, workspace_name_from_proto,
    workspace_to_proto,
};
use crate::workspaces::{WorkspaceManager, WorkspaceRecord};

#[derive(Clone)]
pub(crate) struct WorkspaceService {
    workspaces: WorkspaceManager,
}

impl WorkspaceService {
    pub(crate) fn new(workspace_manager: WorkspaceManager) -> Self {
        Self {
            workspaces: workspace_manager,
        }
    }
}

#[tonic::async_trait]
impl WorkspaceServiceApi for WorkspaceService {
    async fn list_workspaces(
        &self,
        request: Request<ListWorkspacesRequest>,
    ) -> Result<Response<ListWorkspacesResponse>, Status> {
        let span = grpc_span(&request);
        let workspaces = self.workspaces.clone();
        instrument_grpc(span, async move {
            let workspaces = workspaces
                .list_workspaces()
                .await
                .map_err(app_status)?
                .iter()
                .map(workspace_record_to_proto)
                .collect();
            Ok(Response::new(ListWorkspacesResponse { workspaces }))
        })
        .await
    }

    async fn create_workspace(
        &self,
        request: Request<CreateWorkspaceRequest>,
    ) -> Result<Response<CreateWorkspaceResponse>, Status> {
        let span = grpc_span(&request);
        let workspaces = self.workspaces.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let workspace = workspaces
                .create_workspace(&workspace_name)
                .await
                .map_err(app_status)?;
            Ok(Response::new(CreateWorkspaceResponse {
                workspace: Some(workspace_record_to_proto(&workspace)),
            }))
        })
        .await
    }

    async fn delete_workspace(
        &self,
        request: Request<DeleteWorkspaceRequest>,
    ) -> Result<Response<DeleteWorkspaceResponse>, Status> {
        let span = grpc_span(&request);
        let workspaces = self.workspaces.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            // Workspace deletion holds the exclusive app state lock across its database
            // awaits, so it must not be driven by a runtime worker.
            let workspace = run_state_locked_mutation(async move {
                workspaces.delete_workspace(&workspace_name).await
            })
            .await?;
            Ok(Response::new(DeleteWorkspaceResponse {
                workspace: Some(workspace_record_to_proto(&workspace)),
            }))
        })
        .await
    }
}

fn workspace_record_to_proto(record: &WorkspaceRecord) -> Workspace {
    workspace_to_proto(&record.name)
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use tempfile::TempDir;
    use tokio::task;

    use super::*;
    use crate::credentials::{CredentialManager, CredentialStore};
    use crate::sources::materialization::SourceDiagnosticReporter;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::workspaces::{WorkspaceLifecycleLock, WorkspaceName};

    /// `WorkspaceManager::delete_workspace` holds the exclusive app state lock across its database
    /// awaits, while every read path takes the same blocking shared lock straight from a runtime
    /// worker. Running the manager call inline on the handler's worker therefore wedges: the worker
    /// parked in `flock(2)` is the only one that could repoll the lock holder. Routing the call
    /// through `run_state_locked_mutation` keeps it schedulable.
    ///
    /// The scenario runs on its own runtime from a helper thread so that a regression fails the
    /// test on a wall-clock deadline instead of hanging: once the sole worker is parked in
    /// `flock(2)` nothing is left to advance the runtime's timer, so `tokio::time::timeout` cannot
    /// be the backstop here.
    #[test]
    fn delete_workspace_completes_while_runtime_workers_block_on_the_state_lock() {
        let (done_tx, done_rx) = mpsc::channel();
        thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .expect("single-worker runtime");
            let name = runtime.block_on(delete_workspace_under_blocked_workers());
            // The receiver is gone only when the deadline below already failed the test.
            drop(done_tx.send(name));
        });

        let name = done_rx
            .recv_timeout(Duration::from_secs(20))
            .expect("workspace deletion must complete while the runtime worker is parked in flock");
        assert_eq!(name, "work");
    }

    async fn delete_workspace_under_blocked_workers() -> String {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let db = open_test_db(&layout).await;
        let manager = WorkspaceManager::new(
            config_store.clone(),
            CredentialManager::new(CredentialStore::new(layout.clone())),
            layout.clone(),
            None,
            WorkspaceLifecycleLock::default(),
            db,
            SourceDiagnosticReporter::default(),
        );
        let workspace_name = WorkspaceName::parse("work").expect("workspace");
        manager
            .create_workspace(&workspace_name)
            .await
            .expect("create workspace");
        let service = WorkspaceService::new(manager);

        let deletion_finished = Arc::new(AtomicBool::new(false));
        let reader = task::spawn(park_worker_on_the_state_lock(
            config_store,
            layout,
            Arc::clone(&deletion_finished),
        ));

        // gRPC handlers run as spawned tasks, so the handler itself must be driven by a runtime
        // worker for this to reproduce what the server does.
        let delete = task::spawn(async move {
            service
                .delete_workspace(Request::new(DeleteWorkspaceRequest {
                    workspace: Some(Workspace {
                        name: "work".to_string(),
                    }),
                }))
                .await
        });
        let response = delete
            .await
            .expect("delete task")
            .expect("delete workspace");
        deletion_finished.store(true, Ordering::Release);
        reader.await.expect("reader task");

        response
            .into_inner()
            .workspace
            .expect("deleted workspace")
            .name
    }

    /// Mimics a catalog or query read: take the blocking shared state lock directly inside an async
    /// task, so the runtime worker running it parks inside `flock(2)` for as long as the deletion
    /// holds the lock exclusively.
    async fn park_worker_on_the_state_lock(
        config_store: ConfigStore,
        layout: AppStateLayout,
        deletion_finished: Arc<AtomicBool>,
    ) {
        while !deletion_finished.load(Ordering::Acquire) {
            if state_lock_is_held_exclusively(layout.state_lock()) {
                drop(config_store.state_lock_shared().expect("shared state lock"));
                return;
            }
            task::yield_now().await;
        }
    }

    fn state_lock_is_held_exclusively(state_lock: &Path) -> bool {
        let probe = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(state_lock)
            .expect("open state lock for probing");
        match probe.try_lock_shared() {
            Ok(()) => {
                probe.unlock().expect("release probed state lock");
                false
            }
            Err(_) => true,
        }
    }

    async fn open_test_db(layout: &AppStateLayout) -> Arc<CoralDb> {
        let config = DatabaseConfig::load(layout).expect("db config");
        let DatabaseConfig::Sqlite { path } = config else {
            panic!("default test config should be sqlite");
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");
        Arc::new(db)
    }
}
