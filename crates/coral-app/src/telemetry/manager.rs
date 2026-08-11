//! App-level orchestration for local trace inspection.

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::local_store::{
    TraceDetailRecord, TraceStore, TraceStoreError, TraceSummaryRecord, workspace_attribute,
};
use crate::search::response_history::retention_cutoff_unix_nanos;
use crate::state::db::{CoralDb, TraceSearchResponseRecord, now_unix_nanos_i64};
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TraceListView {
    All,
    QueryStream,
}

#[derive(Debug)]
pub(crate) struct ListTracesQuery {
    pub(crate) view: TraceListView,
    pub(crate) workspace: Option<WorkspaceName>,
    pub(crate) page_size: usize,
    pub(crate) offset: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TraceListPage {
    pub(crate) traces: Vec<TraceSummaryRecord>,
    pub(crate) next_offset: Option<usize>,
}

#[derive(Debug)]
pub(crate) struct GetTraceQuery {
    pub(crate) trace_id: String,
    pub(crate) workspace: Option<WorkspaceName>,
    pub(crate) view: TraceListView,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) enum RetainedSearchResponse {
    Response(Vec<u8>),
    TooLarge,
}

impl fmt::Debug for RetainedSearchResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Response(response_proto) => formatter
                .debug_struct("Response")
                .field("response_proto_bytes", &response_proto.len())
                .finish(),
            Self::TooLarge => formatter.write_str("TooLarge"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TraceDetail {
    pub(crate) trace: TraceDetailRecord,
    pub(crate) search_response: Option<RetainedSearchResponse>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TraceManagerError {
    #[error("trace '{trace_id}' not found")]
    NotFound { trace_id: String },
    #[error(transparent)]
    Store(TraceStoreError),
}

impl From<TraceStoreError> for TraceManagerError {
    fn from(error: TraceStoreError) -> Self {
        match error {
            TraceStoreError::NotFound(trace_id) => Self::NotFound { trace_id },
            error => Self::Store(error),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TraceManager {
    retention: Duration,
    search_response_history: Option<SearchResponseHistoryReader>,
    traces: TraceStore,
}

#[derive(Debug, Clone)]
struct SearchResponseHistoryReader {
    db: Arc<CoralDb>,
    retention: Duration,
    warnings: Arc<SearchResponseHistoryReadWarnings>,
}

#[derive(Debug, Default)]
struct SearchResponseHistoryReadWarnings {
    invalid_clock: AtomicBool,
    read_failure: AtomicBool,
}

impl TraceManager {
    pub(crate) fn new(trace_store_dir: PathBuf, retention: Duration) -> Self {
        Self {
            retention,
            search_response_history: None,
            traces: TraceStore::with_retention(trace_store_dir, retention),
        }
    }

    pub(crate) fn with_search_response_history(mut self, db: Arc<CoralDb>) -> Self {
        self.search_response_history = Some(SearchResponseHistoryReader {
            db,
            retention: self.retention,
            warnings: Arc::new(SearchResponseHistoryReadWarnings::default()),
        });
        self
    }

    pub(crate) async fn list_traces(
        &self,
        query: ListTracesQuery,
    ) -> Result<TraceListPage, TraceManagerError> {
        let ListTracesQuery {
            view,
            workspace,
            page_size,
            offset,
        } = query;
        let workspace = workspace.map(|workspace| workspace.as_str().to_string());
        let fetch_limit = page_size.saturating_add(1);
        let mut traces = match view {
            TraceListView::All => match workspace {
                Some(workspace) => {
                    self.traces
                        .list_traces_for_workspace(fetch_limit, offset, workspace)
                        .await
                }
                None => self.traces.list_traces(fetch_limit, offset).await,
            },
            TraceListView::QueryStream => {
                self.traces
                    .list_query_stream(fetch_limit, offset, workspace)
                    .await
            }
        }?;
        let next_offset = (traces.len() > page_size).then(|| offset.saturating_add(page_size));
        if next_offset.is_some() {
            traces.truncate(page_size);
        }
        Ok(TraceListPage {
            traces,
            next_offset,
        })
    }

    pub(crate) async fn get_trace(
        &self,
        query: GetTraceQuery,
    ) -> Result<TraceDetail, TraceManagerError> {
        let GetTraceQuery {
            trace_id,
            workspace,
            view,
        } = query;
        let workspace = workspace.map(|workspace| workspace.as_str().to_string());
        let trace = match view {
            TraceListView::All => match workspace.as_deref() {
                Some(workspace) => {
                    self.traces
                        .get_trace_for_workspace(trace_id.clone(), workspace.to_string())
                        .await
                }
                None => self.traces.get_trace(trace_id.clone()).await,
            },
            TraceListView::QueryStream => {
                self.traces
                    .get_query_stream_trace(trace_id, workspace.clone())
                    .await
            }
        }
        .map_err(TraceManagerError::from)?;
        let search_response = self
            .retained_search_response(&trace, view, workspace.as_deref())
            .await;
        Ok(TraceDetail {
            trace,
            search_response,
        })
    }

    async fn retained_search_response(
        &self,
        trace: &TraceDetailRecord,
        view: TraceListView,
        workspace: Option<&str>,
    ) -> Option<RetainedSearchResponse> {
        let reader = self.search_response_history.as_ref()?;
        let workspace = workspace?;
        let search_span = selected_search_execution(trace, view, workspace)?;
        let now_unix_nanos = match now_unix_nanos_i64() {
            Ok(now) => {
                reader
                    .warnings
                    .invalid_clock
                    .store(false, Ordering::Relaxed);
                now
            }
            Err(error) => {
                if !reader.warnings.invalid_clock.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        error = ?error,
                        "failed to apply Search response history retention"
                    );
                }
                return None;
            }
        };
        let cutoff_unix_nanos = retention_cutoff_unix_nanos(now_unix_nanos, reader.retention);
        let record = match reader
            .db
            .get_trace_search_response(
                workspace,
                &trace.summary.trace_id,
                &search_span.span_id,
                cutoff_unix_nanos,
            )
            .await
        {
            Ok(record) => {
                reader.warnings.read_failure.store(false, Ordering::Relaxed);
                record?
            }
            Err(error) => {
                if !reader.warnings.read_failure.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        error = ?error,
                        workspace,
                        trace_id = %trace.summary.trace_id,
                        search_span_id = %search_span.span_id,
                        "failed to read Search response history"
                    );
                }
                return None;
            }
        };
        retained_search_response(record)
    }
}

fn retained_search_response(record: TraceSearchResponseRecord) -> Option<RetainedSearchResponse> {
    match (record.response_proto, record.oversized_bytes) {
        (Some(response), None) => Some(RetainedSearchResponse::Response(response)),
        (None, Some(_oversized_bytes)) => Some(RetainedSearchResponse::TooLarge),
        (Some(_), Some(_)) | (None, None) => {
            tracing::debug!("ignored inconsistent Search response history outcome");
            None
        }
    }
}

fn selected_search_execution<'a>(
    trace: &'a TraceDetailRecord,
    view: TraceListView,
    workspace: &str,
) -> Option<&'a super::local_store::TraceSpanRecord> {
    use super::local_store::{StoredTraceInvocationKind, StoredTraceOperationKind};

    if view != TraceListView::QueryStream
        || trace.summary.operation_kind != StoredTraceOperationKind::Search
    {
        return None;
    }

    let selected = match trace.summary.invocation_kind {
        StoredTraceInvocationKind::Direct => trace
            .spans
            .iter()
            .find(|span| span.span_id == trace.summary.root_span_id && span.name == "coral.search"),
        StoredTraceInvocationKind::Mcp => nearest_reachable_search_descendant(trace),
        StoredTraceInvocationKind::Unspecified => None,
    }?;
    workspace_attribute(&selected.attributes_json)
        .is_some_and(|span_workspace| span_workspace == workspace)
        .then_some(selected)
}

fn nearest_reachable_search_descendant(
    trace: &TraceDetailRecord,
) -> Option<&super::local_store::TraceSpanRecord> {
    // Query Stream attributes descendants of an explicit MCP entry to that root. Detail records do
    // not retain projector owner IDs, so recover the Search execution through the same parent edges.
    let spans_by_id = trace
        .spans
        .iter()
        .map(|span| (span.span_id.as_str(), span))
        .collect::<HashMap<_, _>>();
    trace
        .spans
        .iter()
        .filter(|span| span.name == "coral.search")
        .filter_map(|span| {
            descendant_depth(span, &trace.summary.root_span_id, &spans_by_id)
                .map(|depth| (depth, span))
        })
        .min_by(|(left_depth, left), (right_depth, right)| {
            left_depth
                .cmp(right_depth)
                .then_with(|| left.start_time_unix_nanos.cmp(&right.start_time_unix_nanos))
                .then_with(|| left.span_id.cmp(&right.span_id))
        })
        .map(|(_depth, span)| span)
}

fn descendant_depth(
    span: &super::local_store::TraceSpanRecord,
    root_span_id: &str,
    spans_by_id: &HashMap<&str, &super::local_store::TraceSpanRecord>,
) -> Option<usize> {
    if span.span_id == root_span_id {
        return Some(0);
    }
    let mut depth = 0usize;
    let mut current = span;
    let mut visited = std::collections::HashSet::new();
    while visited.insert(current.span_id.as_str()) {
        let parent_span_id = current.parent_span_id.as_deref()?;
        depth = depth.saturating_add(1);
        if parent_span_id == root_span_id {
            return Some(depth);
        }
        current = spans_by_id.get(parent_span_id)?;
    }
    None
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use serde_json::json;
    use tempfile::TempDir;

    use super::{
        GetTraceQuery, ListTracesQuery, TraceListView, TraceManager, TraceManagerError,
        selected_search_execution,
    };
    use crate::state::AppStateLayout;
    use crate::state::db::{
        CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig, TraceSearchResponseCapture,
        TraceSearchResponseInsertResult, now_unix_nanos_i64,
    };
    use crate::telemetry::local_store::{
        StoredTraceInvocationKind, StoredTraceOperationKind, StoredTraceStatus, TraceDetailRecord,
        TraceSpanRecord, TraceSummaryRecord,
    };
    use crate::workspaces::WorkspaceName;

    #[test]
    fn retained_search_response_debug_redacts_payload() {
        let payload = b"private-search-response".to_vec();
        let raw_payload_debug = format!("{payload:?}");
        let debug_output = format!("{:?}", super::RetainedSearchResponse::Response(payload));

        assert!(!debug_output.contains("private-search-response"));
        assert!(!debug_output.contains(&raw_payload_debug));
        assert!(debug_output.contains("response_proto_bytes: 23"));
    }

    #[tokio::test]
    async fn manager_scopes_and_paginates_all_trace_lists() {
        assert_manager_scopes_and_paginates_trace_lists(TraceListView::All).await;
    }

    #[tokio::test]
    async fn manager_scopes_and_paginates_query_stream_trace_lists() {
        assert_manager_scopes_and_paginates_trace_lists(TraceListView::QueryStream).await;
    }

    async fn assert_manager_scopes_and_paginates_trace_lists(view: TraceListView) {
        let (_temp, manager) = trace_manager_fixture();
        let alpha = WorkspaceName::parse("alpha").expect("alpha workspace");

        let first_page = manager
            .list_traces(ListTracesQuery {
                view,
                workspace: Some(alpha.clone()),
                page_size: 1,
                offset: 0,
            })
            .await
            .expect("first trace page");
        assert_eq!(first_page.traces.len(), 1);
        assert_eq!(
            first_page.traces.first().expect("first trace").trace_id,
            "alpha-new"
        );
        assert_eq!(first_page.next_offset, Some(1));

        let second_page = manager
            .list_traces(ListTracesQuery {
                view,
                workspace: Some(alpha),
                page_size: 1,
                offset: 1,
            })
            .await
            .expect("second trace page");
        assert_eq!(second_page.traces.len(), 1);
        assert_eq!(
            second_page.traces.first().expect("second trace").trace_id,
            "alpha-old"
        );
        assert_eq!(second_page.next_offset, None);
    }

    #[tokio::test]
    async fn manager_applies_workspace_scope_to_trace_detail() {
        let (_temp, manager) = trace_manager_fixture();

        let detail = manager
            .get_trace(GetTraceQuery {
                trace_id: "beta".to_string(),
                workspace: None,
                view: TraceListView::All,
            })
            .await
            .expect("unscoped beta trace");
        assert_eq!(detail.trace.summary.trace_id, "beta");

        let error = manager
            .get_trace(GetTraceQuery {
                trace_id: "beta".to_string(),
                workspace: Some(WorkspaceName::parse("alpha").expect("alpha workspace")),
                view: TraceListView::All,
            })
            .await
            .expect_err("beta trace must not match alpha workspace");
        assert!(matches!(
            error,
            TraceManagerError::NotFound { trace_id } if trace_id == "beta"
        ));
    }

    #[test]
    fn direct_search_uses_only_the_selected_root_and_requires_its_workspace() {
        let trace = search_trace(
            StoredTraceInvocationKind::Direct,
            "search-root",
            vec![search_span("search-root", None, 10, "alpha")],
        );
        assert_eq!(
            selected_search_execution(&trace, TraceListView::QueryStream, "alpha")
                .map(|span| span.span_id.as_str()),
            Some("search-root")
        );
        assert_eq!(
            selected_search_execution(&trace, TraceListView::QueryStream, "beta"),
            None
        );
        assert_eq!(
            selected_search_execution(&trace, TraceListView::All, "alpha"),
            None
        );
    }

    #[test]
    fn mcp_search_selection_follows_parent_edges_and_is_deterministic() {
        let mut remote_bridge =
            operation_span("bridge", Some("tool-root"), "grpc.server", 2, "alpha");
        remote_bridge.parent_span_is_remote = true;
        let trace = search_trace(
            StoredTraceInvocationKind::Mcp,
            "tool-root",
            vec![
                operation_span(
                    "tool-root",
                    Some("remote-parent"),
                    "coral.mcp.call_tool",
                    1,
                    "alpha",
                ),
                remote_bridge,
                search_span("z-search", Some("bridge"), 10, "alpha"),
                search_span("a-search", Some("bridge"), 10, "alpha"),
                search_span("earlier-search", Some("bridge"), 9, "alpha"),
                search_span("sibling-search", Some("other-root"), 0, "alpha"),
            ],
        );

        assert_eq!(
            selected_search_execution(&trace, TraceListView::QueryStream, "alpha")
                .map(|span| span.span_id.as_str()),
            Some("earlier-search")
        );
    }

    #[test]
    fn nearest_mcp_search_workspace_mismatch_does_not_fall_through() {
        let trace = search_trace(
            StoredTraceInvocationKind::Mcp,
            "tool-root",
            vec![
                operation_span("tool-root", None, "coral.mcp.call_tool", 1, "alpha"),
                search_span("nearest-wrong-workspace", Some("tool-root"), 2, "beta"),
                operation_span("bridge", Some("tool-root"), "grpc.client", 3, "alpha"),
                search_span("deeper-right-workspace", Some("bridge"), 4, "alpha"),
            ],
        );

        assert_eq!(
            selected_search_execution(&trace, TraceListView::QueryStream, "alpha"),
            None
        );
    }

    #[tokio::test]
    async fn mcp_remote_parent_reads_only_the_selected_search_response() {
        let temp = TempDir::new().expect("temp dir");
        let db = open_test_db(&temp).await;
        let mut tx = db.begin().await.expect("workspace tx");
        tx.workspaces().ensure("alpha", 1).await.expect("workspace");
        tx.commit().await.expect("commit workspace");
        let manager = TraceManager::new(temp.path().join("trace-store"), Duration::from_hours(1))
            .with_search_response_history(Arc::clone(&db));

        let mut remote_trace = search_trace(
            StoredTraceInvocationKind::Mcp,
            "tool-root",
            vec![
                operation_span(
                    "tool-root",
                    Some("remote-parent"),
                    "coral.mcp.call_tool",
                    1,
                    "alpha",
                ),
                search_span("selected-search", Some("tool-root"), 2, "alpha"),
            ],
        );
        remote_trace.summary.trace_id = "remote-trace".to_string();
        insert_history_row(&db, "remote-trace", "selected-search", vec![1, 2, 3]).await;
        assert_eq!(
            manager
                .retained_search_response(&remote_trace, TraceListView::QueryStream, Some("alpha"),)
                .await,
            Some(super::RetainedSearchResponse::Response(vec![1, 2, 3]))
        );

        let mut no_fallback_trace = search_trace(
            StoredTraceInvocationKind::Mcp,
            "tool-root",
            vec![
                operation_span("tool-root", None, "coral.mcp.call_tool", 1, "alpha"),
                search_span("selected-without-row", Some("tool-root"), 2, "alpha"),
                operation_span("bridge", Some("tool-root"), "grpc.client", 3, "alpha"),
                search_span("deeper-with-row", Some("bridge"), 4, "alpha"),
            ],
        );
        no_fallback_trace.summary.trace_id = "no-fallback-trace".to_string();
        insert_history_row(&db, "no-fallback-trace", "deeper-with-row", vec![4, 5, 6]).await;
        assert_eq!(
            manager
                .retained_search_response(
                    &no_fallback_trace,
                    TraceListView::QueryStream,
                    Some("alpha"),
                )
                .await,
            None
        );
    }

    #[tokio::test]
    async fn retained_response_is_read_only_for_scoped_query_stream_search() {
        let temp = TempDir::new().expect("temp dir");
        let trace_store = temp.path().join("trace-store");
        std::fs::create_dir_all(&trace_store).expect("trace store dir");
        let record = search_trace_record("search-trace", "alpha", 10, 20);
        let expired_record = search_trace_record("expired-search-trace", "alpha", 30, 40);
        let oversized_record = search_trace_record("oversized-search-trace", "alpha", 50, 60);
        std::fs::write(
            trace_store.join("spans-search.jsonl"),
            format!("{record}\n{expired_record}\n{oversized_record}\n"),
        )
        .expect("write search trace");

        let db = open_test_db(&temp).await;
        let mut tx = db.begin().await.expect("workspace tx");
        tx.workspaces().ensure("alpha", 1).await.expect("workspace");
        tx.commit().await.expect("commit workspace");
        insert_history_row(&db, "search-trace", "search-trace-span", vec![1, 2, 3]).await;
        let two_hours_nanos =
            i64::try_from(Duration::from_hours(2).as_nanos()).expect("duration fits i64");
        insert_history_row_at(
            &db,
            "expired-search-trace",
            "expired-search-trace-span",
            vec![4, 5, 6],
            now_unix_nanos_i64()
                .expect("current time")
                .saturating_sub(two_hours_nanos),
        )
        .await;
        insert_oversized_history_row(
            &db,
            "oversized-search-trace",
            "oversized-search-trace-span",
            1_048_577,
        )
        .await;
        let manager = TraceManager::new(trace_store, Duration::from_hours(1))
            .with_search_response_history(Arc::clone(&db));

        let scoped = manager
            .get_trace(GetTraceQuery {
                trace_id: "search-trace".to_string(),
                workspace: Some(WorkspaceName::parse("alpha").expect("workspace")),
                view: TraceListView::QueryStream,
            })
            .await
            .expect("scoped Query Stream detail");
        assert_eq!(
            scoped.search_response,
            Some(super::RetainedSearchResponse::Response(vec![1, 2, 3]))
        );

        let expired = manager
            .get_trace(GetTraceQuery {
                trace_id: "expired-search-trace".to_string(),
                workspace: Some(WorkspaceName::parse("alpha").expect("workspace")),
                view: TraceListView::QueryStream,
            })
            .await
            .expect("expired response does not hide trace detail");
        assert_eq!(expired.search_response, None);
        assert_eq!(expired.trace.spans.len(), 1);

        let oversized = manager
            .get_trace(GetTraceQuery {
                trace_id: "oversized-search-trace".to_string(),
                workspace: Some(WorkspaceName::parse("alpha").expect("workspace")),
                view: TraceListView::QueryStream,
            })
            .await
            .expect("oversized response does not hide trace detail");
        assert_eq!(
            oversized.search_response,
            Some(super::RetainedSearchResponse::TooLarge)
        );
        assert_eq!(oversized.trace.spans.len(), 1);

        assert_history_absent_for_unscoped_views(&manager).await;

        db.drop_trace_search_responses_for_test()
            .await
            .expect("force response history read failure");
        let read_failure = manager
            .get_trace(GetTraceQuery {
                trace_id: "search-trace".to_string(),
                workspace: Some(WorkspaceName::parse("alpha").expect("workspace")),
                view: TraceListView::QueryStream,
            })
            .await
            .expect("response history read failure does not hide trace detail");
        assert_eq!(read_failure.search_response, None);
        assert_eq!(read_failure.trace.spans.len(), 1);
        assert!(
            manager
                .search_response_history
                .as_ref()
                .expect("history reader")
                .warnings
                .read_failure
                .load(std::sync::atomic::Ordering::Relaxed)
        );
    }

    async fn assert_history_absent_for_unscoped_views(manager: &TraceManager) {
        for (view, workspace) in [
            (TraceListView::QueryStream, None),
            (
                TraceListView::All,
                Some(WorkspaceName::parse("alpha").expect("workspace")),
            ),
        ] {
            let detail = manager
                .get_trace(GetTraceQuery {
                    trace_id: "search-trace".to_string(),
                    workspace,
                    view,
                })
                .await
                .expect("detail remains available");
            assert_eq!(detail.search_response, None);
            assert_eq!(detail.trace.spans.len(), 1);
        }
    }

    fn trace_manager_fixture() -> (TempDir, TraceManager) {
        let temp = TempDir::new().expect("temp dir");
        let trace_store = temp.path().join("trace-store");
        std::fs::create_dir_all(&trace_store).expect("trace store dir");
        let records = [
            trace_record("alpha-old", "alpha", 10, 20),
            trace_record("alpha-new", "alpha", 30, 40),
            trace_record("beta", "beta", 50, 60),
        ];
        let lines = records
            .into_iter()
            .map(|record| record.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(trace_store.join("spans-test.jsonl"), format!("{lines}\n"))
            .expect("write trace records");
        (temp, TraceManager::new(trace_store, Duration::from_mins(1)))
    }

    async fn open_test_db(temp: &TempDir) -> Arc<CoralDb> {
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config")
        else {
            panic!("default test database must be SQLite");
        };
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
                .await
                .expect("open SQLite"),
        );
        db.migrate().await.expect("migrate SQLite");
        db
    }

    async fn insert_history_row(
        db: &CoralDb,
        trace_id: &str,
        search_span_id: &str,
        response_proto: Vec<u8>,
    ) {
        insert_history_row_at(
            db,
            trace_id,
            search_span_id,
            response_proto,
            now_unix_nanos_i64().expect("current time"),
        )
        .await;
    }

    async fn insert_history_row_at(
        db: &CoralDb,
        trace_id: &str,
        search_span_id: &str,
        response_proto: Vec<u8>,
        recorded_at_unix_nanos: i64,
    ) {
        assert_eq!(
            db.insert_trace_search_response(TraceSearchResponseCapture {
                workspace_id: "alpha".to_string(),
                trace_id: trace_id.to_string(),
                search_span_id: search_span_id.to_string(),
                recorded_at_unix_nanos,
                response_proto: Some(response_proto),
                oversized_bytes: None,
            })
            .await
            .expect("insert Search response history"),
            TraceSearchResponseInsertResult::Inserted
        );
    }

    async fn insert_oversized_history_row(
        db: &CoralDb,
        trace_id: &str,
        search_span_id: &str,
        oversized_bytes: i64,
    ) {
        assert_eq!(
            db.insert_trace_search_response(TraceSearchResponseCapture {
                workspace_id: "alpha".to_string(),
                trace_id: trace_id.to_string(),
                search_span_id: search_span_id.to_string(),
                recorded_at_unix_nanos: now_unix_nanos_i64().expect("current time"),
                response_proto: None,
                oversized_bytes: Some(oversized_bytes),
            })
            .await
            .expect("insert oversized Search response history"),
            TraceSearchResponseInsertResult::Inserted
        );
    }

    fn search_trace(
        invocation_kind: StoredTraceInvocationKind,
        root_span_id: &str,
        spans: Vec<TraceSpanRecord>,
    ) -> TraceDetailRecord {
        TraceDetailRecord {
            summary: TraceSummaryRecord {
                trace_id: "trace".to_string(),
                root_span_id: root_span_id.to_string(),
                name: "Search".to_string(),
                query: "repository search".to_string(),
                status: StoredTraceStatus::Ok,
                start_time_unix_nanos: 1,
                end_time_unix_nanos: 20,
                duration_nanos: 19,
                span_count: u32::try_from(spans.len()).expect("span count"),
                row_count: 0,
                row_count_recorded: false,
                operation_kind: StoredTraceOperationKind::Search,
                operation_name: "search".to_string(),
                invocation_kind,
            },
            spans,
        }
    }

    fn search_span(
        span_id: &str,
        parent_span_id: Option<&str>,
        start_time_unix_nanos: i64,
        workspace: &str,
    ) -> TraceSpanRecord {
        operation_span(
            span_id,
            parent_span_id,
            "coral.search",
            start_time_unix_nanos,
            workspace,
        )
    }

    fn operation_span(
        span_id: &str,
        parent_span_id: Option<&str>,
        name: &str,
        start_time_unix_nanos: i64,
        workspace: &str,
    ) -> TraceSpanRecord {
        TraceSpanRecord {
            trace_id: "trace".to_string(),
            span_id: span_id.to_string(),
            parent_span_id: parent_span_id.map(str::to_string),
            parent_span_is_remote: parent_span_id == Some("remote-parent"),
            name: name.to_string(),
            kind: "internal".to_string(),
            status: StoredTraceStatus::Ok,
            status_message: None,
            start_time_unix_nanos,
            end_time_unix_nanos: start_time_unix_nanos + 1,
            duration_nanos: 1,
            attributes_json: json!({ "workspace": workspace }).to_string(),
            events_json: "[]".to_string(),
            links_json: "[]".to_string(),
            resource_json: "{}".to_string(),
            scope_name: "test".to_string(),
            scope_version: None,
            scope_schema_url: None,
            scope_attributes_json: "{}".to_string(),
            trace_flags: 0,
            trace_state: String::new(),
            is_remote: false,
        }
    }

    fn search_trace_record(
        trace_id: &str,
        workspace: &str,
        start_time_unix_nanos: i64,
        end_time_unix_nanos: i64,
    ) -> serde_json::Value {
        let mut record = trace_record(
            trace_id,
            workspace,
            start_time_unix_nanos,
            end_time_unix_nanos,
        );
        let object = record.as_object_mut().expect("trace record object");
        object.insert("name".to_string(), json!("coral.search"));
        object.insert(
            "attributes_json".to_string(),
            json!(
                json!({
                    "coral.stream.entry": true,
                    "coral.stream.kind": "search",
                    "coral.stream.name": "search",
                    "workspace": workspace,
                    "status": "ok",
                })
                .to_string()
            ),
        );
        record
    }

    fn trace_record(
        trace_id: &str,
        workspace: &str,
        start_time_unix_nanos: i64,
        end_time_unix_nanos: i64,
    ) -> serde_json::Value {
        json!({
            "trace_id": trace_id,
            "span_id": format!("{trace_id}-span"),
            "parent_span_id": null,
            "parent_span_is_remote": false,
            "name": "coral.query",
            "kind": "internal",
            "status": "ok",
            "status_message": null,
            "start_time_unix_nanos": start_time_unix_nanos,
            "end_time_unix_nanos": end_time_unix_nanos,
            "duration_nanos": end_time_unix_nanos - start_time_unix_nanos,
            "attributes_json": json!({
                "coral.stream.entry": true,
                "coral.stream.kind": "query",
                "coral.stream.name": trace_id,
                "workspace": workspace,
                "sql": format!("SELECT '{trace_id}'"),
                "status": "ok",
            }).to_string(),
            "events_json": "[]",
            "links_json": "[]",
            "resource_json": "{}",
            "scope_name": "test",
            "scope_version": null,
            "scope_schema_url": null,
            "scope_attributes_json": "{}",
            "trace_flags": 0,
            "trace_state": "",
            "is_remote": false
        })
    }
}
