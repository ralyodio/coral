use std::fmt;

use sea_query::{Expr, ExprTrait, OnConflict, Order, Query};

use crate::state::db::schema::TraceSearchResponses;
use crate::state::db::{CoralTx, DbError, DbSession};

#[derive(Clone, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct TraceSearchResponseRecord {
    pub(crate) response_proto: Option<Vec<u8>>,
    pub(crate) oversized_bytes: Option<i64>,
}

impl fmt::Debug for TraceSearchResponseRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TraceSearchResponseRecord")
            .field(
                "response_proto_bytes",
                &self.response_proto.as_ref().map(Vec::len),
            )
            .field("oversized_bytes", &self.oversized_bytes)
            .finish()
    }
}

pub(crate) struct TraceSearchResponsesRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> TraceSearchResponsesRepo<'a, S>
where
    S: DbSession,
{
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    pub(crate) async fn get(
        &mut self,
        workspace_id: &str,
        trace_id: &str,
        search_span_id: &str,
        retention_cutoff_unix_nanos: i64,
    ) -> Result<Option<TraceSearchResponseRecord>, DbError> {
        let statement = Query::select()
            .columns([
                TraceSearchResponses::ResponseProto,
                TraceSearchResponses::OversizedBytes,
            ])
            .from(TraceSearchResponses::Table)
            .and_where(Expr::col(TraceSearchResponses::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(TraceSearchResponses::TraceId).eq(trace_id))
            .and_where(Expr::col(TraceSearchResponses::SearchSpanId).eq(search_span_id))
            .and_where(
                Expr::col(TraceSearchResponses::RecordedAtUnixNanos)
                    .gte(retention_cutoff_unix_nanos),
            )
            .to_owned();
        self.session.fetch_optional(statement).await
    }

    pub(crate) async fn next_expired_workspace_id(
        &mut self,
        retention_cutoff_unix_nanos: i64,
        after_workspace_id: Option<&str>,
    ) -> Result<Option<String>, DbError> {
        let statement =
            Query::select()
                .column(TraceSearchResponses::WorkspaceId)
                .from(TraceSearchResponses::Table)
                .and_where(
                    Expr::col(TraceSearchResponses::RecordedAtUnixNanos)
                        .lt(retention_cutoff_unix_nanos),
                )
                .and_where_option(after_workspace_id.map(|workspace_id| {
                    Expr::col(TraceSearchResponses::WorkspaceId).gt(workspace_id)
                }))
                .order_by(TraceSearchResponses::WorkspaceId, Order::Asc)
                .limit(1)
                .to_owned();
        let row: Option<(String,)> = self.session.fetch_optional(statement).await?;
        Ok(row.map(|(workspace_id,)| workspace_id))
    }
}

impl TraceSearchResponsesRepo<'_, CoralTx<'_>> {
    pub(crate) async fn insert_first_write_wins(
        &mut self,
        workspace_id: &str,
        trace_id: &str,
        search_span_id: &str,
        recorded_at_unix_nanos: i64,
        response_proto: Option<Vec<u8>>,
        oversized_bytes: Option<i64>,
    ) -> Result<bool, DbError> {
        let statement = Query::insert()
            .into_table(TraceSearchResponses::Table)
            .columns([
                TraceSearchResponses::WorkspaceId,
                TraceSearchResponses::TraceId,
                TraceSearchResponses::SearchSpanId,
                TraceSearchResponses::RecordedAtUnixNanos,
                TraceSearchResponses::ResponseProto,
                TraceSearchResponses::OversizedBytes,
            ])
            .values_panic([
                Expr::val(workspace_id.to_string()),
                Expr::val(trace_id.to_string()),
                Expr::val(search_span_id.to_string()),
                Expr::val(recorded_at_unix_nanos),
                Expr::val(response_proto),
                Expr::val(oversized_bytes),
            ])
            .on_conflict(
                OnConflict::columns([
                    TraceSearchResponses::WorkspaceId,
                    TraceSearchResponses::TraceId,
                    TraceSearchResponses::SearchSpanId,
                ])
                .do_nothing()
                .to_owned(),
            )
            .to_owned();
        Ok(self.session.execute_rows_affected(statement).await? == 1)
    }

    pub(crate) async fn delete_expired_batch(
        &mut self,
        workspace_id: &str,
        retention_cutoff_unix_nanos: i64,
        max_rows: u64,
    ) -> Result<u64, DbError> {
        let expired_keys = Query::select()
            .columns([
                TraceSearchResponses::TraceId,
                TraceSearchResponses::SearchSpanId,
            ])
            .from(TraceSearchResponses::Table)
            .and_where(Expr::col(TraceSearchResponses::WorkspaceId).eq(workspace_id))
            .and_where(
                Expr::col(TraceSearchResponses::RecordedAtUnixNanos)
                    .lt(retention_cutoff_unix_nanos),
            )
            .order_by(TraceSearchResponses::RecordedAtUnixNanos, Order::Asc)
            .order_by(TraceSearchResponses::TraceId, Order::Asc)
            .order_by(TraceSearchResponses::SearchSpanId, Order::Asc)
            .limit(max_rows)
            .to_owned();
        let statement = Query::delete()
            .from_table(TraceSearchResponses::Table)
            .and_where(Expr::col(TraceSearchResponses::WorkspaceId).eq(workspace_id))
            .and_where(
                Expr::tuple([
                    Expr::col(TraceSearchResponses::TraceId),
                    Expr::col(TraceSearchResponses::SearchSpanId),
                ])
                .in_subquery(expired_keys),
            )
            .to_owned();
        self.session.execute_rows_affected(statement).await
    }
}
