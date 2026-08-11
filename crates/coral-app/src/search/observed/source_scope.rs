//! Source-surface routing and opaque scope derivation for observed values.

use coral_engine::{QuerySource, RuntimeRelationKind};
use coral_spec::SqlObjectName;
use serde::Serialize;
use uuid::Uuid;

use crate::hash::sha256_hex;
use crate::search::observed::ObservedValuesLiveScope;
use crate::search::observed::sqlite_queue::ObservedValuesSurfaceKind;

const LEGACY_SOURCE_SCOPE_FORMAT_VERSION: u8 = 1;
const CATALOG_SOURCE_SCOPE_FORMAT_VERSION: u8 = 2;
#[cfg(test)]
const PRE_ACTIVATION_RUNTIME_CONTRACT_FINGERPRINT: &str = "observed-values/pre-activation/v0";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct SurfaceKey {
    pub(super) sql_name: SqlObjectName,
    pub(super) surface_kind: ObservedValuesSurfaceKind,
}

/// Opaque identity supplied by the app-owned runtime-package boundary.
///
/// The queue substrate does not interpret either value. The app-wiring PR
/// replaces the pre-activation seed with a complete runtime-contract
/// fingerprint and an app-owned credential revision.
#[derive(Debug, Clone, Copy)]
pub(super) struct SourceScopeSeed<'a> {
    runtime_contract_fingerprint: &'a str,
    credential_revision: Uuid,
}

#[cfg(test)]
impl SourceScopeSeed<'static> {
    pub(super) const PRE_ACTIVATION: Self =
        Self::new(PRE_ACTIVATION_RUNTIME_CONTRACT_FINGERPRINT, Uuid::nil());
}

impl<'a> SourceScopeSeed<'a> {
    pub(super) const fn new(
        runtime_contract_fingerprint: &'a str,
        credential_revision: Uuid,
    ) -> Self {
        Self {
            runtime_contract_fingerprint,
            credential_revision,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct ObservedSourceSurfaceScope {
    /// Installed source that owns lifecycle clears and invalidation epochs.
    pub(super) source_name: String,
    surface_key: SurfaceKey,
    pub(super) source_scope_id: String,
}

impl ObservedSourceSurfaceScope {
    pub(super) fn key(&self) -> SurfaceKey {
        self.surface_key.clone()
    }

    pub(super) fn live_scope(&self) -> ObservedValuesLiveScope {
        ObservedValuesLiveScope {
            source_name: self.source_name.clone(),
            catalog_name: coral_engine::normalize_catalog_name(Some(
                self.surface_key.sql_name.catalog_name(),
            ))
            .map(ToString::to_string),
            schema_name: self.surface_key.sql_name.schema_name().to_string(),
            source_scope_id: self.source_scope_id.clone(),
            surface_kind: self.surface_key.surface_kind,
            surface_name: self.surface_key.sql_name.name().to_string(),
        }
    }
}

pub(super) fn source_surface_scopes(
    source: &QuerySource,
    seed: SourceScopeSeed<'_>,
) -> Vec<ObservedSourceSurfaceScope> {
    let mut scopes = Vec::new();
    if let Some(catalog) = source.catalog() {
        catalog.for_each_declared_relation(|sql_name, kind| {
            push_surface_scope(source, sql_name, kind, seed, &mut scopes);
        });
    }
    scopes
}

fn push_surface_scope(
    source: &QuerySource,
    sql_name: &coral_spec::SqlObjectName,
    kind: RuntimeRelationKind,
    seed: SourceScopeSeed<'_>,
    scopes: &mut Vec<ObservedSourceSurfaceScope>,
) {
    scopes.push(surface_scope(
        source,
        sql_name,
        if kind == RuntimeRelationKind::TableFunction {
            ObservedValuesSurfaceKind::Function
        } else {
            ObservedValuesSurfaceKind::Table
        },
        seed,
    ));
}

fn surface_scope(
    source: &QuerySource,
    sql_name: &SqlObjectName,
    surface_kind: ObservedValuesSurfaceKind,
    seed: SourceScopeSeed<'_>,
) -> ObservedSourceSurfaceScope {
    let scope_bytes = scope_fingerprint_bytes(sql_name, surface_kind, seed);
    ObservedSourceSurfaceScope {
        source_name: source.source_name().to_string(),
        surface_key: SurfaceKey {
            sql_name: sql_name.clone(),
            surface_kind,
        },
        source_scope_id: sha256_hex(&scope_bytes),
    }
}

fn scope_fingerprint_bytes(
    sql_name: &SqlObjectName,
    surface_kind: ObservedValuesSurfaceKind,
    seed: SourceScopeSeed<'_>,
) -> Vec<u8> {
    if coral_engine::normalize_catalog_name(Some(sql_name.catalog_name())).is_none() {
        serde_json::to_vec(&LegacyScopeFingerprint {
            format_version: LEGACY_SOURCE_SCOPE_FORMAT_VERSION,
            runtime_contract_fingerprint: seed.runtime_contract_fingerprint,
            credential_revision: seed.credential_revision,
            component_source_name: sql_name.schema_name(),
            surface_kind: surface_kind.as_str(),
            surface_name: sql_name.name(),
        })
    } else {
        serde_json::to_vec(&CatalogScopeFingerprint {
            format_version: CATALOG_SOURCE_SCOPE_FORMAT_VERSION,
            runtime_contract_fingerprint: seed.runtime_contract_fingerprint,
            credential_revision: seed.credential_revision,
            catalog_name: sql_name.catalog_name(),
            schema_name: sql_name.schema_name(),
            surface_kind: surface_kind.as_str(),
            surface_name: sql_name.name(),
        })
    }
    .expect("observed-values source scope must serialize")
}

#[derive(Serialize)]
struct LegacyScopeFingerprint<'a> {
    format_version: u8,
    runtime_contract_fingerprint: &'a str,
    credential_revision: Uuid,
    component_source_name: &'a str,
    surface_kind: &'static str,
    surface_name: &'a str,
}

#[derive(Serialize)]
struct CatalogScopeFingerprint<'a> {
    format_version: u8,
    runtime_contract_fingerprint: &'a str,
    credential_revision: Uuid,
    catalog_name: &'a str,
    schema_name: &'a str,
    surface_kind: &'static str,
    surface_name: &'a str,
}

#[cfg(test)]
mod tests {
    use coral_spec::SqlObjectName;
    use uuid::Uuid;

    use super::{SourceScopeSeed, scope_fingerprint_bytes};
    use crate::hash::sha256_hex;
    use crate::search::observed::sqlite_queue::ObservedValuesSurfaceKind;

    /// Pins the hashed bytes across the `component_source_name` -> `source_name`
    /// Rust rename. A change here invalidates every stored observed row.
    #[test]
    fn legacy_scope_fingerprint_hash_is_stable() {
        let scope_bytes = scope_fingerprint_bytes(
            &SqlObjectName::new("datafusion", "github_v4", "issues"),
            ObservedValuesSurfaceKind::Table,
            SourceScopeSeed::new("v1:test-runtime-contract", Uuid::nil()),
        );

        assert_eq!(
            String::from_utf8(scope_bytes.clone()).expect("scope fingerprint is utf-8"),
            concat!(
                r#"{"format_version":1,"#,
                r#""runtime_contract_fingerprint":"v1:test-runtime-contract","#,
                r#""credential_revision":"00000000-0000-0000-0000-000000000000","#,
                r#""component_source_name":"github_v4","#,
                r#""surface_kind":"table","surface_name":"issues"}"#,
            )
        );
        assert_eq!(
            sha256_hex(&scope_bytes),
            "82fca0aa8c55fc4b8a22cf7a8f57a63d5acab8d7d2d84853d3f12b3b7a24886b"
        );
    }

    #[test]
    fn catalog_scope_fingerprint_includes_three_part_identity() {
        let scope_bytes = scope_fingerprint_bytes(
            &SqlObjectName::new("github_v4", "issues", "list"),
            ObservedValuesSurfaceKind::Table,
            SourceScopeSeed::new("v5:test-runtime-contract", Uuid::nil()),
        );

        assert_eq!(
            String::from_utf8(scope_bytes).expect("scope fingerprint is utf-8"),
            concat!(
                r#"{"format_version":2,"#,
                r#""runtime_contract_fingerprint":"v5:test-runtime-contract","#,
                r#""credential_revision":"00000000-0000-0000-0000-000000000000","#,
                r#""catalog_name":"github_v4","schema_name":"issues","#,
                r#""surface_kind":"table","surface_name":"list"}"#,
            )
        );
    }
}
