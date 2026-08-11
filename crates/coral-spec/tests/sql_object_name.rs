//! Integration coverage for the public SQL object coordinate type.

#![allow(
    unused_crate_dependencies,
    reason = "Integration tests inherit the library crate's dependency set and intentionally exercise only a subset of it."
)]

use coral_spec::SqlObjectName;

#[test]
fn projection_sql_object_name_exposes_all_three_normalized_coordinates() {
    let name = SqlObjectName::new("github_v4", "issues", "list_for_repo");

    assert_eq!(name.catalog_name(), "github_v4");
    assert_eq!(name.schema_name(), "issues");
    assert_eq!(name.name(), "list_for_repo");
}
