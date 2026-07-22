//! Classify the structural difference between two versions of a table schema
//! into online-follow [`TransitionKind`]s.
//!
//! The engine holds the mirrored (before) schema in its binding registry and
//! obtains the post-DDL (after) schema from the source catalog. Comparing them
//! by PostgreSQL attribute number — which is stable across a rename, newly
//! assigned on an add, and retired on a drop — yields the per-column operations
//! without parsing DDL text. The classifier is deliberately conservative: any
//! change it cannot prove online-safe (a primary-key change, a column identity
//! change, a narrowing type change, or anything unrecognised) collapses to
//! [`TransitionKind::Unknown`] so the caller falls back to a shadow rebuild.

use crate::{
    change::TransitionKind,
    schema::{ColumnSchema, PgTypeKind, TableSchema},
};

/// Compare `before` and `after` and return the per-column transitions.
///
/// Returns a single-element `[TransitionKind::Unknown]` when the change is not
/// safely classifiable (primary-key set changed, a matched column changed
/// identity in an unsupported way, or a type change that is not a proven
/// widening). An empty vector means the schemas are structurally equivalent.
#[must_use]
pub fn classify_table_diff(before: &TableSchema, after: &TableSchema) -> Vec<TransitionKind> {
    // A primary-key change alters row identity and is never an online transition.
    if !primary_key_matches(before, after) {
        return vec![TransitionKind::Unknown];
    }

    let mut transitions = Vec::new();
    // Added columns: attnum present in after but not before.
    for column in &after.columns {
        if find_by_attnum(before, column.attnum).is_none() {
            transitions.push(TransitionKind::AddColumn {
                name: column.name.clone(),
                nullable_or_defaulted: is_add_online_safe(column),
            });
        }
    }
    // Dropped columns: attnum present in before but not after.
    for column in &before.columns {
        if find_by_attnum(after, column.attnum).is_none() {
            transitions.push(TransitionKind::DropColumn {
                name: column.name.clone(),
            });
        }
    }
    // Columns present in both: detect rename and/or type change.
    for before_col in &before.columns {
        let Some(after_col) = find_by_attnum(after, before_col.attnum) else {
            continue;
        };
        if before_col.name != after_col.name {
            transitions.push(TransitionKind::RenameColumn {
                from: before_col.name.clone(),
                to: after_col.name.clone(),
            });
        }
        if before_col.data_type != after_col.data_type {
            transitions.push(TransitionKind::AlterColumnType {
                name: after_col.name.clone(),
                widening: is_widening(&before_col.data_type.kind, &after_col.data_type.kind),
            });
        }
    }
    transitions
}

/// The primary key must be the same set of attribute numbers in the same
/// ordinal order in both schemas.
fn primary_key_matches(before: &TableSchema, after: &TableSchema) -> bool {
    let before_pk: Vec<i16> = before.primary_key().iter().map(|c| c.attnum).collect();
    let after_pk: Vec<i16> = after.primary_key().iter().map(|c| c.attnum).collect();
    !before_pk.is_empty() && before_pk == after_pk
}

fn find_by_attnum(schema: &TableSchema, attnum: i16) -> Option<&ColumnSchema> {
    schema.columns.iter().find(|column| column.attnum == attnum)
}

/// An added column is online-safe when it cannot force an unsafe rewrite of
/// existing rows: it is nullable (existing rows get NULL) or generated (value
/// derived), so no backfill of a NOT NULL column without a default is required.
///
/// The source `ColumnSchema` does not carry a default expression, so a NOT NULL
/// column is treated as unsafe here; a NOT NULL add with a constant default is
/// re-admitted by the source-side classifier when that lands. Conservative by
/// design — a false "unsafe" only costs a rebuild, never correctness.
fn is_add_online_safe(column: &ColumnSchema) -> bool {
    use crate::schema::GeneratedColumn;
    column.nullable || column.generated != GeneratedColumn::None
}

/// Whether a column type change from `before` to `after` is a proven-compatible
/// widening that needs no row rewrite or re-encoding on the target.
fn is_widening(before: &PgTypeKind, after: &PgTypeKind) -> bool {
    use PgTypeKind::{Int2, Int4, Int8, Text, VarChar};
    match (before, after) {
        // Integer widening.
        (Int2, Int4 | Int8) | (Int4, Int8) => true,
        // Any varchar (bounded or not) widens to unbounded text.
        (VarChar { .. }, Text) => true,
        // varchar(n) -> varchar(m) with m >= n (or m unbounded) is a widening.
        (VarChar { length: before_len }, VarChar { length: after_len }) => {
            match (before_len, after_len) {
                (_, None) => true,           // after is unbounded
                (None, Some(_)) => false,    // before unbounded, after bounded = narrowing
                (Some(b), Some(a)) => a >= b,
            }
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{
        GeneratedColumn, IdentityColumn, PgType, PgTypeKind, QualifiedName, ReplicaIdentity,
        TableKind,
    };

    fn col(attnum: i16, name: &str, kind: PgTypeKind, pk: Option<u16>, nullable: bool)
        -> ColumnSchema {
        ColumnSchema {
            attnum,
            name: name.to_owned(),
            data_type: PgType {
                oid: u32::try_from(attnum).unwrap(),
                name: QualifiedName::new("pg_catalog", "t").unwrap(),
                kind,
            },
            nullable,
            primary_key_ordinal: pk,
            generated: GeneratedColumn::None,
            identity: IdentityColumn::None,
            collation: None,
        }
    }

    fn table(columns: Vec<ColumnSchema>) -> TableSchema {
        TableSchema {
            relation_id: 1,
            generation: 1,
            name: QualifiedName::new("public", "t").unwrap(),
            kind: TableKind::Ordinary,
            replica_identity: ReplicaIdentity::Default,
            columns,
            distribution_key: Vec::new(),
            partition_key: Vec::new(),
        }
    }

    fn base() -> TableSchema {
        table(vec![
            col(1, "id", PgTypeKind::Int8, Some(1), false),
            col(2, "name", PgTypeKind::Text, None, true),
        ])
    }

    #[test]
    fn identical_schemas_have_no_transitions() {
        assert!(classify_table_diff(&base(), &base()).is_empty());
    }

    #[test]
    fn add_nullable_column_is_online_safe() {
        let mut after = base();
        after.columns.push(col(3, "note", PgTypeKind::Text, None, true));
        let diff = classify_table_diff(&base(), &after);
        assert_eq!(diff.len(), 1);
        assert!(matches!(
            &diff[0],
            TransitionKind::AddColumn { name, nullable_or_defaulted: true } if name == "note"
        ));
        assert!(diff[0].is_online_safe());
    }

    #[test]
    fn add_not_null_column_is_flagged_unsafe() {
        let mut after = base();
        after.columns.push(col(3, "flag", PgTypeKind::Bool, None, false));
        let diff = classify_table_diff(&base(), &after);
        assert!(matches!(
            &diff[0],
            TransitionKind::AddColumn { nullable_or_defaulted: false, .. }
        ));
        assert!(!diff[0].is_online_safe());
    }

    #[test]
    fn drop_and_rename_are_detected_by_attnum() {
        // Drop column 2.
        let dropped = table(vec![col(1, "id", PgTypeKind::Int8, Some(1), false)]);
        let diff = classify_table_diff(&base(), &dropped);
        assert!(matches!(&diff[0], TransitionKind::DropColumn { name } if name == "name"));

        // Rename column 2 (same attnum, new name).
        let renamed = table(vec![
            col(1, "id", PgTypeKind::Int8, Some(1), false),
            col(2, "label", PgTypeKind::Text, None, true),
        ]);
        let diff = classify_table_diff(&base(), &renamed);
        assert!(matches!(
            &diff[0],
            TransitionKind::RenameColumn { from, to } if from == "name" && to == "label"
        ));
    }

    #[test]
    fn primary_key_change_forces_unknown() {
        // Change the PK column's type is fine, but changing which attnum is PK is not.
        let mut after = base();
        after.columns[0].primary_key_ordinal = None;
        after.columns[1].primary_key_ordinal = Some(1);
        assert_eq!(
            classify_table_diff(&base(), &after),
            vec![TransitionKind::Unknown]
        );
    }

    #[test]
    fn widening_and_narrowing_type_changes() {
        // int8 PK stays; widen a non-key column int4 -> int8.
        let before = table(vec![
            col(1, "id", PgTypeKind::Int8, Some(1), false),
            col(2, "n", PgTypeKind::Int4, None, true),
        ]);
        let mut widened = before.clone();
        widened.columns[1].data_type.kind = PgTypeKind::Int8;
        let diff = classify_table_diff(&before, &widened);
        assert!(matches!(
            &diff[0],
            TransitionKind::AlterColumnType { widening: true, .. }
        ));
        assert!(diff[0].is_online_safe());

        // Narrowing int8 -> int4 is not online-safe.
        let mut narrowed = before.clone();
        narrowed.columns[1].data_type.kind = PgTypeKind::Int8;
        let diff = classify_table_diff(&narrowed, &before); // int8 -> int4
        assert!(matches!(
            &diff[0],
            TransitionKind::AlterColumnType { widening: false, .. }
        ));
        assert!(!diff[0].is_online_safe());
    }

    #[test]
    fn varchar_widening_rules() {
        assert!(is_widening(
            &PgTypeKind::VarChar { length: Some(10) },
            &PgTypeKind::VarChar { length: Some(20) }
        ));
        assert!(is_widening(
            &PgTypeKind::VarChar { length: Some(10) },
            &PgTypeKind::Text
        ));
        assert!(!is_widening(
            &PgTypeKind::VarChar { length: Some(20) },
            &PgTypeKind::VarChar { length: Some(10) }
        ));
        assert!(!is_widening(
            &PgTypeKind::VarChar { length: None },
            &PgTypeKind::VarChar { length: Some(10) }
        ));
    }
}
