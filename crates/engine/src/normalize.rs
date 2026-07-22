//! Fold pgoutput row events into one current-state staging row per lineage.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use cloudberry_etl_core::{
    CoreError,
    change::{Cell, RowChange, TableChange, TransactionChange, Tuple},
    schema::TableSchema,
};
use cloudberry_etl_target_cloudberry::apply::{StageOperation, StagingRow};
use thiserror::Error;

use crate::batch::TransactionBatch;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum NormalizeError {
    #[error(transparent)]
    Schema(#[from] CoreError),
    #[error("row belongs to relation {actual}, expected relation {expected}")]
    RelationMismatch { expected: u32, actual: u32 },
    #[error("row for relation {relation_id} uses generation {actual}, expected {expected}")]
    GenerationMismatch {
        relation_id: u32,
        expected: u64,
        actual: u64,
    },
    #[error("{tuple_kind} tuple has {actual} values, expected {expected}")]
    InvalidTupleArity {
        tuple_kind: &'static str,
        expected: usize,
        actual: usize,
    },
    #[error(
        "old-key tuple has {actual} values, expected either {key_only} key values or {full} table values"
    )]
    InvalidOldKeyArity {
        key_only: usize,
        full: usize,
        actual: usize,
    },
    #[error("primary-key column `{column}` is {state} in a {tuple_kind} tuple")]
    InvalidPrimaryKey {
        column: String,
        state: &'static str,
        tuple_kind: &'static str,
    },
    #[error("column `{column}` contains binary pgoutput data, which text COPY cannot apply")]
    BinaryValue { column: String },
    #[error("inserted row is missing column `{column}`")]
    IncompleteInsert { column: String },
    #[error("ambiguous row-change sequence: {0}")]
    AmbiguousSequence(String),
    #[error("transaction change source failed: {0}")]
    ChangeSource(String),
}

/// Normalize changes for exactly one relation and schema generation.
///
/// The result contains at most one row per source lineage. It is immediately
/// consumable by `target_cloudberry::apply`: inserts are complete, stable-key
/// updates retain presence information, and primary-key changes are represented
/// as a single `Move` from the batch-start key to the final key.
pub fn normalize_table_changes<'a>(
    schema: &TableSchema,
    changes: impl IntoIterator<Item = &'a TableChange>,
) -> Result<Vec<StagingRow>, NormalizeError> {
    schema.validate_supported()?;
    let mut normalizer = Normalizer::new(schema);
    for change in changes {
        normalizer.push(change)?;
    }
    normalizer.finish()
}

/// Normalize every row for `schema` across the committed transactions in a batch.
/// Rows for other relations are intentionally ignored.
pub fn normalize_table_batch(
    schema: &TableSchema,
    batch: &TransactionBatch,
) -> Result<Vec<StagingRow>, NormalizeError> {
    let mut changes = Vec::new();
    for transaction in batch.transactions() {
        let reader = transaction
            .change_source
            .reader()
            .map_err(|error| NormalizeError::ChangeSource(error.to_string()))?;
        for change in reader {
            let change = change.map_err(|error| NormalizeError::ChangeSource(error.to_string()))?;
            if let TransactionChange::Row(change) = change
                && change.relation_id == schema.relation_id
            {
                changes.push(change);
            }
        }
    }
    normalize_table_changes(schema, &changes)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key(Vec<KeyCell>);

impl Key {
    fn cells(&self) -> Vec<Cell> {
        self.0.iter().map(KeyCell::cell).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum KeyCell {
    Text(Bytes),
}

impl KeyCell {
    fn cell(&self) -> Cell {
        match self {
            Self::Text(value) => Cell::Text(value.clone()),
        }
    }
}

#[derive(Debug)]
struct PrimaryKeyLayout {
    /// Schema column indexes ordered by primary-key ordinal.
    ordinal_indexes: Vec<usize>,
    /// For each ordinal key column, its position in a pgoutput key-only tuple.
    key_tuple_positions: Vec<usize>,
}

impl PrimaryKeyLayout {
    fn new(schema: &TableSchema) -> Self {
        let ordinal_indexes = schema
            .primary_key()
            .into_iter()
            .map(|key_column| {
                schema
                    .columns
                    .iter()
                    .position(|column| column.attnum == key_column.attnum)
                    .expect("primary-key columns belong to their schema")
            })
            .collect::<Vec<_>>();
        let schema_order = schema
            .columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| column.primary_key_ordinal.map(|_| index))
            .collect::<Vec<_>>();
        let key_tuple_positions = ordinal_indexes
            .iter()
            .map(|ordinal_index| {
                schema_order
                    .iter()
                    .position(|schema_index| schema_index == ordinal_index)
                    .expect("primary-key schema order contains every key column")
            })
            .collect();
        Self {
            ordinal_indexes,
            key_tuple_positions,
        }
    }
}

#[derive(Debug)]
struct Lineage {
    origin_key: Option<Key>,
    current_key: Option<Key>,
    cells: Vec<Cell>,
    first_sequence: usize,
}

struct Normalizer<'a> {
    schema: &'a TableSchema,
    primary_key: PrimaryKeyLayout,
    lineages: Vec<Lineage>,
    current_by_key: HashMap<Key, usize>,
    origin_by_key: HashMap<Key, usize>,
    sequence: usize,
}

impl<'a> Normalizer<'a> {
    fn new(schema: &'a TableSchema) -> Self {
        Self {
            schema,
            primary_key: PrimaryKeyLayout::new(schema),
            lineages: Vec::new(),
            current_by_key: HashMap::new(),
            origin_by_key: HashMap::new(),
            sequence: 0,
        }
    }

    fn push(&mut self, change: &TableChange) -> Result<(), NormalizeError> {
        if change.relation_id != self.schema.relation_id {
            return Err(NormalizeError::RelationMismatch {
                expected: self.schema.relation_id,
                actual: change.relation_id,
            });
        }
        if change.generation != self.schema.generation {
            return Err(NormalizeError::GenerationMismatch {
                relation_id: change.relation_id,
                expected: self.schema.generation,
                actual: change.generation,
            });
        }

        let sequence = self.sequence;
        self.sequence = self.sequence.saturating_add(1);
        match &change.change {
            RowChange::Insert { new } => self.insert(new, sequence),
            RowChange::Update { old_key, new } => self.update(old_key.as_ref(), new, sequence),
            RowChange::Delete { old_key } => self.delete(old_key, sequence),
        }
    }

    fn insert(&mut self, new: &Tuple, sequence: usize) -> Result<(), NormalizeError> {
        self.validate_new_tuple(new, "insert")?;
        if let Some((index, _)) = new
            .cells
            .iter()
            .enumerate()
            .find(|(_, cell)| matches!(cell, Cell::UnchangedToast))
        {
            return Err(NormalizeError::IncompleteInsert {
                column: self.schema.columns[index].name.clone(),
            });
        }
        let key = self.key_from_full_tuple(new, "insert")?;
        if self.current_by_key.contains_key(&key) {
            return Err(self.ambiguous("insert targets a key that is already live"));
        }

        if let Some(lineage_index) = self.origin_by_key.get(&key).copied()
            && self.lineages[lineage_index].current_key.is_none()
        {
            let lineage = &mut self.lineages[lineage_index];
            lineage.current_key = Some(key.clone());
            lineage.cells.clone_from(&new.cells);
            self.current_by_key.insert(key, lineage_index);
            return Ok(());
        }

        let lineage_index = self.lineages.len();
        self.lineages.push(Lineage {
            origin_key: None,
            current_key: Some(key.clone()),
            cells: new.cells.clone(),
            first_sequence: sequence,
        });
        self.current_by_key.insert(key, lineage_index);
        Ok(())
    }

    fn update(
        &mut self,
        old_tuple: Option<&Tuple>,
        new: &Tuple,
        sequence: usize,
    ) -> Result<(), NormalizeError> {
        self.validate_new_tuple(new, "update")?;
        let old_key = old_tuple
            .map(|tuple| self.key_from_old_tuple(tuple))
            .transpose()?;
        let new_key = self.key_from_update_tuple(new, old_key.as_ref())?;
        let lookup_key = old_key.as_ref().unwrap_or(&new_key);

        let lineage_index = match self.current_by_key.get(lookup_key).copied() {
            Some(index) => index,
            None => {
                if self.origin_by_key.contains_key(lookup_key) {
                    return Err(self.ambiguous(
                        "update references a key whose baseline lineage has already moved or died",
                    ));
                }
                self.new_baseline(lookup_key.clone(), sequence, true)
            }
        };

        {
            let lineage = &mut self.lineages[lineage_index];
            for (stored, incoming) in lineage.cells.iter_mut().zip(&new.cells) {
                if !matches!(incoming, Cell::UnchangedToast) {
                    stored.clone_from(incoming);
                }
            }
        }
        let final_key = self.key_from_cells(&self.lineages[lineage_index].cells, "update")?;
        let previous_key = self.lineages[lineage_index]
            .current_key
            .as_ref()
            .expect("updates operate on a live lineage")
            .clone();

        if final_key != previous_key {
            if self
                .current_by_key
                .get(&final_key)
                .is_some_and(|other| *other != lineage_index)
            {
                return Err(self.ambiguous("primary-key update targets another live lineage"));
            }
            if self
                .origin_by_key
                .get(&final_key)
                .is_some_and(|other| *other != lineage_index)
            {
                let origin_lineage = self.origin_by_key[&final_key];
                if self.lineages[origin_lineage].current_key.as_ref() == Some(&final_key) {
                    return Err(self.ambiguous("primary-key update targets another live lineage"));
                }
            }
            self.current_by_key.remove(&previous_key);
            self.current_by_key.insert(final_key.clone(), lineage_index);
            self.lineages[lineage_index].current_key = Some(final_key);
        }
        Ok(())
    }

    fn delete(&mut self, old_tuple: &Tuple, sequence: usize) -> Result<(), NormalizeError> {
        let key = self.key_from_old_tuple(old_tuple)?;
        let lineage_index = match self.current_by_key.remove(&key) {
            Some(index) => index,
            None => {
                if self.origin_by_key.contains_key(&key) {
                    return Err(self.ambiguous(
                        "delete references a key whose baseline lineage has already moved or died",
                    ));
                }
                self.new_baseline(key.clone(), sequence, false)
            }
        };
        self.lineages[lineage_index].current_key = None;
        Ok(())
    }

    fn new_baseline(&mut self, key: Key, sequence: usize, live: bool) -> usize {
        let lineage_index = self.lineages.len();
        let mut cells = vec![Cell::UnchangedToast; self.schema.columns.len()];
        self.write_key(&mut cells, &key);
        self.lineages.push(Lineage {
            origin_key: Some(key.clone()),
            current_key: live.then(|| key.clone()),
            cells,
            first_sequence: sequence,
        });
        self.origin_by_key.insert(key.clone(), lineage_index);
        if live {
            self.current_by_key.insert(key, lineage_index);
        }
        lineage_index
    }

    fn finish(mut self) -> Result<Vec<StagingRow>, NormalizeError> {
        let baseline_keys = self.origin_by_key.keys().cloned().collect::<HashSet<_>>();
        let moved_destinations = self
            .lineages
            .iter()
            .filter_map(
                |lineage| match (&lineage.origin_key, &lineage.current_key) {
                    (Some(origin), Some(current)) if origin != current => Some(current.clone()),
                    _ => None,
                },
            )
            .collect::<HashSet<_>>();
        self.lineages.sort_by_key(|lineage| lineage.first_sequence);
        let lineages = std::mem::take(&mut self.lineages);
        lineages
            .into_iter()
            .filter_map(|lineage| match (lineage.origin_key, lineage.current_key) {
                (None, None) => None,
                (None, Some(current)) => Some(self.inserted_staging_row(
                    lineage.cells,
                    if baseline_keys.contains(&current) {
                        StageOperation::Upsert
                    } else {
                        StageOperation::Insert
                    },
                )),
                (Some(origin), Some(current)) if origin == current => Some(Ok(StagingRow {
                    operation: StageOperation::Upsert,
                    cells: lineage.cells,
                    old_key: None,
                })),
                (Some(origin), Some(_)) => Some(Ok(StagingRow {
                    operation: StageOperation::Move,
                    cells: lineage.cells,
                    old_key: Some(origin.cells()),
                })),
                (Some(origin), None)
                    if self.current_by_key.contains_key(&origin)
                        && !moved_destinations.contains(&origin) =>
                {
                    None
                }
                (Some(origin), None) => {
                    let mut cells = vec![Cell::UnchangedToast; self.schema.columns.len()];
                    self.write_key(&mut cells, &origin);
                    Some(Ok(StagingRow {
                        operation: StageOperation::Delete,
                        cells,
                        old_key: None,
                    }))
                }
            })
            .collect()
    }

    fn inserted_staging_row(
        &self,
        cells: Vec<Cell>,
        operation: StageOperation,
    ) -> Result<StagingRow, NormalizeError> {
        if let Some((index, _)) = cells
            .iter()
            .enumerate()
            .find(|(_, cell)| matches!(cell, Cell::UnchangedToast))
        {
            return Err(NormalizeError::IncompleteInsert {
                column: self.schema.columns[index].name.clone(),
            });
        }
        Ok(StagingRow {
            operation,
            cells,
            old_key: None,
        })
    }

    fn validate_new_tuple(
        &self,
        tuple: &Tuple,
        tuple_kind: &'static str,
    ) -> Result<(), NormalizeError> {
        if tuple.cells.len() != self.schema.columns.len() {
            return Err(NormalizeError::InvalidTupleArity {
                tuple_kind,
                expected: self.schema.columns.len(),
                actual: tuple.cells.len(),
            });
        }
        if let Some((index, _)) = tuple
            .cells
            .iter()
            .enumerate()
            .find(|(_, cell)| matches!(cell, Cell::Binary(_)))
        {
            return Err(NormalizeError::BinaryValue {
                column: self.schema.columns[index].name.clone(),
            });
        }
        Ok(())
    }

    fn key_from_old_tuple(&self, tuple: &Tuple) -> Result<Key, NormalizeError> {
        let indexes = if tuple.cells.len() == self.schema.columns.len() {
            self.primary_key.ordinal_indexes.clone()
        } else if tuple.cells.len() == self.primary_key.ordinal_indexes.len() {
            self.primary_key.key_tuple_positions.clone()
        } else {
            return Err(NormalizeError::InvalidOldKeyArity {
                key_only: self.primary_key.ordinal_indexes.len(),
                full: self.schema.columns.len(),
                actual: tuple.cells.len(),
            });
        };
        self.key_from_selected_cells(&tuple.cells, &indexes, "old-key")
    }

    fn key_from_full_tuple(
        &self,
        tuple: &Tuple,
        tuple_kind: &'static str,
    ) -> Result<Key, NormalizeError> {
        self.key_from_selected_cells(&tuple.cells, &self.primary_key.ordinal_indexes, tuple_kind)
    }

    fn key_from_update_tuple(
        &self,
        tuple: &Tuple,
        old_key: Option<&Key>,
    ) -> Result<Key, NormalizeError> {
        let cells = self
            .primary_key
            .ordinal_indexes
            .iter()
            .enumerate()
            .map(|(ordinal, index)| match &tuple.cells[*index] {
                Cell::UnchangedToast => old_key
                    .and_then(|key| key.0.get(ordinal))
                    .cloned()
                    .ok_or_else(|| NormalizeError::InvalidPrimaryKey {
                        column: self.schema.columns[*index].name.clone(),
                        state: "unchanged without an old key",
                        tuple_kind: "update",
                    }),
                cell => self.key_cell(cell, *index, "update"),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Key(cells))
    }

    fn key_from_cells(
        &self,
        cells: &[Cell],
        tuple_kind: &'static str,
    ) -> Result<Key, NormalizeError> {
        self.key_from_selected_cells(cells, &self.primary_key.ordinal_indexes, tuple_kind)
    }

    fn key_from_selected_cells(
        &self,
        cells: &[Cell],
        indexes: &[usize],
        tuple_kind: &'static str,
    ) -> Result<Key, NormalizeError> {
        indexes
            .iter()
            .zip(&self.primary_key.ordinal_indexes)
            .map(|(cell_index, schema_index)| {
                self.key_cell(&cells[*cell_index], *schema_index, tuple_kind)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Key)
    }

    fn key_cell(
        &self,
        cell: &Cell,
        schema_index: usize,
        tuple_kind: &'static str,
    ) -> Result<KeyCell, NormalizeError> {
        match cell {
            Cell::Text(value) => Ok(KeyCell::Text(value.clone())),
            Cell::Binary(_) => Err(NormalizeError::BinaryValue {
                column: self.schema.columns[schema_index].name.clone(),
            }),
            Cell::Null => Err(NormalizeError::InvalidPrimaryKey {
                column: self.schema.columns[schema_index].name.clone(),
                state: "NULL",
                tuple_kind,
            }),
            Cell::UnchangedToast => Err(NormalizeError::InvalidPrimaryKey {
                column: self.schema.columns[schema_index].name.clone(),
                state: "unchanged",
                tuple_kind,
            }),
        }
    }

    fn write_key(&self, cells: &mut [Cell], key: &Key) {
        for (index, value) in self.primary_key.ordinal_indexes.iter().zip(key.cells()) {
            cells[*index] = value;
        }
    }

    fn ambiguous(&self, reason: impl Into<String>) -> NormalizeError {
        NormalizeError::AmbiguousSequence(reason.into())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::Utc;
    use cloudberry_etl_core::{
        change::{SourcePosition, SourceTransaction},
        lsn::PgLsn,
        schema::{
            ColumnSchema, GeneratedColumn, IdentityColumn, PgType, PgTypeKind, QualifiedName,
            ReplicaIdentity, TableKind,
        },
    };

    use super::*;
    use crate::batch::{BatchLimits, Batcher};

    fn text(value: &str) -> Cell {
        Cell::Text(Bytes::copy_from_slice(value.as_bytes()))
    }

    fn tuple(cells: Vec<Cell>) -> Tuple {
        Tuple { cells }
    }

    fn column(attnum: i16, name: &str, primary_key_ordinal: Option<u16>) -> ColumnSchema {
        ColumnSchema {
            attnum,
            name: name.to_owned(),
            data_type: PgType {
                oid: 23,
                name: QualifiedName::new("pg_catalog", "int4").unwrap(),
                kind: PgTypeKind::Int4,
            },
            nullable: primary_key_ordinal.is_none(),
            primary_key_ordinal,
            generated: GeneratedColumn::None,
            identity: IdentityColumn::None,
            collation: None,
        }
    }

    fn schema() -> TableSchema {
        TableSchema {
            relation_id: 7,
            generation: 3,
            name: QualifiedName::new("public", "items").unwrap(),
            kind: TableKind::Ordinary,
            replica_identity: ReplicaIdentity::Default,
            columns: vec![column(1, "id", Some(1)), column(2, "payload", None)],
            distribution_key: Vec::new(),
            partition_key: Vec::new(),
        }
    }

    fn row(change: RowChange) -> TableChange {
        TableChange {
            relation_id: 7,
            generation: 3,
            change,
        }
    }

    fn insert(id: &str, payload: &str) -> TableChange {
        row(RowChange::Insert {
            new: tuple(vec![text(id), text(payload)]),
        })
    }

    fn update(id: &str, payload: Cell) -> TableChange {
        row(RowChange::Update {
            old_key: None,
            new: tuple(vec![text(id), payload]),
        })
    }

    fn move_key(old: &str, new: &str, payload: Cell) -> TableChange {
        row(RowChange::Update {
            old_key: Some(tuple(vec![text(old)])),
            new: tuple(vec![text(new), payload]),
        })
    }

    fn delete(id: &str) -> TableChange {
        row(RowChange::Delete {
            old_key: tuple(vec![text(id)]),
        })
    }

    fn upsert(id: &str, payload: Cell) -> StagingRow {
        StagingRow {
            operation: StageOperation::Upsert,
            cells: vec![text(id), payload],
            old_key: None,
        }
    }

    fn insertion(id: &str, payload: Cell) -> StagingRow {
        StagingRow {
            operation: StageOperation::Insert,
            cells: vec![text(id), payload],
            old_key: None,
        }
    }

    fn deletion(id: &str) -> StagingRow {
        StagingRow {
            operation: StageOperation::Delete,
            cells: vec![text(id), Cell::UnchangedToast],
            old_key: None,
        }
    }

    fn movement(old: &str, new: &str, payload: Cell) -> StagingRow {
        StagingRow {
            operation: StageOperation::Move,
            cells: vec![text(new), payload],
            old_key: Some(vec![text(old)]),
        }
    }

    fn text_value(cell: &Cell) -> String {
        match cell {
            Cell::Text(value) => String::from_utf8(value.to_vec()).unwrap(),
            other => panic!("expected text cell, got {other:?}"),
        }
    }

    fn apply_current_state(target: &mut HashMap<String, String>, rows: &[StagingRow]) {
        let before = target.clone();
        let materialized_moves = rows
            .iter()
            .filter(|row| row.operation == StageOperation::Move)
            .map(|row| {
                let old = text_value(&row.old_key.as_ref().unwrap()[0]);
                let new = text_value(&row.cells[0]);
                let payload = match &row.cells[1] {
                    Cell::UnchangedToast => before.get(&old).unwrap().clone(),
                    cell => text_value(cell),
                };
                (old, new, payload)
            })
            .collect::<Vec<_>>();

        for row in rows {
            match row.operation {
                StageOperation::Delete => {
                    target.remove(&text_value(&row.cells[0]));
                }
                StageOperation::Move => {
                    target.remove(&text_value(&row.old_key.as_ref().unwrap()[0]));
                }
                StageOperation::Insert | StageOperation::Upsert => {}
            }
        }
        for (_, new, payload) in materialized_moves {
            target.insert(new, payload);
        }
        for row in rows.iter().filter(|row| {
            matches!(
                row.operation,
                StageOperation::Insert | StageOperation::Upsert
            )
        }) {
            let key = text_value(&row.cells[0]);
            if !matches!(row.cells[1], Cell::UnchangedToast) {
                target.insert(key, text_value(&row.cells[1]));
            }
        }
    }

    #[test]
    fn folds_same_key_sequences_to_the_final_state() {
        let cases = vec![
            ("I", vec![insert("1", "a")], vec![insertion("1", text("a"))]),
            (
                "I-U",
                vec![insert("1", "a"), update("1", text("b"))],
                vec![insertion("1", text("b"))],
            ),
            ("I-D", vec![insert("1", "a"), delete("1")], vec![]),
            (
                "I-U-D",
                vec![insert("1", "a"), update("1", text("b")), delete("1")],
                vec![],
            ),
            (
                "I-D-I",
                vec![insert("1", "a"), delete("1"), insert("1", "c")],
                vec![insertion("1", text("c"))],
            ),
            (
                "U",
                vec![update("1", text("a"))],
                vec![upsert("1", text("a"))],
            ),
            (
                "U-U",
                vec![update("1", text("a")), update("1", Cell::UnchangedToast)],
                vec![upsert("1", text("a"))],
            ),
            (
                "U-D",
                vec![update("1", text("a")), delete("1")],
                vec![deletion("1")],
            ),
            (
                "U-D-I",
                vec![update("1", text("a")), delete("1"), insert("1", "c")],
                vec![upsert("1", text("c"))],
            ),
            ("D", vec![delete("1")], vec![deletion("1")]),
            (
                "D-I",
                vec![delete("1"), insert("1", "c")],
                vec![upsert("1", text("c"))],
            ),
            (
                "D-I-D",
                vec![delete("1"), insert("1", "c"), delete("1")],
                vec![deletion("1")],
            ),
        ];

        for (name, changes, expected) in cases {
            let actual = normalize_table_changes(&schema(), &changes).unwrap();
            assert_eq!(actual, expected, "case {name}");
        }
    }

    #[test]
    fn retains_unknown_toast_for_a_stable_baseline_key() {
        let changes = [update("1", Cell::UnchangedToast)];
        assert_eq!(
            normalize_table_changes(&schema(), &changes).unwrap(),
            [upsert("1", Cell::UnchangedToast)]
        );
    }

    #[test]
    fn collapses_primary_key_moves_and_merges_later_values() {
        let cases = vec![
            (
                "A-B",
                vec![move_key("1", "2", Cell::UnchangedToast)],
                vec![StagingRow {
                    operation: StageOperation::Move,
                    cells: vec![text("2"), Cell::UnchangedToast],
                    old_key: Some(vec![text("1")]),
                }],
            ),
            (
                "A-B-update",
                vec![
                    move_key("1", "2", Cell::UnchangedToast),
                    update("2", text("latest")),
                ],
                vec![StagingRow {
                    operation: StageOperation::Move,
                    cells: vec![text("2"), text("latest")],
                    old_key: Some(vec![text("1")]),
                }],
            ),
            (
                "A-B-C",
                vec![
                    move_key("1", "2", Cell::UnchangedToast),
                    move_key("2", "3", Cell::UnchangedToast),
                ],
                vec![StagingRow {
                    operation: StageOperation::Move,
                    cells: vec![text("3"), Cell::UnchangedToast],
                    old_key: Some(vec![text("1")]),
                }],
            ),
            (
                "A-B-A",
                vec![
                    move_key("1", "2", Cell::UnchangedToast),
                    move_key("2", "1", Cell::UnchangedToast),
                ],
                vec![upsert("1", Cell::UnchangedToast)],
            ),
        ];

        for (name, changes, expected) in cases {
            let actual = normalize_table_changes(&schema(), &changes).unwrap();
            assert_eq!(actual, expected, "case {name}");
        }
    }

    #[test]
    fn delete_after_move_deletes_the_batch_start_key() {
        let changes = [move_key("1", "2", Cell::UnchangedToast), delete("2")];
        assert_eq!(
            normalize_table_changes(&schema(), &changes).unwrap(),
            [deletion("1")]
        );
    }

    #[test]
    fn inserted_then_moved_then_deleted_disappears() {
        let changes = [
            insert("1", "a"),
            move_key("1", "2", Cell::UnchangedToast),
            delete("2"),
        ];
        assert!(
            normalize_table_changes(&schema(), &changes)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn permits_reusing_a_moved_rows_old_key_for_a_new_insert() {
        let changes = [
            move_key("1", "2", Cell::UnchangedToast),
            insert("1", "replacement"),
        ];
        assert_eq!(
            normalize_table_changes(&schema(), &changes).unwrap(),
            [
                StagingRow {
                    operation: StageOperation::Move,
                    cells: vec![text("2"), Cell::UnchangedToast],
                    old_key: Some(vec![text("1")]),
                },
                upsert("1", text("replacement")),
            ]
        );
    }

    #[test]
    fn replacement_at_a_deleted_origin_suppresses_the_redundant_delete() {
        let changes = [
            move_key("1", "2", Cell::UnchangedToast),
            insert("1", "replacement"),
            delete("2"),
        ];
        assert_eq!(
            normalize_table_changes(&schema(), &changes).unwrap(),
            [upsert("1", text("replacement"))]
        );
    }

    #[test]
    fn move_can_reuse_a_key_released_by_another_batch_start_lineage() {
        let delete_then_move = [delete("2"), move_key("1", "2", Cell::UnchangedToast)];
        assert_eq!(
            normalize_table_changes(&schema(), &delete_then_move).unwrap(),
            [deletion("2"), movement("1", "2", Cell::UnchangedToast),]
        );

        let move_chain = [
            move_key("2", "3", Cell::UnchangedToast),
            move_key("1", "2", Cell::UnchangedToast),
        ];
        assert_eq!(
            normalize_table_changes(&schema(), &move_chain).unwrap(),
            [
                movement("2", "3", Cell::UnchangedToast),
                movement("1", "2", Cell::UnchangedToast),
            ]
        );
    }

    #[test]
    fn collapses_a_temporary_key_swap_to_two_batch_start_moves() {
        let changes = [
            move_key("1", "9", Cell::UnchangedToast),
            move_key("2", "1", Cell::UnchangedToast),
            move_key("9", "2", Cell::UnchangedToast),
        ];
        assert_eq!(
            normalize_table_changes(&schema(), &changes).unwrap(),
            [
                movement("1", "2", Cell::UnchangedToast),
                movement("2", "1", Cell::UnchangedToast),
            ]
        );
    }

    #[test]
    fn chunk_size_does_not_change_delete_move_or_chain_final_state() {
        let cases = [
            vec![delete("2"), move_key("1", "2", Cell::UnchangedToast)],
            vec![
                move_key("2", "3", Cell::UnchangedToast),
                move_key("1", "2", Cell::UnchangedToast),
            ],
        ];

        for changes in cases {
            let mut results = Vec::new();
            for chunk_size in [1, 2] {
                let mut target = HashMap::from([
                    ("1".to_owned(), "one".to_owned()),
                    ("2".to_owned(), "two".to_owned()),
                ]);
                for chunk in changes.chunks(chunk_size) {
                    let rows = normalize_table_changes(&schema(), chunk).unwrap();
                    apply_current_state(&mut target, &rows);
                }
                results.push(target);
            }
            assert_eq!(results[0], results[1]);
        }
    }

    #[test]
    fn key_only_old_tuples_use_relation_order_and_emit_pk_ordinal_order() {
        let mut composite = schema();
        composite.columns = vec![
            column(1, "tenant", Some(2)),
            column(2, "id", Some(1)),
            column(3, "payload", None),
        ];
        let key_only = TableChange {
            relation_id: 7,
            generation: 3,
            change: RowChange::Update {
                old_key: Some(tuple(vec![text("10"), text("1")])),
                new: tuple(vec![text("20"), text("2"), Cell::UnchangedToast]),
            },
        };
        let expected = StagingRow {
            operation: StageOperation::Move,
            cells: vec![text("20"), text("2"), Cell::UnchangedToast],
            old_key: Some(vec![text("1"), text("10")]),
        };
        let key_only_result = normalize_table_changes(&composite, [&key_only]).unwrap();
        assert_eq!(key_only_result.as_slice(), std::slice::from_ref(&expected));

        let full_width = TableChange {
            change: RowChange::Update {
                old_key: Some(tuple(vec![text("10"), text("1"), text("ignored")])),
                new: tuple(vec![text("20"), text("2"), Cell::UnchangedToast]),
            },
            ..key_only
        };
        assert_eq!(
            normalize_table_changes(&composite, [&full_width]).unwrap(),
            [expected]
        );
    }

    #[test]
    fn fills_an_unchanged_new_key_from_the_explicit_old_key() {
        let change = row(RowChange::Update {
            old_key: Some(tuple(vec![text("1")])),
            new: tuple(vec![Cell::UnchangedToast, text("new")]),
        });
        assert_eq!(
            normalize_table_changes(&schema(), [&change]).unwrap(),
            [upsert("1", text("new"))]
        );
    }

    #[test]
    fn rejects_invalid_tuple_keys_and_binary_values() {
        let cases = [
            row(RowChange::Insert {
                new: tuple(vec![text("1")]),
            }),
            row(RowChange::Delete {
                old_key: tuple(vec![]),
            }),
            row(RowChange::Delete {
                old_key: tuple(vec![Cell::Null]),
            }),
            row(RowChange::Update {
                old_key: None,
                new: tuple(vec![Cell::UnchangedToast, text("x")]),
            }),
            row(RowChange::Insert {
                new: tuple(vec![text("1"), Cell::UnchangedToast]),
            }),
            row(RowChange::Insert {
                new: tuple(vec![text("1"), Cell::Binary(Bytes::from_static(b"x"))]),
            }),
        ];

        for change in cases {
            assert!(normalize_table_changes(&schema(), [&change]).is_err());
        }
    }

    #[test]
    fn rejects_relation_generation_and_destination_conflicts() {
        let mut wrong_relation = update("1", text("x"));
        wrong_relation.relation_id = 8;
        assert!(matches!(
            normalize_table_changes(&schema(), [&wrong_relation]),
            Err(NormalizeError::RelationMismatch { .. })
        ));

        let mut wrong_generation = update("1", text("x"));
        wrong_generation.generation = 4;
        assert!(matches!(
            normalize_table_changes(&schema(), [&wrong_generation]),
            Err(NormalizeError::GenerationMismatch { .. })
        ));

        let conflict = [
            update("2", Cell::UnchangedToast),
            move_key("1", "2", Cell::UnchangedToast),
        ];
        assert!(matches!(
            normalize_table_changes(&schema(), &conflict),
            Err(NormalizeError::AmbiguousSequence(_))
        ));
    }

    #[test]
    fn normalizes_only_the_requested_relation_from_a_transaction_batch() {
        let mut other = insert("9", "ignored");
        other.relation_id = 8;
        let transaction = SourceTransaction {
            xid: 1,
            commit_time: Utc::now(),
            final_position: SourcePosition {
                node_id: 1,
                system_identifier: 2,
                timeline: 1,
                lsn: PgLsn::new(10),
            },
            changes: vec![
                TransactionChange::Row(other),
                TransactionChange::Row(insert("1", "kept")),
            ],
        };
        let mut batcher = Batcher::new(BatchLimits {
            max_rows: 10,
            max_bytes: 1_024,
            max_delay: Duration::from_secs(1),
        })
        .unwrap();
        batcher.push(transaction).unwrap();
        let batch = batcher.flush().unwrap();
        assert_eq!(
            normalize_table_batch(&schema(), &batch).unwrap(),
            [insertion("1", text("kept"))]
        );
    }
}
