//! Versioned schema registry and compatibility rules.
//!
//! Arrow schemas are persisted as hex-encoded Arrow IPC schema messages, so a
//! restored schema is byte-for-byte the one that was committed (no lossy
//! `DataType` string round-trip).

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use serde::{Deserialize, Serialize};

use crate::error::{CatalogError, Result};

/// Monotonic schema version of a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchemaVersion(pub u32);

/// A persisted schema version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRecord {
    pub version: SchemaVersion,
    /// Hex-encoded Arrow IPC schema.
    pub schema: String,
    pub created_at: i64,
    pub comment: Option<String>,
    /// Rename history (`old name -> new name`), applied when reading older files.
    #[serde(default)]
    pub renames: BTreeMap<String, String>,
}

/// A requested schema change.
#[derive(Debug, Clone)]
pub enum SchemaChange {
    /// Append a (nullable) column.
    AddColumn { field: Field, default: Option<String> },
    /// Rename a column (metadata-only; readers map old file names).
    RenameColumn { from: String, to: String },
    /// Widen a column to a compatible larger type.
    WidenType { column: String, to: DataType },
}

fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xf) as usize] as char);
    }
    s
}

fn from_hex(s: &str) -> Result<Vec<u8>> {
    if s.len() % 2 != 0 {
        return Err(CatalogError::Corrupt("odd-length hex schema".into()));
    }
    let bytes = s.as_bytes();
    let val = |c: u8| -> Result<u8> {
        match c {
            b'0'..=b'9' => Ok(c - b'0'),
            b'a'..=b'f' => Ok(c - b'a' + 10),
            b'A'..=b'F' => Ok(c - b'A' + 10),
            _ => Err(CatalogError::Corrupt("invalid hex digit".into())),
        }
    };
    let mut out = Vec::with_capacity(s.len() / 2);
    for pair in bytes.chunks(2) {
        out.push((val(pair[0])? << 4) | val(pair[1])?);
    }
    Ok(out)
}

/// Serialize an Arrow schema to a hex-encoded IPC schema message.
pub fn schema_to_hex(schema: &SchemaRef) -> Result<String> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema)?;
        writer.finish()?;
    }
    Ok(to_hex(&buf))
}

/// Restore an Arrow schema from [`schema_to_hex`].
pub fn schema_from_hex(hex: &str) -> Result<SchemaRef> {
    let bytes = from_hex(hex)?;
    let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    Ok(reader.schema())
}

impl SchemaRecord {
    /// Build a persisted record for `schema` at `version`.
    pub fn new(version: u32, schema: &SchemaRef, comment: Option<String>) -> Result<Self> {
        Ok(Self {
            version: SchemaVersion(version),
            schema: schema_to_hex(schema)?,
            created_at: now_ns(),
            comment,
            renames: BTreeMap::new(),
        })
    }

    /// Decode the Arrow schema.
    pub fn arrow(&self) -> Result<SchemaRef> {
        schema_from_hex(&self.schema)
    }
}

/// Current wall-clock time in nanoseconds since the epoch.
pub fn now_ns() -> i64 {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| chrono::Utc::now().timestamp_micros() * 1_000)
}

/// True when `to` can represent every value of `from` without loss.
pub fn is_widening(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    if from == to {
        return true;
    }
    match (from, to) {
        (Int16, Int32 | Int64 | Float32 | Float64) => true,
        (Int32, Int64 | Float64) => true,
        (UInt16, UInt32 | UInt64 | Int32 | Int64 | Float32 | Float64) => true,
        (UInt32, UInt64 | Int64 | Float64) => true,
        (Float32, Float64) => true,
        (Date32, Timestamp(_, _)) => true,
        (Timestamp(u1, tz1), Timestamp(u2, tz2)) => tz1 == tz2 && unit_rank(u2) >= unit_rank(u1),
        _ => false,
    }
}

fn unit_rank(u: &TimeUnit) -> u8 {
    match u {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}

/// Validate that `new` is a backward-compatible evolution of `old`.
///
/// Rules (frozen for B2-1):
/// * every old column must still exist with the same or a widening type;
/// * every new column must be nullable.
pub fn check_compatible(old: &Schema, new: &Schema) -> Result<()> {
    for old_field in old.fields() {
        let new_field = new
            .field_with_name(old_field.name())
            .map_err(|_| {
                CatalogError::SchemaIncompatible(format!(
                    "column `{}` was dropped",
                    old_field.name()
                ))
            })?;
        if !is_widening(old_field.data_type(), new_field.data_type()) {
            return Err(CatalogError::SchemaIncompatible(format!(
                "column `{}` changed type {} -> {} (not a widening cast)",
                old_field.name(),
                old_field.data_type(),
                new_field.data_type()
            )));
        }
    }
    for new_field in new.fields() {
        if old.field_with_name(new_field.name()).is_err() && !new_field.is_nullable() {
            return Err(CatalogError::SchemaIncompatible(format!(
                "new column `{}` must be nullable",
                new_field.name()
            )));
        }
    }
    Ok(())
}

/// Apply a [`SchemaChange`], returning the new schema and an optional rename
/// `(old, new)` mapping to record in the registry.
pub fn apply_change(current: &SchemaRef, change: &SchemaChange) -> Result<(SchemaRef, Option<(String, String)>)> {
    let mut fields: Vec<Field> = current.fields().iter().map(|f| f.as_ref().clone()).collect();
    match change {
        SchemaChange::AddColumn { field, default: _ } => {
            if !field.is_nullable() {
                return Err(CatalogError::SchemaIncompatible(format!(
                    "added column `{}` must be nullable",
                    field.name()
                )));
            }
            if current.field_with_name(field.name()).is_ok() {
                return Err(CatalogError::SchemaIncompatible(format!(
                    "column `{}` already exists",
                    field.name()
                )));
            }
            fields.push(field.clone());
            Ok((Arc::new(Schema::new(fields)), None))
        }
        SchemaChange::RenameColumn { from, to } => {
            let idx = current
                .index_of(from)
                .map_err(|_| CatalogError::SchemaIncompatible(format!("column `{from}` not found")))?;
            if current.field_with_name(to).is_ok() {
                return Err(CatalogError::SchemaIncompatible(format!(
                    "column `{to}` already exists"
                )));
            }
            fields[idx] = fields[idx].clone().with_name(to.clone());
            Ok((Arc::new(Schema::new(fields)), Some((from.clone(), to.clone()))))
        }
        SchemaChange::WidenType { column, to } => {
            let idx = current
                .index_of(column)
                .map_err(|_| CatalogError::SchemaIncompatible(format!("column `{column}` not found")))?;
            if !is_widening(fields[idx].data_type(), to) {
                return Err(CatalogError::SchemaIncompatible(format!(
                    "column `{column}` cannot widen {} -> {to}",
                    fields[idx].data_type()
                )));
            }
            fields[idx] = fields[idx].clone().with_data_type(to.clone());
            Ok((Arc::new(Schema::new(fields)), None))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema_a() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
        ]))
    }

    #[test]
    fn ipc_round_trip_preserves_schema() {
        let s = schema_a();
        let rec = SchemaRecord::new(1, &s, None).unwrap();
        let back = rec.arrow().unwrap();
        assert_eq!(back.fields().len(), 2);
        assert_eq!(back.field(0).data_type(), &DataType::UInt64);
        assert_eq!(back.field(1).name(), "value");
    }

    #[test]
    fn add_nullable_column_is_compatible() {
        let old = schema_a();
        let (new, _) = apply_change(
            &old,
            &SchemaChange::AddColumn {
                field: Field::new("tag", DataType::Utf8, true),
                default: None,
            },
        )
        .unwrap();
        check_compatible(&old, &new).unwrap();
    }

    #[test]
    fn add_non_null_column_rejected() {
        let old = schema_a();
        let err = apply_change(
            &old,
            &SchemaChange::AddColumn {
                field: Field::new("tag", DataType::Utf8, false),
                default: None,
            },
        )
        .unwrap_err();
        assert!(matches!(err, CatalogError::SchemaIncompatible(_)));
    }

    #[test]
    fn widening_allowed_narrowing_rejected() {
        let old = schema_a();
        let (wide, _) = apply_change(
            &old,
            &SchemaChange::WidenType {
                column: "value".into(),
                to: DataType::Float64,
            },
        )
        .unwrap();
        check_compatible(&old, &wide).unwrap();

        let narrow = Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("value", DataType::Float64, false),
        ]);
        assert!(check_compatible(&old, &narrow).is_err());
    }

    #[test]
    fn rename_records_mapping() {
        let old = schema_a();
        let (new, mapping) = apply_change(
            &old,
            &SchemaChange::RenameColumn {
                from: "value".into(),
                to: "amount".into(),
            },
        )
        .unwrap();
        assert_eq!(mapping, Some(("value".to_string(), "amount".to_string())));
        assert!(new.field_with_name("amount").is_ok());
    }
}
