//! Arrow-backed node and edge tables.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, TimestampNanosecondArray, UInt16Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::csr::TemporalCSR;
use crate::error::{GtvError, Result};

/// Canonical column names for edge tables.
pub mod edge_cols {
    pub const SRC: &str = "src";
    pub const DST: &str = "dst";
    pub const EDGE_TYPE: &str = "edge_type";
    pub const VALID_FROM: &str = "valid_from";
    pub const VALID_TO: &str = "valid_to";
}

/// Canonical column name for the node id column.
pub const NODE_ID_COL: &str = "id";

fn timestamp_ns() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, None)
}

/// Schema of a temporal edge table: src, dst, edge_type, valid_from, valid_to.
pub fn edge_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(edge_cols::SRC, DataType::UInt64, false),
        Field::new(edge_cols::DST, DataType::UInt64, false),
        Field::new(edge_cols::EDGE_TYPE, DataType::UInt16, false),
        Field::new(edge_cols::VALID_FROM, timestamp_ns(), false),
        Field::new(edge_cols::VALID_TO, timestamp_ns(), false),
    ]))
}

/// Downcast a column of `batch` by name to a concrete Arrow array type.
fn column<'a, T>(batch: &'a RecordBatch, name: &str) -> Result<&'a T>
where
    T: Array + 'static,
{
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| GtvError::Schema(format!("missing column `{name}`")))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| GtvError::Schema(format!("column `{name}` has unexpected type")))
}

fn ensure_column_type(batch: &RecordBatch, name: &str, expected: &DataType) -> Result<()> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| GtvError::Schema(format!("missing column `{name}`")))?;
    let schema = batch.schema();
    let actual = schema.field(idx).data_type();
    if actual != expected {
        return Err(GtvError::Schema(format!(
            "column `{name}` expected {expected}, found {actual}"
        )));
    }
    Ok(())
}

/// An immutable table of nodes backed by a `RecordBatch`. Requires an `id` (u64)
/// column; any extra columns are treated as node attributes.
#[derive(Debug, Clone)]
pub struct NodeTable {
    batch: RecordBatch,
}

impl NodeTable {
    pub fn new(batch: RecordBatch) -> Result<Self> {
        ensure_column_type(&batch, NODE_ID_COL, &DataType::UInt64)?;
        Ok(Self { batch })
    }

    pub fn len(&self) -> usize {
        self.batch.num_rows()
    }

    pub fn is_empty(&self) -> bool {
        self.batch.num_rows() == 0
    }

    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    pub fn ids(&self) -> &UInt64Array {
        column::<UInt64Array>(&self.batch, NODE_ID_COL).expect("validated at construction")
    }
}

/// An immutable table of temporal edges backed by a `RecordBatch` with the
/// canonical edge schema (see [`edge_schema`]).
#[derive(Debug, Clone)]
pub struct EdgeTable {
    batch: RecordBatch,
}

impl EdgeTable {
    pub fn new(batch: RecordBatch) -> Result<Self> {
        ensure_column_type(&batch, edge_cols::SRC, &DataType::UInt64)?;
        ensure_column_type(&batch, edge_cols::DST, &DataType::UInt64)?;
        ensure_column_type(&batch, edge_cols::EDGE_TYPE, &DataType::UInt16)?;
        ensure_column_type(&batch, edge_cols::VALID_FROM, &timestamp_ns())?;
        ensure_column_type(&batch, edge_cols::VALID_TO, &timestamp_ns())?;
        Ok(Self { batch })
    }

    /// Convenience constructor from raw column vectors.
    pub fn from_vecs(
        src: Vec<u64>,
        dst: Vec<u64>,
        edge_type: Vec<u16>,
        valid_from: Vec<i64>,
        valid_to: Vec<i64>,
    ) -> Result<Self> {
        let n = src.len();
        if dst.len() != n || edge_type.len() != n || valid_from.len() != n || valid_to.len() != n {
            return Err(GtvError::InvalidArgument(
                "edge column vectors have mismatched lengths".into(),
            ));
        }
        let batch = RecordBatch::try_new(
            edge_schema(),
            vec![
                Arc::new(UInt64Array::from(src)) as ArrayRef,
                Arc::new(UInt64Array::from(dst)) as ArrayRef,
                Arc::new(UInt16Array::from(edge_type)) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(valid_from)) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(valid_to)) as ArrayRef,
            ],
        )?;
        Self::new(batch)
    }

    pub fn len(&self) -> usize {
        self.batch.num_rows()
    }

    pub fn is_empty(&self) -> bool {
        self.batch.num_rows() == 0
    }

    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    pub fn src(&self) -> &UInt64Array {
        column(&self.batch, edge_cols::SRC).expect("validated at construction")
    }

    pub fn dst(&self) -> &UInt64Array {
        column(&self.batch, edge_cols::DST).expect("validated at construction")
    }

    pub fn edge_type(&self) -> &UInt16Array {
        column(&self.batch, edge_cols::EDGE_TYPE).expect("validated at construction")
    }

    pub fn valid_from(&self) -> &TimestampNanosecondArray {
        column(&self.batch, edge_cols::VALID_FROM).expect("validated at construction")
    }

    pub fn valid_to(&self) -> &TimestampNanosecondArray {
        column(&self.batch, edge_cols::VALID_TO).expect("validated at construction")
    }

    /// Build a [`TemporalCSR`] index over these edges.
    pub fn to_csr(&self, node_count: usize) -> Result<TemporalCSR> {
        TemporalCSR::from_arrays(
            self.src(),
            self.dst(),
            self.valid_from(),
            self.valid_to(),
            self.edge_type(),
            node_count,
        )
    }
}
