//! Helpers for extracting scalar literals from table-function arguments.

use arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::scalar::ScalarValue;

/// Extract an `i64` from a literal argument (coercing any integer type).
pub(crate) fn expr_to_i64(expr: &Expr) -> Result<i64> {
    match expr {
        Expr::Literal(sv, _) => match sv.clone().cast_to(&DataType::Int64)? {
            ScalarValue::Int64(Some(v)) => Ok(v),
            other => Err(DataFusionError::Execution(format!(
                "expected an integer literal, got {other:?}"
            ))),
        },
        _ => Err(DataFusionError::Execution(
            "arguments must be literals".into(),
        )),
    }
}

/// Extract a `u64` from a literal argument (coercing any integer type).
pub(crate) fn expr_to_u64(expr: &Expr) -> Result<u64> {
    match expr {
        Expr::Literal(sv, _) => match sv.clone().cast_to(&DataType::UInt64)? {
            ScalarValue::UInt64(Some(v)) => Ok(v),
            other => Err(DataFusionError::Execution(format!(
                "expected an integer literal, got {other:?}"
            ))),
        },
        _ => Err(DataFusionError::Execution(
            "arguments must be literals".into(),
        )),
    }
}

/// Extract a `String` from a literal argument (accepting `Utf8`/`LargeUtf8`).
pub(crate) fn expr_to_string(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Literal(sv, _) => match sv {
            ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => Ok(v.clone()),
            other => Err(DataFusionError::Execution(format!(
                "expected a string literal, got {other:?}"
            ))),
        },
        _ => Err(DataFusionError::Execution(
            "arguments must be literals".into(),
        )),
    }
}
