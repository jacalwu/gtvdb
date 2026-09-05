//! Oracle-style `TRUNC(timestamp, fmt)` scalar UDF, registered as `trunc`.
//!
//! The name deliberately avoids `truncate` (confusable with Oracle's
//! `TRUNCATE TABLE` DDL) and stays a single polymorphic function:
//!
//! * `trunc(ts, fmt)` — timestamp bucketing. `fmt` units (case-insensitive):
//!   `SS` second, `MI` minute, `HH` hour, `DD` day, `MM` month, `YYYY` year.
//!   Fine units (SS..DD) floor via epoch arithmetic (UTC); `MM`/`YYYY` floor
//!   via chrono (calendar day 1). `ts` may be an Arrow `Timestamp` of any unit
//!   or a raw `Int64` nanosecond count (gtv demo convention).
//! * `trunc(x [, digits])` — numeric truncation toward zero, mirroring the
//!   DataFusion math `trunc` built-in it shadows (floats; `digits` = decimals
//!   after the point, negative = tens/hundreds/…). `Int64` is an identity.
//!
//! Invalid units / non-string, non-int second arguments return a `DataFusion`
//! error rather than panicking.

use std::sync::Arc;

use arrow::array::{
    as_primitive_array, as_string_array, Array, ArrayRef, Float64Array, Int64Array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Float64Type, Int64Type, TimeUnit};
use chrono::{Datelike, TimeZone, Utc};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

const NS_PER_S: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_S;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_S;
const NS_PER_DAY: i64 = 86_400 * NS_PER_S;

/// Nanoseconds per one raw unit of `dt`, or error for non-temporal types.
fn unit_nanos(dt: &DataType) -> DfResult<i64> {
    match dt {
        DataType::Timestamp(TimeUnit::Second, _) => Ok(NS_PER_S),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Ok(NS_PER_S / 1_000),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(NS_PER_S / 1_000_000),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(1),
        // demo convention: temporal columns stored as raw Int64 nanoseconds
        DataType::Int64 => Ok(1),
        other => Err(DataFusionError::Execution(format!(
            "trunc: unsupported time type {other:?}"
        ))),
    }
}

/// Floor one raw timestamp `v` (counts in units of `unit_ns` ns) to the start
/// of the requested `fmt` bucket. Unknown units are an error (Oracle TRUNC
/// rejects them too).
fn bucket_value(v: i64, unit_ns: i64, fmt: &str) -> DfResult<i64> {
    match fmt {
        "SS" => Ok(v - v.rem_euclid(NS_PER_S / unit_ns)),
        "MI" => Ok(v - v.rem_euclid(NS_PER_MIN / unit_ns)),
        "HH" => Ok(v - v.rem_euclid(NS_PER_HOUR / unit_ns)),
        "DD" => Ok(v - v.rem_euclid(NS_PER_DAY / unit_ns)),
        "MM" | "YYYY" => {
            let ns = v.saturating_mul(unit_ns);
            let dt = Utc.timestamp_nanos(ns);
            let m = if fmt == "YYYY" { 1 } else { dt.month() };
            match Utc.with_ymd_and_hms(dt.year(), m, 1, 0, 0, 0).single() {
                Some(t) => Ok(t.timestamp_nanos_opt().unwrap_or(ns) / unit_ns),
                // Year out of chrono's representable range: leave unchanged.
                None => Ok(v),
            }
        }
        other => Err(DataFusionError::Execution(format!(
            "trunc: unknown unit `{other}` (supported: SS, MI, HH, DD, MM, YYYY)"
        ))),
    }
}

/// Numeric truncation toward zero (DataFusion math `trunc` semantics that this
/// function shadows): `digits` decimals after the point; negative `digits`
/// truncate at tens/hundreds/…
fn numeric_trunc_f64(values: &[f64], digits: &[i64]) -> Vec<f64> {
    values
        .iter()
        .enumerate()
        .map(|(i, &v)| {
            let d = if digits.len() == values.len() { digits[i] } else { digits[0] };
            if d == 0 {
                v.trunc()
            } else if d > 0 {
                let p = 10f64.powi(d as i32);
                (v * p).trunc() / p
            } else {
                let p = 10f64.powi((-d) as i32);
                (v / p).trunc() * p
            }
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TruncUdf {
    name: String,
    signature: Signature,
}

impl TruncUdf {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            // 1 or 2 arbitrary args; arity is fixed here and the argument *types*
            // are validated per call (numeric vs timestamp bucket paths).
            signature: Signature::one_of(vec![TypeSignature::Any(1), TypeSignature::Any(2)], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TruncUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DfResult<DataType> {
        let first = arg_types
            .first()
            .ok_or_else(|| DataFusionError::Execution("trunc: missing argument".into()))?;
        match first {
            DataType::Int64 | DataType::Float64 | DataType::Timestamp(..) => Ok(first.clone()),
            other => Err(DataFusionError::Execution(format!(
                "trunc: unsupported first argument type {other:?} \
                 (expected Int64/Float64/Timestamp)"
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let arr = arrays
            .first()
            .ok_or_else(|| DataFusionError::Execution("trunc: missing argument".into()))?;

        // -------------------------------------------------------------------
        // Numeric path: trunc(3.9), trunc(3.9876, 2) — DataFusion math parity.
        // -------------------------------------------------------------------
        match arr.data_type() {
            DataType::Float64 => {
                let digits: Vec<i64> = if arrays.len() > 1 {
                    extract_digits(&arrays[1])?
                } else {
                    vec![0]
                };
                let f64s = as_primitive_array::<Float64Type>(arr.as_ref()).values();
                let out = Float64Array::from(numeric_trunc_f64(f64s, &digits));
                return Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef));
            }
            DataType::Int64 => {
                // trunc(int) is the identity; with a second argument the demo
                // convention is an Int64-nanosecond timestamp, so the argument
                // must be a string fmt unit (not a numeric `digits`).
                if arrays.len() == 1 {
                    return Ok(ColumnarValue::Array(arr.clone()));
                }
            }
            DataType::Timestamp(..) => {}
            other => {
                return Err(DataFusionError::Execution(format!(
                    "trunc: unsupported first argument type {other:?} \
                     (expected Int64/Float64/Timestamp)"
                )))
            }
        }

        // -------------------------------------------------------------------
        // Timestamp bucket path: trunc(ts, 'DD') / trunc(int64_ns, 'MM').
        // -------------------------------------------------------------------
        let second = arrays
            .get(1)
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "trunc: timestamp bucketing needs two arguments: trunc(ts, 'DD')".into(),
                )
            })?
            .clone();
        if second.data_type() != &DataType::Utf8 {
            return Err(DataFusionError::Execution(format!(
                "trunc: second argument must be a string unit \
                 (SS, MI, HH, DD, MM, YYYY), got {:?}",
                second.data_type()
            )));
        }
        let fmts = as_string_array(second.as_ref());
        if fmts.is_empty() {
            return Err(DataFusionError::Execution("trunc: empty fmt argument".into()));
        }
        let unit_ns = unit_nanos(arr.data_type())?;
        let casted = cast(arr, &DataType::Int64)?;
        let values = as_primitive_array::<Int64Type>(casted.as_ref()).values();
        let per_row = fmts.len() == values.len();
        if !per_row && fmts.len() != 1 {
            return Err(DataFusionError::Execution(
                "trunc: fmt length must be 1 or match the input length".into(),
            ));
        }
        let mut out = Vec::with_capacity(values.len());
        for (i, &v) in values.iter().enumerate() {
            let raw = if per_row { fmts.value(i) } else { fmts.value(0) };
            out.push(bucket_value(v, unit_ns, &raw.to_uppercase())?);
        }
        let truncated = Int64Array::from(out);
        // Cast back to the original temporal type (Timestamp units preserved).
        Ok(ColumnarValue::Array(cast(&truncated, arr.data_type())?))
    }
}

/// Extract the `digits` argument of the numeric path (Int64 scalar or column).
fn extract_digits(arr: &ArrayRef) -> DfResult<Vec<i64>> {
    if arr.data_type() != &DataType::Int64 {
        return Err(DataFusionError::Execution(format!(
            "trunc: numeric `digits` must be an integer, got {:?}",
            arr.data_type()
        )));
    }
    let ints = as_primitive_array::<Int64Type>(arr.as_ref()).values();
    if ints.is_empty() {
        return Err(DataFusionError::Execution("trunc: empty `digits`".into()));
    }
    Ok(ints.to_vec())
}

/// Register `trunc(timestamp|number, …)` — Oracle-style time bucketing for
/// temporal/Int64-ns inputs, numeric truncation toward zero otherwise.
pub fn trunc_udf() -> ScalarUDF {
    ScalarUDF::from(TruncUdf::new("trunc"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, TimestampNanosecondArray};
    use arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;
    use datafusion::scalar::ScalarValue;

    fn field(name: &str, dt: DataType) -> Arc<arrow::datatypes::Field> {
        Arc::new(Field::new(name, dt, false))
    }

    fn invoke(
        udf: &TruncUdf,
        inputs: Vec<ColumnarValue>,
    ) -> DfResult<ColumnarValue> {
        let n = match inputs.first() {
            Some(ColumnarValue::Array(a)) => a.len(),
            _ => 0,
        };
        let args = ScalarFunctionArgs {
            arg_fields: inputs.iter().enumerate().map(|(i, _)| field(&format!("a{i}"), DataType::Null)).collect(),
            args: inputs,
            number_rows: n,
            return_field: field("trunc", DataType::Null),
            config_options: Arc::new(ConfigOptions::default()),
        };
        udf.invoke_with_args(args)
    }

    fn ints_out(v: ColumnarValue) -> Vec<i64> {
        match v {
            ColumnarValue::Array(a) => {
                let casted = cast(&a, &DataType::Int64).unwrap();
                as_primitive_array::<Int64Type>(casted.as_ref()).values().to_vec()
            }
            _ => panic!("expected array"),
        }
    }

    fn ints_in(v: Vec<i64>) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(Int64Array::from(v)))
    }

    fn fmts_in(v: Vec<&str>) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(StringArray::from(v)))
    }

    /// A single scalar fmt that DataFusion broadcasts to every row.
    fn scalar_fmt(s: &str) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s.to_string())))
    }

    fn scalar_i(v: i64) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v)))
    }

    /// 2024-01-01 00:00:00 UTC … a few boundaries across days/months.
    fn boundaries() -> Vec<i64> {
        let day = NS_PER_DAY;
        // 2024-01-01 00:00 (epoch ns)
        let jan1 = 1_704_067_200i64 * NS_PER_S;
        vec![
            jan1,               // 2024-01-01 00:00
            jan1 + day - 1,     // 2024-01-01 23:59:59.999999999
            jan1 + 30 * day,    // 2024-01-31 00:00
            jan1 + 30 * day + 22 * 3_600 * NS_PER_S, // 2024-01-31 22:00
            jan1 + 31 * day,    // 2024-02-01 00:00
            jan1 + 31 * day + 2 * 3_600 * NS_PER_S,  // 2024-02-01 02:00
        ]
    }

    #[test]
    fn bucket_dd_floors_to_day_start() {
        let udf = TruncUdf::new("trunc");
        let b = boundaries();
        let out = ints_out(invoke(&udf, vec![ints_in(b.clone()), fmts_in(vec!["DD"; b.len()])]).unwrap());
        let day = NS_PER_DAY;
        let jan1 = 1_704_067_200i64 * NS_PER_S;
        let want = vec![
            jan1, jan1, jan1 + 30 * day, jan1 + 30 * day, jan1 + 31 * day, jan1 + 31 * day,
        ];
        assert_eq!(out, want);
    }

    #[test]
    fn bucket_mm_and_yyyy() {
        let udf = TruncUdf::new("trunc");
        let b = boundaries();
        let day = NS_PER_DAY;
        let jan1 = 1_704_067_200i64 * NS_PER_S;
        let mm = ints_out(invoke(&udf, vec![ints_in(b.clone()), fmts_in(vec!["MM"; b.len()])]).unwrap());
        let yy = ints_out(invoke(&udf, vec![ints_in(b.clone()), fmts_in(vec!["YYYY"; b.len()])]).unwrap());
        assert_eq!(mm, vec![jan1, jan1, jan1, jan1, jan1 + 31 * day, jan1 + 31 * day]);
        assert_eq!(yy, vec![jan1; 6]);
    }

    #[test]
    fn fmt_broadcasts_single_value() {
        let udf = TruncUdf::new("trunc");
        let b = boundaries();
        // one scalar fmt broadcast to every row (DataFusion literal form)
        let out = ints_out(invoke(&udf, vec![ints_in(b.clone()), scalar_fmt("DD")]).unwrap());
        let jan1 = 1_704_067_200i64 * NS_PER_S;
        let day = NS_PER_DAY;
        assert_eq!(out[1], jan1);
        assert_eq!(out[4], jan1 + 31 * day);
    }

    #[test]
    fn numeric_trunc_toward_zero() {
        let udf = TruncUdf::new("trunc");
        let col = ColumnarValue::Array(Arc::new(arrow::array::Float64Array::from(vec![3.9, -3.9, 2.0])));
        let one = ints_out(invoke(&udf, vec![col.clone()]).unwrap());
        assert_eq!(one, vec![3, -3, 2]);
        let dec = invoke(&udf, vec![col.clone(), scalar_i(1)]).unwrap();
        let dec = match dec {
            ColumnarValue::Array(a) => {
                as_primitive_array::<Float64Type>(a.as_ref()).values().to_vec()
            }
            _ => panic!(),
        };
        assert_eq!(dec, vec![3.9, -3.9, 2.0]); // one decimal keeps all
    }

    #[test]
    fn numeric_trunc_digits() {
        let udf = TruncUdf::new("trunc");
        let col = ColumnarValue::Array(Arc::new(arrow::array::Float64Array::from(vec![1.23456, -1.23456])));
        let two = match invoke(&udf, vec![col.clone(), scalar_i(2)]).unwrap() {
            ColumnarValue::Array(a) => as_primitive_array::<Float64Type>(a.as_ref()).values().to_vec(),
            _ => panic!(),
        };
        assert_eq!(two, vec![1.23, -1.23]);
    }

    #[test]
    fn unknown_unit_is_error_not_silent() {
        let udf = TruncUdf::new("trunc");
        let err = invoke(&udf, vec![ints_in(vec![0]), fmts_in(vec!["XX"])]).unwrap_err();
        assert!(err.to_string().contains("unknown unit"));
    }

    #[test]
    fn numeric_second_arg_on_timestamp_is_error() {
        let udf = TruncUdf::new("trunc");
        // trunc(ts, 123): neither a string unit nor a numeric-digits call.
        let err = invoke(&udf, vec![ints_in(vec![0]), ints_in(vec![123])]).unwrap_err();
        assert!(err.to_string().contains("second argument must be a string"));
    }

    #[test]
    fn timestamp_typed_input() {
        let udf = TruncUdf::new("trunc");
        let ns = boundaries();
        let col = ColumnarValue::Array(Arc::new(TimestampNanosecondArray::from(ns.clone())) as ArrayRef);
        let out = invoke(&udf, vec![col, fmts_in(vec!["DD"; ns.len()])]).unwrap();
        // Output is the original Timestamp type with DD-floored values.
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!(),
        };
        assert_eq!(arr.data_type(), &DataType::Timestamp(TimeUnit::Nanosecond, None));
        let jan1 = 1_704_067_200i64 * NS_PER_S;
        let day = NS_PER_DAY;
        let vals = as_primitive_array::<arrow::datatypes::TimestampNanosecondType>(arr.as_ref())
            .values()
            .to_vec();
        assert_eq!(vals[0], jan1);
        assert_eq!(vals[4], jan1 + 31 * day);
    }
}
