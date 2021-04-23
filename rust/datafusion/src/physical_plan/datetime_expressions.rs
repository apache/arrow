// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! DateTime expressions
use std::sync::Arc;

use super::ColumnarValue;
use crate::{
    error::{DataFusionError, Result},
    scalar::{ScalarType, ScalarValue},
};
use arrow::{
    array::{Array, ArrayRef, GenericStringArray, PrimitiveArray, StringOffsetSizeTrait},
    datatypes::{ArrowPrimitiveType, DataType, TimestampNanosecondType},
};
use arrow::{
    array::{
        Date32Array, Date64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    },
    compute::kernels::temporal,
    datatypes::TimeUnit,
    temporal_conversions::timestamp_ns_to_datetime,
};
use chrono::prelude::*;
use chrono::Duration;
use chrono::LocalResult;

#[inline]
/// Accepts a string in RFC3339 / ISO8601 standard format and some
/// variants and converts it to a nanosecond precision timestamp.
///
/// Implements the `to_timestamp` function to convert a string to a
/// timestamp, following the model of spark SQL’s to_`timestamp`.
///
/// In addition to RFC3339 / ISO8601 standard timestamps, it also
/// accepts strings that use a space ` ` to separate the date and time
/// as well as strings that have no explicit timezone offset.
///
/// Examples of accepted inputs:
/// * `1997-01-31T09:26:56.123Z`        # RCF3339
/// * `1997-01-31T09:26:56.123-05:00`   # RCF3339
/// * `1997-01-31 09:26:56.123-05:00`   # close to RCF3339 but with a space rather than T
/// * `1997-01-31T09:26:56.123`         # close to RCF3339 but no timezone offset specified
/// * `1997-01-31 09:26:56.123`         # close to RCF3339 but uses a space and no timezone offset
/// * `1997-01-31 09:26:56`             # close to RCF3339, no fractional seconds
//
/// Internally, this function uses the `chrono` library for the
/// datetime parsing
///
/// We hope to extend this function in the future with a second
/// parameter to specifying the format string.
///
/// ## Timestamp Precision
///
/// DataFusion uses the maximum precision timestamps supported by
/// Arrow (nanoseconds stored as a 64-bit integer) timestamps. This
/// means the range of dates that timestamps can represent is ~1677 AD
/// to 2262 AM
///
///
/// ## Timezone / Offset Handling
///
/// By using the Arrow format, DataFusion inherits Arrow’s handling of
/// timestamp values. Specifically, the stored numerical values of
/// timestamps are stored compared to offset UTC.
///
/// This function intertprets strings without an explicit time zone as
/// timestamps with offsets of the local time on the machine that ran
/// the datafusion query
///
/// For example, `1997-01-31 09:26:56.123Z` is interpreted as UTC, as
/// it has an explicit timezone specifier (“Z” for Zulu/UTC)
///
/// `1997-01-31T09:26:56.123` is interpreted as a local timestamp in
/// the timezone of the machine that ran DataFusion. For example, if
/// the system timezone is set to Americas/New_York (UTC-5) the
/// timestamp will be interpreted as though it were
/// `1997-01-31T09:26:56.123-05:00`
fn string_to_timestamp_nanos(s: &str) -> Result<i64> {
    // Fast path:  RFC3339 timestamp (with a T)
    // Example: 2020-09-08T13:42:29.190855Z
    if let Ok(ts) = DateTime::parse_from_rfc3339(s) {
        return Ok(ts.timestamp_nanos());
    }

    // Implement quasi-RFC3339 support by trying to parse the
    // timestamp with various other format specifiers to to support
    // separating the date and time with a space ' ' rather than 'T' to be
    // (more) compatible with Apache Spark SQL

    // timezone offset, using ' ' as a separator
    // Example: 2020-09-08 13:42:29.190855-05:00
    if let Ok(ts) = DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%:z") {
        return Ok(ts.timestamp_nanos());
    }

    // with an explicit Z, using ' ' as a separator
    // Example: 2020-09-08 13:42:29Z
    if let Ok(ts) = Utc.datetime_from_str(s, "%Y-%m-%d %H:%M:%S%.fZ") {
        return Ok(ts.timestamp_nanos());
    }

    // Support timestamps without an explicit timezone offset, again
    // to be compatible with what Apache Spark SQL does.

    // without a timezone specifier as a local time, using T as a separator
    // Example: 2020-09-08T13:42:29.190855
    if let Ok(ts) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S.%f") {
        return naive_datetime_to_timestamp(s, ts);
    }

    // without a timezone specifier as a local time, using T as a
    // separator, no fractional seconds
    // Example: 2020-09-08T13:42:29
    if let Ok(ts) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return naive_datetime_to_timestamp(s, ts);
    }

    // without a timezone specifier as a local time, using ' ' as a separator
    // Example: 2020-09-08 13:42:29.190855
    if let Ok(ts) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S.%f") {
        return naive_datetime_to_timestamp(s, ts);
    }

    // without a timezone specifier as a local time, using ' ' as a
    // separator, no fractional seconds
    // Example: 2020-09-08 13:42:29
    if let Ok(ts) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return naive_datetime_to_timestamp(s, ts);
    }

    // Note we don't pass along the error message from the underlying
    // chrono parsing because we tried several different format
    // strings and we don't know which the user was trying to
    // match. Ths any of the specific error messages is likely to be
    // be more confusing than helpful
    Err(DataFusionError::Execution(format!(
        "Error parsing '{}' as timestamp",
        s
    )))
}

/// Converts the naive datetime (which has no specific timezone) to a
/// nanosecond epoch timestamp relative to UTC.
fn naive_datetime_to_timestamp(s: &str, datetime: NaiveDateTime) -> Result<i64> {
    let l = Local {};

    match l.from_local_datetime(&datetime) {
        LocalResult::None => Err(DataFusionError::Execution(format!(
            "Error parsing '{}' as timestamp: local time representation is invalid",
            s
        ))),
        LocalResult::Single(local_datetime) => {
            Ok(local_datetime.with_timezone(&Utc).timestamp_nanos())
        }
        // Ambiguous times can happen if the timestamp is exactly when
        // a daylight savings time transition occurs, for example, and
        // so the datetime could validly be said to be in two
        // potential offsets. However, since we are about to convert
        // to UTC anyways, we can pick one arbitrarily
        LocalResult::Ambiguous(local_datetime, _) => {
            Ok(local_datetime.with_timezone(&Utc).timestamp_nanos())
        }
    }
}

// given a function `op` that maps a `&str` to a Result of an arrow native type,
// returns a `PrimitiveArray` after the application
// of the function to `args[0]`.
/// # Errors
/// This function errors iff:
/// * the number of arguments is not 1 or
/// * the first argument is not castable to a `GenericStringArray` or
/// * the function `op` errors
pub(crate) fn unary_string_to_primitive_function<'a, T, O, F>(
    args: &[&'a dyn Array],
    op: F,
    name: &str,
) -> Result<PrimitiveArray<O>>
where
    O: ArrowPrimitiveType,
    T: StringOffsetSizeTrait,
    F: Fn(&'a str) -> Result<O::Native>,
{
    if args.len() != 1 {
        return Err(DataFusionError::Internal(format!(
            "{:?} args were supplied but {} takes exactly one argument",
            args.len(),
            name,
        )));
    }

    let array = args[0]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .ok_or_else(|| {
            DataFusionError::Internal("failed to downcast to string".to_string())
        })?;

    // first map is the iterator, second is for the `Option<_>`
    array.iter().map(|x| x.map(|x| op(x)).transpose()).collect()
}

// given an function that maps a `&str` to a arrow native type,
// returns a `ColumnarValue` where the function is applied to either a `ArrayRef` or `ScalarValue`
// depending on the `args`'s variant.
fn handle<'a, O, F, S>(
    args: &'a [ColumnarValue],
    op: F,
    name: &str,
) -> Result<ColumnarValue>
where
    O: ArrowPrimitiveType,
    S: ScalarType<O::Native>,
    F: Fn(&'a str) -> Result<O::Native>,
{
    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8 => Ok(ColumnarValue::Array(Arc::new(
                unary_string_to_primitive_function::<i32, O, _>(&[a.as_ref()], op, name)?,
            ))),
            DataType::LargeUtf8 => Ok(ColumnarValue::Array(Arc::new(
                unary_string_to_primitive_function::<i64, O, _>(&[a.as_ref()], op, name)?,
            ))),
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function {}",
                other, name,
            ))),
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(a) => {
                let result = a.as_ref().map(|x| (op)(x)).transpose()?;
                Ok(ColumnarValue::Scalar(S::scalar(result)))
            }
            ScalarValue::LargeUtf8(a) => {
                let result = a.as_ref().map(|x| (op)(x)).transpose()?;
                Ok(ColumnarValue::Scalar(S::scalar(result)))
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function {}",
                other, name
            ))),
        },
    }
}

/// to_timestamp SQL function
pub fn to_timestamp(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle::<TimestampNanosecondType, _, TimestampNanosecondType>(
        args,
        string_to_timestamp_nanos,
        "to_timestamp",
    )
}

fn date_trunc_single(granularity: &str, value: i64) -> Result<i64> {
    let value = timestamp_ns_to_datetime(value).with_nanosecond(0);
    let value = match granularity {
        "second" => value,
        "minute" => value.and_then(|d| d.with_second(0)),
        "hour" => value
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0)),
        "day" => value
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0)),
        "week" => value
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .map(|d| d - Duration::seconds(60 * 60 * 24 * d.weekday() as i64)),
        "month" => value
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .and_then(|d| d.with_day0(0)),
        "year" => value
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .and_then(|d| d.with_day0(0))
            .and_then(|d| d.with_month0(0)),
        unsupported => {
            return Err(DataFusionError::Execution(format!(
                "Unsupported date_trunc granularity: {}",
                unsupported
            )))
        }
    };
    // `with_x(0)` are infalible because `0` are always a valid
    Ok(value.unwrap().timestamp_nanos())
}

/// date_trunc SQL function
pub fn date_trunc(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let (granularity, array) = (&args[0], &args[1]);

    let granularity =
        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) = granularity {
            v
        } else {
            return Err(DataFusionError::Execution(
                "Granularity of `date_trunc` must be non-null scalar Utf8".to_string(),
            ));
        };

    let f = |x: Option<i64>| x.map(|x| date_trunc_single(granularity, x)).transpose();

    Ok(match array {
        ColumnarValue::Scalar(scalar) => {
            if let ScalarValue::TimestampNanosecond(v) = scalar {
                ColumnarValue::Scalar(ScalarValue::TimestampNanosecond((f)(*v)?))
            } else {
                return Err(DataFusionError::Execution(
                    "array of `date_trunc` must be non-null scalar Utf8".to_string(),
                ));
            }
        }
        ColumnarValue::Array(array) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let array = array
                .iter()
                .map(f)
                .collect::<Result<TimestampNanosecondArray>>()?;

            ColumnarValue::Array(Arc::new(array))
        }
    })
}

macro_rules! extract_date_part {
    ($ARRAY: expr, $FN:expr) => {
        match $ARRAY.data_type() {
            DataType::Date32 => {
                let array = $ARRAY.as_any().downcast_ref::<Date32Array>().unwrap();
                Ok($FN(array)?)
            }
            DataType::Date64 => {
                let array = $ARRAY.as_any().downcast_ref::<Date64Array>().unwrap();
                Ok($FN(array)?)
            }
            DataType::Timestamp(time_unit, None) => match time_unit {
                TimeUnit::Second => {
                    let array = $ARRAY
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap();
                    Ok($FN(array)?)
                }
                TimeUnit::Millisecond => {
                    let array = $ARRAY
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    Ok($FN(array)?)
                }
                TimeUnit::Microsecond => {
                    let array = $ARRAY
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    Ok($FN(array)?)
                }
                TimeUnit::Nanosecond => {
                    let array = $ARRAY
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();
                    Ok($FN(array)?)
                }
            },
            datatype => Err(DataFusionError::Internal(format!(
                "Extract does not support datatype {:?}",
                datatype
            ))),
        }
    };
}

/// DATE_PART SQL function
pub fn date_part(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return Err(DataFusionError::Execution(
            "Expected two arguments in DATE_PART".to_string(),
        ));
    }
    let (date_part, array) = (&args[0], &args[1]);

    let date_part = if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) = date_part {
        v
    } else {
        return Err(DataFusionError::Execution(
            "First argument of `DATE_PART` must be non-null scalar Utf8".to_string(),
        ));
    };

    let is_scalar = matches!(array, ColumnarValue::Scalar(_));

    let array = match array {
        ColumnarValue::Array(array) => array.clone(),
        ColumnarValue::Scalar(scalar) => scalar.to_array(),
    };

    let arr = match date_part.to_lowercase().as_str() {
        "hour" => extract_date_part!(array, temporal::hour),
        "year" => extract_date_part!(array, temporal::year),
        _ => Err(DataFusionError::Execution(format!(
            "Date part '{}' not supported",
            date_part
        ))),
    }?;

    Ok(if is_scalar {
        ColumnarValue::Scalar(ScalarValue::try_from_array(
            &(Arc::new(arr) as ArrayRef),
            0,
        )?)
    } else {
        ColumnarValue::Array(Arc::new(arr))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, StringBuilder};

    use super::*;

    #[test]
    fn to_timestamp_arrays_and_nulls() -> Result<()> {
        // ensure that arrow array implementation is wired up and handles nulls correctly

        let mut string_builder = StringBuilder::new(2);
        let mut ts_builder = TimestampNanosecondArray::builder(2);

        string_builder.append_value("2020-09-08T13:42:29.190855Z")?;
        ts_builder.append_value(1599572549190855000)?;

        string_builder.append_null()?;
        ts_builder.append_null()?;
        let expected_timestamps = &ts_builder.finish() as &dyn Array;

        let string_array =
            ColumnarValue::Array(Arc::new(string_builder.finish()) as ArrayRef);
        let parsed_timestamps = to_timestamp(&[string_array])
            .expect("that to_timestamp parsed values without error");
        if let ColumnarValue::Array(parsed_array) = parsed_timestamps {
            assert_eq!(parsed_array.len(), 2);
            assert_eq!(expected_timestamps, parsed_array.as_ref());
        } else {
            panic!("Expected a columnar array")
        }
        Ok(())
    }

    #[test]
    fn date_trunc_test() {
        let cases = vec![
            (
                "2020-09-08T13:42:29.190855Z",
                "second",
                "2020-09-08T13:42:29.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "minute",
                "2020-09-08T13:42:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "hour",
                "2020-09-08T13:00:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "day",
                "2020-09-08T00:00:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "week",
                "2020-09-07T00:00:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "month",
                "2020-09-01T00:00:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "year",
                "2020-01-01T00:00:00.000000Z",
            ),
            (
                "2021-01-01T13:42:29.190855Z",
                "week",
                "2020-12-28T00:00:00.000000Z",
            ),
            (
                "2020-01-01T13:42:29.190855Z",
                "week",
                "2019-12-30T00:00:00.000000Z",
            ),
        ];

        cases.iter().for_each(|(original, granularity, expected)| {
            let original = string_to_timestamp_nanos(original).unwrap();
            let expected = string_to_timestamp_nanos(expected).unwrap();
            let result = date_trunc_single(granularity, original).unwrap();
            assert_eq!(result, expected);
        });
    }

    #[test]
    fn to_timestamp_invalid_input_type() -> Result<()> {
        // pass the wrong type of input array to to_timestamp and test
        // that we get an error.

        let mut builder = Int64Array::builder(1);
        builder.append_value(1)?;
        let int64array = ColumnarValue::Array(Arc::new(builder.finish()));

        let expected_err =
            "Internal error: Unsupported data type Int64 for function to_timestamp";
        match to_timestamp(&[int64array]) {
            Ok(_) => panic!("Expected error but got success"),
            Err(e) => {
                assert!(
                    e.to_string().contains(expected_err),
                    "Can not find expected error '{}'. Actual error '{}'",
                    expected_err,
                    e
                );
            }
        }
        Ok(())
    }
}
