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

use crate::error::{DataFusionError, Result};
use arrow::{
    array::{Array, ArrayData, ArrayRef, StringArray, TimestampNanosecondArray},
    buffer::Buffer,
    datatypes::{DataType, TimeUnit, ToByteSlice},
};
use chrono::Duration;
use chrono::{prelude::*, LocalResult};

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

/// convert an array of strings into `Timestamp(Nanosecond, None)`
pub fn to_timestamp(args: &[ArrayRef]) -> Result<TimestampNanosecondArray> {
    let num_rows = args[0].len();
    let string_args =
        &args[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "could not cast to_timestamp input to StringArray".to_string(),
                )
            })?;

    let result = (0..num_rows)
        .map(|i| {
            if string_args.is_null(i) {
                // NB: Since we use the same null bitset as the input,
                // the output for this value will be ignored, but we
                // need some value in the array we are building.
                Ok(0)
            } else {
                string_to_timestamp_nanos(string_args.value(i))
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let data = ArrayData::new(
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        num_rows,
        Some(string_args.null_count()),
        string_args.data().null_buffer().cloned(),
        0,
        vec![Buffer::from(result.to_byte_slice())],
        vec![],
    );

    Ok(TimestampNanosecondArray::from(Arc::new(data)))
}

/// date_trunc SQL function
pub fn date_trunc(args: &[ArrayRef]) -> Result<TimestampNanosecondArray> {
    let granularity_array =
        &args[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "Could not cast date_trunc granularity input to StringArray"
                        .to_string(),
                )
            })?;

    let array = &args[1]
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "Could not cast date_trunc array input to TimestampNanosecondArray"
                    .to_string(),
            )
        })?;

    let range = 0..array.len();
    let result = range
        .map(|i| {
            if array.is_null(i) {
                Ok(0_i64)
            } else {
                let date_time = match granularity_array.value(i) {
                    "second" => array
                        .value_as_datetime(i)
                        .and_then(|d| d.with_nanosecond(0)),
                    "minute" => array
                        .value_as_datetime(i)
                        .and_then(|d| d.with_nanosecond(0))
                        .and_then(|d| d.with_second(0)),
                    "hour" => array
                        .value_as_datetime(i)
                        .and_then(|d| d.with_nanosecond(0))
                        .and_then(|d| d.with_second(0))
                        .and_then(|d| d.with_minute(0)),
                    "day" => array
                        .value_as_datetime(i)
                        .and_then(|d| d.with_nanosecond(0))
                        .and_then(|d| d.with_second(0))
                        .and_then(|d| d.with_minute(0))
                        .and_then(|d| d.with_hour(0)),
                    "week" => array
                        .value_as_datetime(i)
                        .and_then(|d| d.with_nanosecond(0))
                        .and_then(|d| d.with_second(0))
                        .and_then(|d| d.with_minute(0))
                        .and_then(|d| d.with_hour(0))
                        .map(|d| {
                            d - Duration::seconds(60 * 60 * 24 * d.weekday() as i64)
                        }),
                    "month" => array
                        .value_as_datetime(i)
                        .and_then(|d| d.with_nanosecond(0))
                        .and_then(|d| d.with_second(0))
                        .and_then(|d| d.with_minute(0))
                        .and_then(|d| d.with_hour(0))
                        .and_then(|d| d.with_day0(0)),
                    "year" => array
                        .value_as_datetime(i)
                        .and_then(|d| d.with_nanosecond(0))
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
                date_time.map(|d| d.timestamp_nanos()).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Can't truncate date time: {:?}",
                        array.value_as_datetime(i)
                    ))
                })
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let data = ArrayData::new(
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        array.len(),
        Some(array.null_count()),
        array.data().null_buffer().cloned(),
        0,
        vec![Buffer::from(result.to_byte_slice())],
        vec![],
    );

    Ok(TimestampNanosecondArray::from(Arc::new(data)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringBuilder};

    use super::*;

    #[test]
    fn string_to_timestamp_timezone() -> Result<()> {
        // Explicit timezone
        assert_eq!(
            1599572549190855000,
            parse_timestamp("2020-09-08T13:42:29.190855+00:00")?
        );
        assert_eq!(
            1599572549190855000,
            parse_timestamp("2020-09-08T13:42:29.190855Z")?
        );
        assert_eq!(
            1599572549000000000,
            parse_timestamp("2020-09-08T13:42:29Z")?
        ); // no fractional part
        assert_eq!(
            1599590549190855000,
            parse_timestamp("2020-09-08T13:42:29.190855-05:00")?
        );
        Ok(())
    }

    #[test]
    fn string_to_timestamp_timezone_space() -> Result<()> {
        // Ensure space rather than T between time and date is accepted
        assert_eq!(
            1599572549190855000,
            parse_timestamp("2020-09-08 13:42:29.190855+00:00")?
        );
        assert_eq!(
            1599572549190855000,
            parse_timestamp("2020-09-08 13:42:29.190855Z")?
        );
        assert_eq!(
            1599572549000000000,
            parse_timestamp("2020-09-08 13:42:29Z")?
        ); // no fractional part
        assert_eq!(
            1599590549190855000,
            parse_timestamp("2020-09-08 13:42:29.190855-05:00")?
        );
        Ok(())
    }

    /// Interprets a naive_datetime (with no explicit timzone offset)
    /// using the local timezone and returns the timestamp in UTC (0
    /// offset)
    fn naive_datetime_to_timestamp(naive_datetime: &NaiveDateTime) -> i64 {
        // Note: Use chrono APIs that are different than
        // naive_datetime_to_timestamp to compute the utc offset to
        // try and double check the logic
        let utc_offset_secs = match Local.offset_from_local_datetime(&naive_datetime) {
            LocalResult::Single(local_offset) => {
                local_offset.fix().local_minus_utc() as i64
            }
            _ => panic!("Unexpected failure converting to local datetime"),
        };
        let utc_offset_nanos = utc_offset_secs * 1_000_000_000;
        naive_datetime.timestamp_nanos() - utc_offset_nanos
    }

    #[test]
    fn string_to_timestamp_no_timezone() -> Result<()> {
        // This test is designed to succeed in regardless of the local
        // timezone the test machine is running. Thus it is still
        // somewhat suceptable to bugs in the use of chrono
        let naive_datetime = NaiveDateTime::new(
            NaiveDate::from_ymd(2020, 9, 8),
            NaiveTime::from_hms_nano(13, 42, 29, 190855),
        );

        // Ensure both T and ' ' variants work
        assert_eq!(
            naive_datetime_to_timestamp(&naive_datetime),
            parse_timestamp("2020-09-08T13:42:29.190855")?
        );

        assert_eq!(
            naive_datetime_to_timestamp(&naive_datetime),
            parse_timestamp("2020-09-08 13:42:29.190855")?
        );

        // Also ensure that parsing timestamps with no fractional
        // second part works as well
        let naive_datetime_whole_secs = NaiveDateTime::new(
            NaiveDate::from_ymd(2020, 9, 8),
            NaiveTime::from_hms(13, 42, 29),
        );

        // Ensure both T and ' ' variants work
        assert_eq!(
            naive_datetime_to_timestamp(&naive_datetime_whole_secs),
            parse_timestamp("2020-09-08T13:42:29")?
        );

        assert_eq!(
            naive_datetime_to_timestamp(&naive_datetime_whole_secs),
            parse_timestamp("2020-09-08 13:42:29")?
        );

        Ok(())
    }

    #[test]
    fn string_to_timestamp_invalid() -> Result<()> {
        // Test parsing invalid formats

        // It would be nice to make these messages better
        expect_timestamp_parse_error("", "Error parsing '' as timestamp");
        expect_timestamp_parse_error("SS", "Error parsing 'SS' as timestamp");
        expect_timestamp_parse_error(
            "Wed, 18 Feb 2015 23:16:09 GMT",
            "Error parsing 'Wed, 18 Feb 2015 23:16:09 GMT' as timestamp",
        );

        Ok(())
    }

    // Parse a timestamp to timestamp int with a useful human readable error message
    fn parse_timestamp(s: &str) -> Result<i64> {
        let result = string_to_timestamp_nanos(s);
        if let Err(e) = &result {
            eprintln!("Error parsing timestamp '{}': {:?}", s, e);
        }
        result
    }

    fn expect_timestamp_parse_error(s: &str, expected_err: &str) {
        match string_to_timestamp_nanos(s) {
            Ok(v) => panic!(
                "Expected error '{}' while parsing '{}', but parsed {} instead",
                expected_err, s, v
            ),
            Err(e) => {
                assert!(e.to_string().contains(expected_err),
                        "Can not find expected error '{}' while parsing '{}'. Actual error '{}'",
                        expected_err, s, e);
            }
        }
    }

    #[test]
    fn to_timestamp_arrays_and_nulls() -> Result<()> {
        // ensure that arrow array implementation is wired up and handles nulls correctly

        let mut string_builder = StringBuilder::new(2);
        let mut ts_builder = TimestampNanosecondArray::builder(2);

        string_builder.append_value("2020-09-08T13:42:29.190855Z")?;
        ts_builder.append_value(1599572549190855000)?;

        string_builder.append_null()?;
        ts_builder.append_null()?;

        let string_array = Arc::new(string_builder.finish());
        let parsed_timestamps = to_timestamp(&[string_array])
            .expect("that to_timestamp parsed values without error");

        let expected_timestamps = ts_builder.finish();

        assert_eq!(parsed_timestamps.len(), 2);
        assert_eq!(expected_timestamps, parsed_timestamps);
        Ok(())
    }

    #[test]
    fn date_trunc_test() -> Result<()> {
        let mut ts_builder = StringBuilder::new(2);
        let mut truncated_builder = StringBuilder::new(2);
        let mut string_builder = StringBuilder::new(2);

        ts_builder.append_null()?;
        truncated_builder.append_null()?;
        string_builder.append_value("second")?;

        ts_builder.append_value("2020-09-08T13:42:29.190855Z")?;
        truncated_builder.append_value("2020-09-08T13:42:29.000000Z")?;
        string_builder.append_value("second")?;

        ts_builder.append_value("2020-09-08T13:42:29.190855Z")?;
        truncated_builder.append_value("2020-09-08T13:42:00.000000Z")?;
        string_builder.append_value("minute")?;

        ts_builder.append_value("2020-09-08T13:42:29.190855Z")?;
        truncated_builder.append_value("2020-09-08T13:00:00.000000Z")?;
        string_builder.append_value("hour")?;

        ts_builder.append_value("2020-09-08T13:42:29.190855Z")?;
        truncated_builder.append_value("2020-09-08T00:00:00.000000Z")?;
        string_builder.append_value("day")?;

        ts_builder.append_value("2020-09-08T13:42:29.190855Z")?;
        truncated_builder.append_value("2020-09-07T00:00:00.000000Z")?;
        string_builder.append_value("week")?;

        ts_builder.append_value("2020-09-08T13:42:29.190855Z")?;
        truncated_builder.append_value("2020-09-01T00:00:00.000000Z")?;
        string_builder.append_value("month")?;

        ts_builder.append_value("2020-09-08T13:42:29.190855Z")?;
        truncated_builder.append_value("2020-01-01T00:00:00.000000Z")?;
        string_builder.append_value("year")?;

        ts_builder.append_value("2021-01-01T13:42:29.190855Z")?;
        truncated_builder.append_value("2020-12-28T00:00:00.000000Z")?;
        string_builder.append_value("week")?;

        ts_builder.append_value("2020-01-01T13:42:29.190855Z")?;
        truncated_builder.append_value("2019-12-30T00:00:00.000000Z")?;
        string_builder.append_value("week")?;

        let string_array = Arc::new(string_builder.finish());
        let ts_array = Arc::new(to_timestamp(&[Arc::new(ts_builder.finish())]).unwrap());
        let date_trunc_array = date_trunc(&[string_array, ts_array])
            .expect("that to_timestamp parsed values without error");

        let expected_timestamps =
            to_timestamp(&[Arc::new(truncated_builder.finish())]).unwrap();

        assert_eq!(date_trunc_array, expected_timestamps);
        Ok(())
    }

    #[test]
    fn to_timestamp_invalid_input_type() -> Result<()> {
        // pass the wrong type of input array to to_timestamp and test
        // that we get an error.

        let mut builder = Int64Array::builder(1);
        builder.append_value(1)?;
        let int64array = Arc::new(builder.finish());

        let expected_err =
            "Internal error: could not cast to_timestamp input to StringArray";
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
