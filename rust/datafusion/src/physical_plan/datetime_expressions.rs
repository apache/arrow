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

use crate::error::{ExecutionError, Result};
use arrow::array::{Array, ArrayRef, StringArray, TimestampNanosecondArray};
use chrono::prelude::*;

#[inline]
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

    // with an explict Z, using ' ' as a separator
    // Example: 2020-09-08 13:42:29Z
    if let Ok(ts) = Utc.datetime_from_str(s, "%Y-%m-%d %H:%M:%S%.fZ") {
        return Ok(ts.timestamp_nanos());
    }

    // Support timestamps without an explicit timezone offset, again
    // to be compatible with what Apache Spark SQL does.

    // without a timezone specifier as a local time, using T as a separator
    // Example: 2020-09-08T13:42:29.190855
    if let Ok(ts) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S.%f") {
        return Ok(ts.timestamp_nanos());
    }

    // without a timezone specifier as a local time, using ' ' as a separator
    // Example: 2020-09-08 13:42:29.190855
    if let Ok(ts) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S.%f") {
        return Ok(ts.timestamp_nanos());
    }

    // Note we don't pass along the error message from the underlying
    // chrono parsing because we tried several different format
    // strings and we don't know which the user was trying to
    // match. Ths any of the specific error messages is likely to be
    // be more confusing than helpful
    Err(ExecutionError::General(format!(
        "Error parsing '{}' as timestamp",
        s
    )))
}

/// convert an array of strings into `Timestamp(Nanosecond, None)`
pub fn to_timestamp(args: &[ArrayRef]) -> Result<TimestampNanosecondArray> {
    let num_rows = args[0].len();
    let mut ts_builder = TimestampNanosecondArray::builder(num_rows);
    let string_args =
        &args[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                ExecutionError::General(format!(
                    "Internal error: could not cast to_timestamp input to StringArray"
                ))
            })?;

    for i in 0..string_args.len() {
        if string_args.is_null(i) {
            ts_builder.append_null()?
        } else {
            ts_builder.append_value(string_to_timestamp_nanos(string_args.value(i))?)?;
        }
    }

    Ok(ts_builder.finish())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;

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

    #[test]
    fn string_to_timestamp_no_timezone() -> Result<()> {
        let expected_date_time = NaiveDateTime::new(
            NaiveDate::from_ymd(2020, 09, 08),
            NaiveTime::from_hms_nano(13, 42, 29, 190855),
        )
        .timestamp_nanos();

        // Ensure both T and ' ' variants work
        assert_eq!(
            expected_date_time,
            parse_timestamp("2020-09-08T13:42:29.190855")?
        );

        assert_eq!(
            expected_date_time,
            parse_timestamp("2020-09-08 13:42:29.190855")?
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
            Ok(v) => assert!(
                false,
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

        let mut string_builder = StringArray::builder(2);
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
