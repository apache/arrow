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
    compute::kernels::cast_utils::string_to_timestamp_nanos,
    datatypes::{DataType, TimeUnit, ToByteSlice},
};
use chrono::prelude::*;
use chrono::Duration;

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
                    .map_err(DataFusionError::ArrowError)
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
