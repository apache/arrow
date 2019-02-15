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

//! Implement [`Record`] for [`Time`], [`Date`], and [`Timestamp`].

use chrono::{Local, NaiveTime, TimeZone, Timelike, Utc};
use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
    fmt::{self, Display},
    result,
};

use crate::{
    basic::Repetition,
    column::reader::ColumnReader,
    data_type::{Int96, Int96Type},
    errors::{ParquetError, Result},
    record::{
        reader::{I96Reader, MapReader},
        schemas::{DateSchema, I32Schema, I64Schema, TimeSchema, TimestampSchema},
        triplet::TypedTripletIter,
        types::{downcast, Value},
        Reader, Record,
    },
    schema::types::{ColumnPath, Type},
};

const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;
const SECONDS_PER_DAY: i64 = 86_400;
const MILLIS_PER_SECOND: i64 = 1_000;
const MICROS_PER_MILLI: i64 = 1_000;
const NANOS_PER_MICRO: i64 = 1_000;

/// Corresponds to the [Date logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#date).
#[derive(Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Date(pub(super) i32);
impl Date {
    /// Create a Date from the number of days since the Unix epoch
    pub fn from_days(days: i32) -> Self {
        Date(days)
    }
    /// Get the number of days since the Unix epoch
    pub fn as_days(&self) -> i32 {
        self.0
    }
}
impl Record for Date {
    type Schema = DateSchema;
    type Reader = impl Reader<Item = Self>;

    fn parse(
        schema: &Type,
        repetition: Option<Repetition>,
    ) -> Result<(String, Self::Schema)> {
        Value::parse(schema, repetition).and_then(downcast)
    }

    fn reader(
        _schema: &Self::Schema,
        path: &mut Vec<String>,
        def_level: i16,
        rep_level: i16,
        paths: &mut HashMap<ColumnPath, ColumnReader>,
        batch_size: usize,
    ) -> Self::Reader {
        MapReader(
            i32::reader(&I32Schema, path, def_level, rep_level, paths, batch_size),
            |days| Ok(Date(days)),
        )
    }
}
impl From<chrono::Date<Utc>> for Date {
    fn from(date: chrono::Date<Utc>) -> Self {
        Date(
            (date
                .and_time(NaiveTime::from_hms(0, 0, 0))
                .unwrap()
                .timestamp()
                / SECONDS_PER_DAY)
                .try_into()
                .unwrap(),
        )
    }
}
impl From<Date> for chrono::Date<Utc> {
    fn from(date: Date) -> Self {
        let x = Utc.timestamp(i64::from(date.0) * SECONDS_PER_DAY, 0);
        assert_eq!(x.time(), NaiveTime::from_hms(0, 0, 0));
        x.date()
    }
}
impl Display for Date {
    // Input is a number of days since the epoch in UTC.
    // Date is displayed in local timezone.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let dt = Local
            .timestamp(i64::from(self.0) * SECONDS_PER_DAY, 0)
            .date();
        dt.format("%Y-%m-%d %:z").fmt(f)
    }
}

/// Corresponds to the [Time logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#time).
#[derive(Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Time(pub(super) u64);
impl Time {
    /// Create a Time from the number of milliseconds since midnight
    pub fn from_millis(millis: u32) -> Option<Self> {
        if millis < (SECONDS_PER_DAY * MILLIS_PER_SECOND) as u32 {
            Some(Time(millis as u64 * MICROS_PER_MILLI as u64))
        } else {
            None
        }
    }
    /// Create a Time from the number of microseconds since midnight
    pub fn from_micros(micros: u64) -> Option<Self> {
        if micros < (SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI) as u64 {
            Some(Time(micros as u64))
        } else {
            None
        }
    }
    /// Get the number of milliseconds since midnight
    pub fn as_millis(&self) -> u32 {
        (self.0 / MICROS_PER_MILLI as u64) as u32
    }
    /// Get the number of microseconds since midnight
    pub fn as_micros(&self) -> u64 {
        self.0
    }
}
impl Record for Time {
    type Schema = TimeSchema;
    type Reader = impl Reader<Item = Self>;

    fn parse(
        schema: &Type,
        repetition: Option<Repetition>,
    ) -> Result<(String, Self::Schema)> {
        Value::parse(schema, repetition).and_then(downcast)
    }

    fn reader(
        schema: &Self::Schema,
        path: &mut Vec<String>,
        def_level: i16,
        rep_level: i16,
        paths: &mut HashMap<ColumnPath, ColumnReader>,
        batch_size: usize,
    ) -> Self::Reader {
        match schema {
            TimeSchema::Micros => sum::Sum2::A(MapReader(
                i64::reader(&I64Schema, path, def_level, rep_level, paths, batch_size),
                |micros: i64| {
                    micros
                        .try_into()
                        .ok()
                        .and_then(Time::from_micros)
                        .ok_or_else(|| {
                            ParquetError::General(format!(
                                "Invalid Time Micros {}",
                                micros
                            ))
                        })
                },
            )),
            TimeSchema::Millis => sum::Sum2::B(MapReader(
                i32::reader(&I32Schema, path, def_level, rep_level, paths, batch_size),
                |millis: i32| {
                    millis
                        .try_into()
                        .ok()
                        .and_then(Time::from_millis)
                        .ok_or_else(|| {
                            ParquetError::General(format!(
                                "Invalid Time Millis {}",
                                millis
                            ))
                        })
                },
            )),
        }
    }
}
impl From<NaiveTime> for Time {
    fn from(time: NaiveTime) -> Self {
        Time(
            time.num_seconds_from_midnight() as u64
                * (MILLIS_PER_SECOND * MICROS_PER_MILLI) as u64
                + time.nanosecond() as u64 / NANOS_PER_MICRO as u64,
        )
    }
}
impl From<Time> for NaiveTime {
    fn from(time: Time) -> Self {
        NaiveTime::from_num_seconds_from_midnight(
            (time.0 / (MICROS_PER_MILLI * MILLIS_PER_SECOND) as u64) as u32,
            (time.0 % (MICROS_PER_MILLI * MILLIS_PER_SECOND) as u64
                * NANOS_PER_MICRO as u64) as u32,
        )
    }
}
impl Display for Time {
    // Input is a number of microseconds since midnight.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let dt = NaiveTime::from_num_seconds_from_midnight(
            (self.0 as i64 / MICROS_PER_MILLI / MILLIS_PER_SECOND)
                .try_into()
                .unwrap(),
            (self.0 as i64 % MICROS_PER_MILLI / MILLIS_PER_SECOND)
                .try_into()
                .unwrap(),
        );
        dt.format("%H:%M:%S").fmt(f)
    }
}

/// Corresponds to the [Timestamp logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp).
#[derive(Copy, Clone, Hash, PartialEq, Eq, Debug)]
pub struct Timestamp(pub(super) Int96);
impl Timestamp {
    /// Create a Timestamp from the number of days and nanoseconds since the Julian epoch
    pub fn from_day_nanos(days: i64, nanos: i64) -> Self {
        Timestamp(Int96::from(vec![
            (nanos & 0xffffffff).try_into().unwrap(),
            ((nanos as u64) >> 32).try_into().unwrap(),
            days.try_into().unwrap(),
        ]))
    }

    /// Create a Timestamp from the number of milliseconds since the Unix epoch
    pub fn from_millis(millis: i64) -> Self {
        let day: i64 =
            JULIAN_DAY_OF_EPOCH + millis / (SECONDS_PER_DAY * MILLIS_PER_SECOND);
        let nanoseconds: i64 = (millis
            - ((day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY * MILLIS_PER_SECOND))
            * MICROS_PER_MILLI
            * NANOS_PER_MICRO;

        Timestamp(Int96::from(vec![
            (nanoseconds & 0xffffffff).try_into().unwrap(),
            ((nanoseconds as u64) >> 32).try_into().unwrap(),
            day.try_into().unwrap(),
        ]))
    }

    /// Create a Timestamp from the number of microseconds since the Unix epoch
    pub fn from_micros(micros: i64) -> Self {
        let day: i64 = JULIAN_DAY_OF_EPOCH
            + micros / (SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI);
        let nanoseconds: i64 = (micros
            - ((day - JULIAN_DAY_OF_EPOCH)
                * SECONDS_PER_DAY
                * MILLIS_PER_SECOND
                * MICROS_PER_MILLI))
            * NANOS_PER_MICRO;

        Timestamp(Int96::from(vec![
            (nanoseconds & 0xffffffff).try_into().unwrap(),
            ((nanoseconds as u64) >> 32).try_into().unwrap(),
            day.try_into().unwrap(),
        ]))
    }

    /// Create a Timestamp from the number of nanoseconds since the Unix epoch
    pub fn from_nanos(nanos: i64) -> Self {
        let day: i64 = JULIAN_DAY_OF_EPOCH
            + nanos
                / (SECONDS_PER_DAY
                    * MILLIS_PER_SECOND
                    * MICROS_PER_MILLI
                    * NANOS_PER_MICRO);
        let nanoseconds: i64 = nanos
            - ((day - JULIAN_DAY_OF_EPOCH)
                * SECONDS_PER_DAY
                * MILLIS_PER_SECOND
                * MICROS_PER_MILLI
                * NANOS_PER_MICRO);

        Timestamp(Int96::from(vec![
            (nanoseconds & 0xffffffff).try_into().unwrap(),
            ((nanoseconds as u64) >> 32).try_into().unwrap(),
            day.try_into().unwrap(),
        ]))
    }

    /// Get the number of days and nanoseconds since the Julian epoch
    pub fn as_day_nanos(&self) -> (i64, i64) {
        let day = i64::from(self.0.data()[2]);
        let nanoseconds =
            (i64::from(self.0.data()[1]) << 32) + i64::from(self.0.data()[0]);
        (day, nanoseconds)
    }

    /// Get the number of milliseconds since the Unix epoch
    pub fn as_millis(&self) -> Option<i64> {
        let day = i64::from(self.0.data()[2]);
        let nanoseconds =
            (i64::from(self.0.data()[1]) << 32) + i64::from(self.0.data()[0]);
        let seconds = day
            .checked_sub(JULIAN_DAY_OF_EPOCH)?
            .checked_mul(SECONDS_PER_DAY)?;
        Some(
            seconds.checked_mul(MILLIS_PER_SECOND)?.checked_add(
                nanoseconds
                    .checked_div(NANOS_PER_MICRO)?
                    .checked_div(MICROS_PER_MILLI)?,
            )?,
        )
    }

    /// Get the number of microseconds since the Unix epoch
    pub fn as_micros(&self) -> Option<i64> {
        let day = i64::from(self.0.data()[2]);
        let nanoseconds =
            (i64::from(self.0.data()[1]) << 32) + i64::from(self.0.data()[0]);
        let seconds = day
            .checked_sub(JULIAN_DAY_OF_EPOCH)?
            .checked_mul(SECONDS_PER_DAY)?;
        Some(
            seconds
                .checked_mul(MILLIS_PER_SECOND)?
                .checked_mul(MICROS_PER_MILLI)?
                .checked_add(nanoseconds.checked_div(NANOS_PER_MICRO)?)?,
        )
    }

    /// Get the number of nanoseconds since the Unix epoch
    pub fn as_nanos(&self) -> Option<i64> {
        let day = i64::from(self.0.data()[2]);
        let nanoseconds =
            (i64::from(self.0.data()[1]) << 32) + i64::from(self.0.data()[0]);
        let seconds = day
            .checked_sub(JULIAN_DAY_OF_EPOCH)?
            .checked_mul(SECONDS_PER_DAY)?;
        Some(
            seconds
                .checked_mul(MILLIS_PER_SECOND)?
                .checked_mul(MICROS_PER_MILLI)?
                .checked_mul(NANOS_PER_MICRO)?
                .checked_add(nanoseconds)?,
        )
    }
}
impl Record for Timestamp {
    type Schema = TimestampSchema;
    type Reader = impl Reader<Item = Self>;

    fn parse(
        schema: &Type,
        repetition: Option<Repetition>,
    ) -> Result<(String, Self::Schema)> {
        Value::parse(schema, repetition).and_then(downcast)
    }

    fn reader(
        schema: &Self::Schema,
        path: &mut Vec<String>,
        def_level: i16,
        rep_level: i16,
        paths: &mut HashMap<ColumnPath, ColumnReader>,
        batch_size: usize,
    ) -> Self::Reader {
        match schema {
            TimestampSchema::Int96 => sum::Sum3::A(MapReader(
                {
                    let col_path = ColumnPath::new(path.to_vec());
                    let col_reader = paths.remove(&col_path).unwrap();
                    I96Reader {
                        column: TypedTripletIter::<Int96Type>::new(
                            def_level, rep_level, col_reader, batch_size,
                        ),
                    }
                },
                |x| Ok(Timestamp(x)),
            )),
            TimestampSchema::Millis => sum::Sum3::B(MapReader(
                i64::reader(&I64Schema, path, def_level, rep_level, paths, batch_size),
                |x| Ok(Timestamp::from_millis(x)),
            )),
            TimestampSchema::Micros => sum::Sum3::C(MapReader(
                i64::reader(&I64Schema, path, def_level, rep_level, paths, batch_size),
                |x| Ok(Timestamp::from_micros(x)),
            )),
        }
    }
}
impl From<chrono::DateTime<Utc>> for Timestamp {
    fn from(timestamp: chrono::DateTime<Utc>) -> Self {
        Timestamp::from_nanos(timestamp.timestamp_nanos())
    }
}
impl TryFrom<Timestamp> for chrono::DateTime<Utc> {
    type Error = ();

    fn try_from(timestamp: Timestamp) -> result::Result<Self, Self::Error> {
        Ok(Utc.timestamp(
            timestamp.as_millis().unwrap() / MILLIS_PER_SECOND,
            (timestamp.as_day_nanos().1
                % (MILLIS_PER_SECOND * MICROS_PER_MILLI * NANOS_PER_MICRO))
                as u32,
        ))
    }
}
impl Display for Timestamp {
    // Datetime is displayed in local timezone.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let dt = Local.timestamp(self.as_millis().unwrap() / MILLIS_PER_SECOND, 0);
        dt.format("%Y-%m-%d %H:%M:%S %:z").fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::NaiveDate;

    #[test]
    fn test_int96() {
        let value = Timestamp(Int96::from(vec![0, 0, 2454923]));
        assert_eq!(value.as_millis().unwrap(), 1238544000000);

        let value = Timestamp(Int96::from(vec![4165425152, 13, 2454923]));
        assert_eq!(value.as_millis().unwrap(), 1238544060000);

        let value = Timestamp(Int96::from(vec![0, 0, 0]));
        assert_eq!(value.as_millis().unwrap(), -210866803200000);
    }

    #[test]
    fn test_convert_date_to_string() {
        fn check_date_conversion(y: i32, m: u32, d: u32) {
            let datetime = NaiveDate::from_ymd(y, m, d).and_hms(0, 0, 0);
            let dt = Local.from_utc_datetime(&datetime);
            let date = Date((dt.timestamp() / SECONDS_PER_DAY) as i32);
            assert_eq!(date.to_string(), dt.format("%Y-%m-%d %:z").to_string());
            let date2 = Date::from(<chrono::Date<Utc>>::from(date));
            assert_eq!(date, date2);
        }

        check_date_conversion(2010, 01, 02);
        check_date_conversion(2014, 05, 01);
        check_date_conversion(2016, 02, 29);
        check_date_conversion(2017, 09, 12);
        check_date_conversion(2018, 03, 31);
    }

    #[test]
    fn test_convert_time_to_string() {
        fn check_time_conversion(h: u32, mi: u32, s: u32) {
            let chrono_time = NaiveTime::from_hms(h, mi, s);
            let time = Time::from(chrono_time);
            assert_eq!(time.to_string(), chrono_time.format("%H:%M:%S").to_string());
        }

        check_time_conversion(13, 12, 54);
        check_time_conversion(08, 23, 01);
        check_time_conversion(11, 06, 32);
        check_time_conversion(16, 38, 00);
        check_time_conversion(21, 15, 12);
    }

    #[test]
    fn test_convert_timestamp_to_string() {
        fn check_datetime_conversion(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) {
            let datetime = NaiveDate::from_ymd(y, m, d).and_hms(h, mi, s);
            let dt = Local.from_utc_datetime(&datetime);
            let res = Timestamp::from_nanos(dt.timestamp_nanos()).to_string();
            let exp = dt.format("%Y-%m-%d %H:%M:%S %:z").to_string();
            assert_eq!(res, exp);
        }

        check_datetime_conversion(2010, 01, 02, 13, 12, 54);
        check_datetime_conversion(2011, 01, 03, 08, 23, 01);
        check_datetime_conversion(2012, 04, 05, 11, 06, 32);
        check_datetime_conversion(2013, 05, 12, 16, 38, 00);
        check_datetime_conversion(2014, 11, 28, 21, 15, 12);
    }
}
