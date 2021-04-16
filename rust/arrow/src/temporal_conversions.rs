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

//! Conversion methods for dates and times.

use chrono::{Duration, NaiveDateTime, NaiveTime};

/// Number of seconds in a day
const SECONDS_IN_DAY: i64 = 86_400;
/// Number of milliseconds in a second
const MILLISECONDS: i64 = 1_000;
/// Number of microseconds in a second
const MICROSECONDS: i64 = 1_000_000;
/// Number of nanoseconds in a second
const NANOSECONDS: i64 = 1_000_000_000;

/// converts a `i32` representing a `date32` to [`NaiveDateTime`]
#[inline]
pub fn date32_to_datetime(v: i32) -> NaiveDateTime {
    NaiveDateTime::from_timestamp(v as i64 * SECONDS_IN_DAY, 0)
}

/// converts a `i64` representing a `date64` to [`NaiveDateTime`]
#[inline]
pub fn date64_to_datetime(v: i64) -> NaiveDateTime {
    NaiveDateTime::from_timestamp(
        // extract seconds from milliseconds
        v / MILLISECONDS,
        // discard extracted seconds and convert milliseconds to nanoseconds
        (v % MILLISECONDS * MICROSECONDS) as u32,
    )
}

/// converts a `i32` representing a `time32(s)` to [`NaiveDateTime`]
#[inline]
pub fn time32s_to_time(v: i32) -> NaiveTime {
    NaiveTime::from_num_seconds_from_midnight(v as u32, 0)
}

/// converts a `i32` representing a `time32(ms)` to [`NaiveDateTime`]
#[inline]
pub fn time32ms_to_time(v: i32) -> NaiveTime {
    let v = v as i64;
    NaiveTime::from_num_seconds_from_midnight(
        // extract seconds from milliseconds
        (v / MILLISECONDS) as u32,
        // discard extracted seconds and convert milliseconds to
        // nanoseconds
        (v % MILLISECONDS * MICROSECONDS) as u32,
    )
}

/// converts a `i64` representing a `time64(us)` to [`NaiveDateTime`]
#[inline]
pub fn time64us_to_time(v: i64) -> NaiveTime {
    NaiveTime::from_num_seconds_from_midnight(
        // extract seconds from microseconds
        (v / MICROSECONDS) as u32,
        // discard extracted seconds and convert microseconds to
        // nanoseconds
        (v % MICROSECONDS * MILLISECONDS) as u32,
    )
}

/// converts a `i64` representing a `time64(ns)` to [`NaiveDateTime`]
#[inline]
pub fn time64ns_to_time(v: i64) -> NaiveTime {
    NaiveTime::from_num_seconds_from_midnight(
        // extract seconds from nanoseconds
        (v / NANOSECONDS) as u32,
        // discard extracted seconds
        (v % NANOSECONDS) as u32,
    )
}

/// converts a `i64` representing a `timestamp(s)` to [`NaiveDateTime`]
#[inline]
pub fn timestamp_s_to_datetime(v: i64) -> NaiveDateTime {
    NaiveDateTime::from_timestamp(v, 0)
}

/// converts a `i64` representing a `timestamp(ms)` to [`NaiveDateTime`]
#[inline]
pub fn timestamp_ms_to_datetime(v: i64) -> NaiveDateTime {
    NaiveDateTime::from_timestamp(
        // extract seconds from milliseconds
        v / MILLISECONDS,
        // discard extracted seconds and convert milliseconds to nanoseconds
        (v % MILLISECONDS * MICROSECONDS) as u32,
    )
}

/// converts a `i64` representing a `timestamp(us)` to [`NaiveDateTime`]
#[inline]
pub fn timestamp_us_to_datetime(v: i64) -> NaiveDateTime {
    NaiveDateTime::from_timestamp(
        // extract seconds from microseconds
        v / MICROSECONDS,
        // discard extracted seconds and convert microseconds to nanoseconds
        (v % MICROSECONDS * MILLISECONDS) as u32,
    )
}

/// converts a `i64` representing a `timestamp(ns)` to [`NaiveDateTime`]
#[inline]
pub fn timestamp_ns_to_datetime(v: i64) -> NaiveDateTime {
    NaiveDateTime::from_timestamp(
        // extract seconds from nanoseconds
        v / NANOSECONDS,
        // discard extracted seconds
        (v % NANOSECONDS) as u32,
    )
}

/// converts a `i64` representing a `duration(s)` to [`Duration`]
#[inline]
pub fn duration_s_to_duration(v: i64) -> Duration {
    Duration::seconds(v)
}

/// converts a `i64` representing a `duration(ms)` to [`Duration`]
#[inline]
pub fn duration_ms_to_duration(v: i64) -> Duration {
    Duration::milliseconds(v)
}

/// converts a `i64` representing a `duration(us)` to [`Duration`]
#[inline]
pub fn duration_us_to_duration(v: i64) -> Duration {
    Duration::microseconds(v)
}

/// converts a `i64` representing a `duration(ns)` to [`Duration`]
#[inline]
pub fn duration_ns_to_duration(v: i64) -> Duration {
    Duration::nanoseconds(v)
}
