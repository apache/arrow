use std::{collections::HashMap, convert::TryInto, error::Error, num::TryFromIntError};

use crate::{
    column::reader::ColumnReader,
    data_type::{Int64Type, Int96, Int96Type},
    errors::ParquetError,
    record::{
        reader::{I64Reader, I96Reader, MapReader},
        schemas::TimestampSchema,
        triplet::TypedTripletIter,
        types::{downcast, Value},
        Deserialize,
    },
    schema::types::{ColumnDescPtr, ColumnPath, Type},
};

const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;
const SECONDS_PER_DAY: i64 = 86_400;
const MILLIS_PER_SECOND: i64 = 1_000;
const MICROS_PER_MILLI: i64 = 1_000;
const NANOS_PER_MICRO: i64 = 1_000;

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct Timestamp(pub(super) Int96);
impl Timestamp {
    fn as_day_nanos(&self) -> (i64, i64) {
        let day = self.0.data()[2] as i64;
        let nanoseconds = ((self.0.data()[1] as i64) << 32) + self.0.data()[0] as i64;
        (day, nanoseconds)
    }

    fn as_millis(&self) -> Option<i64> {
        let day = self.0.data()[2] as i64;
        let nanoseconds = ((self.0.data()[1] as i64) << 32) + self.0.data()[0] as i64;
        let seconds = (day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY;
        Some(seconds * MILLIS_PER_SECOND + nanoseconds / NANOS_PER_MICRO / MICROS_PER_MILLI)
    }

    fn as_micros(&self) -> Option<i64> {
        let day = self.0.data()[2] as i64;
        let nanoseconds = ((self.0.data()[1] as i64) << 32) + self.0.data()[0] as i64;
        let seconds = (day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY;
        Some(seconds * MILLIS_PER_SECOND * MICROS_PER_MILLI + nanoseconds / NANOS_PER_MICRO)
    }

    fn as_nanos(&self) -> Option<i64> {
        let day = self.0.data()[2] as i64;
        let nanoseconds = ((self.0.data()[1] as i64) << 32) + self.0.data()[0] as i64;
        let seconds = (day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY;
        Some(seconds * MILLIS_PER_SECOND * MICROS_PER_MILLI * NANOS_PER_MICRO + nanoseconds)
    }
}

impl Deserialize for Timestamp {
    // existential type Reader: Reader<Item = Self>;
    type Reader = sum::Sum3<
        MapReader<I96Reader, fn(Int96) -> Result<Self, ParquetError>>,
        MapReader<I64Reader, fn(i64) -> Result<Self, ParquetError>>,
        MapReader<I64Reader, fn(i64) -> Result<Self, ParquetError>>,
    >;
    type Schema = TimestampSchema;

    fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
        Value::parse(schema).and_then(downcast)
    }

    fn reader(
        schema: &Self::Schema,
        path: &mut Vec<String>,
        curr_def_level: i16,
        curr_rep_level: i16,
        paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
        batch_size: usize,
    ) -> Self::Reader {
        let col_path = ColumnPath::new(path.to_vec());
        let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
        assert_eq!(
            (curr_def_level, curr_rep_level),
            (col_descr.max_def_level(), col_descr.max_rep_level())
        );
        match schema {
            TimestampSchema::Int96 => sum::Sum3::A(MapReader(
                I96Reader {
                    column: TypedTripletIter::<Int96Type>::new(
                        curr_def_level,
                        curr_rep_level,
                        batch_size,
                        col_reader,
                    ),
                },
                (|x| Ok(Timestamp(x))) as fn(_) -> _,
            )),
            TimestampSchema::Millis => sum::Sum3::B(MapReader(
                I64Reader {
                    column: TypedTripletIter::<Int64Type>::new(
                        curr_def_level,
                        curr_rep_level,
                        batch_size,
                        col_reader,
                    ),
                },
                (|millis| {
                    let day: i64 = ((JULIAN_DAY_OF_EPOCH * SECONDS_PER_DAY * MILLIS_PER_SECOND)
                        + millis)
                        / (SECONDS_PER_DAY * MILLIS_PER_SECOND);
                    let nanoseconds: i64 = (millis
                        - ((day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY * MILLIS_PER_SECOND))
                        * MICROS_PER_MILLI
                        * NANOS_PER_MICRO;

                    Ok(Timestamp(Int96::new(
                        (nanoseconds & 0xffff).try_into().unwrap(),
                        ((nanoseconds as u64) >> 32).try_into().unwrap(),
                        day.try_into().map_err(|err: TryFromIntError| {
                            ParquetError::General(err.description().to_owned())
                        })?,
                    )))
                }) as fn(_) -> _,
            )),
            TimestampSchema::Micros => sum::Sum3::C(MapReader(
                I64Reader {
                    column: TypedTripletIter::<Int64Type>::new(
                        curr_def_level,
                        curr_rep_level,
                        batch_size,
                        col_reader,
                    ),
                },
                (|micros| {
                    let day: i64 = ((JULIAN_DAY_OF_EPOCH
                        * SECONDS_PER_DAY
                        * MILLIS_PER_SECOND
                        * MICROS_PER_MILLI)
                        + micros)
                        / (SECONDS_PER_DAY * MILLIS_PER_SECOND * MICROS_PER_MILLI);
                    let nanoseconds: i64 = (micros
                        - ((day - JULIAN_DAY_OF_EPOCH)
                            * SECONDS_PER_DAY
                            * MILLIS_PER_SECOND
                            * MICROS_PER_MILLI))
                        * NANOS_PER_MICRO;

                    Ok(Timestamp(Int96::new(
                        (nanoseconds & 0xffff).try_into().unwrap(),
                        ((nanoseconds as u64) >> 32).try_into().unwrap(),
                        day.try_into().map_err(|err: TryFromIntError| {
                            ParquetError::General(err.description().to_owned())
                        })?,
                    )))
                }) as fn(_) -> _,
            )),
        }
    }
}

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct Date(pub(super) i32);
#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct Time(pub(super) i64);
