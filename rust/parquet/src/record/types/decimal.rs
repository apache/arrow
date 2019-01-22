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

use std::collections::HashMap;

use crate::{
    column::reader::ColumnReader,
    data_type::{ByteArrayType, Decimal, FixedLenByteArrayType, Int32Type, Int64Type},
    errors::ParquetError,
    record::{
        reader::{
            ByteArrayReader, FixedLenByteArrayReader, I32Reader, I64Reader, MapReader, Reader,
        },
        schemas::DecimalSchema,
        triplet::TypedTripletIter,
        types::{downcast, Value},
        Deserialize,
    },
    schema::types::{ColumnDescPtr, ColumnPath, Type},
};

impl Deserialize for Decimal {
    // existential type Reader: Reader<Item = Self>;
    type Reader = sum::Sum3<
        MapReader<I32Reader, fn(i32) -> Result<Self, ParquetError>>,
        MapReader<I64Reader, fn(i64) -> Result<Self, ParquetError>>,
        MapReader<ByteArrayReader, fn(Vec<u8>) -> Result<Self, ParquetError>>,
    >;
    type Schema = DecimalSchema;

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
        unimplemented!()
        // match *schema {
        //     DecimalSchema::Int32 { precision, scale } => sum::Sum3::A(MapReader(
        //         I32Reader {
        //             column: TypedTripletIter::<Int32Type>::new(
        //                 curr_def_level,
        //                 curr_rep_level,
        //                 batch_size,
        //                 col_reader,
        //             ),
        //         },
        //         move |x| Ok(Decimal::from_i32(x, precision as i32, scale as i32)),
        //     )),
        //     DecimalSchema::Int64 { precision, scale } => sum::Sum3::B(MapReader(
        //         I64Reader {
        //             column: TypedTripletIter::<Int64Type>::new(
        //                 curr_def_level,
        //                 curr_rep_level,
        //                 batch_size,
        //                 col_reader,
        //             ),
        //         },
        //         move |x| Ok(Decimal::from_i64(x, precision as i32, scale as i32)),
        //     )),
        //     DecimalSchema::Array { precision, scale } => sum::Sum3::C(MapReader(
        //         ByteArrayReader {
        //             column: TypedTripletIter::<ByteArrayType>::new(
        //                 curr_def_level,
        //                 curr_rep_level,
        //                 batch_size,
        //                 col_reader,
        //             ),
        //         },
        //         move |x| {
        //             Ok(Decimal::from_bytes(
        //                 unimplemented!(),
        //                 precision as i32,
        //                 scale as i32,
        //             ))
        //         },
        //     )),
        // }
    }
}
