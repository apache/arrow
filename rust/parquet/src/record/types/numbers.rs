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

use std::{collections::HashMap, marker::PhantomData};

use crate::{
    basic::Repetition,
    column::reader::ColumnReader,
    data_type::{BoolType, DoubleType, FloatType, Int32Type, Int64Type},
    errors::Result,
    record::{
        reader::{
            BoolReader, F32Reader, F64Reader, I32Reader, I64Reader, MapReader,
            TryIntoReader,
        },
        schemas::{
            BoolSchema, F32Schema, F64Schema, I16Schema, I32Schema, I64Schema, I8Schema,
            U16Schema, U32Schema, U64Schema, U8Schema,
        },
        triplet::TypedTripletIter,
        types::{downcast, Value},
        Reader, Record,
    },
    schema::types::{ColumnPath, Type},
};

impl Record for bool {
    type Reader = BoolReader;
    type Schema = BoolSchema;

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
        let col_path = ColumnPath::new(path.to_vec());
        let col_reader = paths.remove(&col_path).unwrap();
        BoolReader {
            column: TypedTripletIter::<BoolType>::new(
                def_level, rep_level, col_reader, batch_size,
            ),
        }
    }
}

impl Record for i8 {
    type Reader = TryIntoReader<I32Reader, i8>;
    type Schema = I8Schema;

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
        TryIntoReader(
            i32::reader(&I32Schema, path, def_level, rep_level, paths, batch_size),
            PhantomData,
        )
    }
}
impl Record for u8 {
    type Reader = TryIntoReader<I32Reader, u8>;
    type Schema = U8Schema;

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
        TryIntoReader(
            i32::reader(&I32Schema, path, def_level, rep_level, paths, batch_size),
            PhantomData,
        )
    }
}

impl Record for i16 {
    type Reader = TryIntoReader<I32Reader, i16>;
    type Schema = I16Schema;

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
        TryIntoReader(
            i32::reader(&I32Schema, path, def_level, rep_level, paths, batch_size),
            PhantomData,
        )
    }
}
impl Record for u16 {
    type Reader = TryIntoReader<I32Reader, u16>;
    type Schema = U16Schema;

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
        TryIntoReader(
            i32::reader(&I32Schema, path, def_level, rep_level, paths, batch_size),
            PhantomData,
        )
    }
}

impl Record for i32 {
    type Reader = I32Reader;
    type Schema = I32Schema;

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
        let col_path = ColumnPath::new(path.to_vec());
        let col_reader = paths.remove(&col_path).unwrap();
        I32Reader {
            column: TypedTripletIter::<Int32Type>::new(
                def_level, rep_level, col_reader, batch_size,
            ),
        }
    }
}
impl Record for u32 {
    type Reader = impl Reader<Item = Self>;
    type Schema = U32Schema;

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
            |x| Ok(x as u32),
        )
    }
}

impl Record for i64 {
    type Reader = I64Reader;
    type Schema = I64Schema;

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
        let col_path = ColumnPath::new(path.to_vec());
        let col_reader = paths.remove(&col_path).unwrap();
        I64Reader {
            column: TypedTripletIter::<Int64Type>::new(
                def_level, rep_level, col_reader, batch_size,
            ),
        }
    }
}
impl Record for u64 {
    type Reader = impl Reader<Item = Self>;
    type Schema = U64Schema;

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
            i64::reader(&I64Schema, path, def_level, rep_level, paths, batch_size),
            |x| Ok(x as u64),
        )
    }
}

impl Record for f32 {
    type Reader = F32Reader;
    type Schema = F32Schema;

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
        let col_path = ColumnPath::new(path.to_vec());
        let col_reader = paths.remove(&col_path).unwrap();
        F32Reader {
            column: TypedTripletIter::<FloatType>::new(
                def_level, rep_level, col_reader, batch_size,
            ),
        }
    }
}
impl Record for f64 {
    type Reader = F64Reader;
    type Schema = F64Schema;

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
        let col_path = ColumnPath::new(path.to_vec());
        let col_reader = paths.remove(&col_path).unwrap();
        F64Reader {
            column: TypedTripletIter::<DoubleType>::new(
                def_level, rep_level, col_reader, batch_size,
            ),
        }
    }
}
