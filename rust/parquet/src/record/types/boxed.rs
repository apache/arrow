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
    basic::Repetition,
    column::reader::ColumnReader,
    data_type::{ByteArrayType, Decimal, FixedLenByteArrayType, Int32Type, Int64Type},
    errors::ParquetError,
    record::{
        reader::BoxReader,
        schemas::BoxSchema,
        triplet::TypedTripletIter,
        types::{downcast, Value},
        Deserialize,
    },
    schema::types::{ColumnPath, Type},
};

default impl<T: ?Sized> Deserialize for Box<T>
where
    T: Deserialize,
{
    type Reader = BoxReader<T::Reader>;
    type Schema = BoxSchema<T::Schema>;

    fn parse(
        schema: &Type,
        repetition: Option<Repetition>,
    ) -> Result<(String, Self::Schema), ParquetError> {
        T::parse(schema, repetition)
            .map(|(name, schema)| (name, unsafe { known_type(BoxSchema(schema)) }))
    }

    fn reader(
        schema: &Self::Schema,
        path: &mut Vec<String>,
        def_level: i16,
        rep_level: i16,
        paths: &mut HashMap<ColumnPath, ColumnReader>,
        batch_size: usize,
    ) -> Self::Reader {
        let schema =
            unsafe { known_type::<&Self::Schema, &BoxSchema<T::Schema>>(schema) };
        let ret = BoxReader(T::reader(
            &schema.0, path, def_level, rep_level, paths, batch_size,
        ));
        unsafe { known_type(ret) }
    }
}

/// This is used until specialization can handle groups of items together
unsafe fn known_type<A, B>(a: A) -> B {
    use std::mem;
    assert_eq!(
        (mem::size_of::<A>(), mem::align_of::<A>()),
        (mem::size_of::<B>(), mem::align_of::<B>())
    );
    let ret = mem::transmute_copy(&a);
    mem::forget(a);
    ret
}
