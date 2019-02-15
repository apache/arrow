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

//! Implement [`Record`] for tuples up to length 32.

use std::{
    collections::HashMap,
    fmt::{self, Debug},
    intrinsics::unlikely,
};

use crate::{
    basic::Repetition,
    column::reader::ColumnReader,
    errors::{ParquetError, Result},
    record::{
        display::DisplaySchemaGroup,
        reader::TupleReader,
        schemas::{TupleSchema, ValueSchema},
        types::{group::Group, Downcast, Value},
        Reader, Record, Schema,
    },
    schema::types::{ColumnPath, Type},
};

/// Macro to implement [`Reader`] on tuples up to length 32.
macro_rules! impl_parquet_record_tuple {
    ($len:tt $($t:ident $i:tt)*) => (
        // Tuples correspond to Parquet groups with an equal number of fields with corresponding types.
        impl<$($t,)*> Reader for TupleReader<($($t,)*)> where $($t: Reader,)* {
            type Item = ($($t::Item,)*);

            #[allow(unused_variables, non_snake_case)]
            fn read(&mut self, def_level: i16, rep_level: i16) -> Result<Self::Item> {
                $(
                    let $t = (self.0).$i.read(def_level, rep_level);
                )*
                if unsafe{unlikely($($t.is_err() ||)* false)} {
                    $($t?;)*
                    unreachable!()
                }
                Ok((
                    $($t.unwrap(),)*
                ))
            }
            fn advance_columns(&mut self) -> Result<()> {
                #[allow(unused_mut)]
                let mut res = Ok(());
                $(
                    res = res.and((self.0).$i.advance_columns());
                )*
                res
            }
            #[inline]
            fn has_next(&self) -> bool {
                $(if true { (self.0).$i.has_next() } else)*
                {
                    true
                }
            }
            #[inline]
            fn current_def_level(&self) -> i16 {
                $(if true { (self.0).$i.current_def_level() } else)*
                {
                    panic!("Current definition level: empty group reader")
                }
            }
            #[inline]
            fn current_rep_level(&self) -> i16 {
                $(if true { (self.0).$i.current_rep_level() } else)*
                {
                    panic!("Current repetition level: empty group reader")
                }
            }
        }
        impl<$($t,)*> Default for TupleSchema<($((String,$t,),)*)> where $($t: Default,)* {
            fn default() -> Self {
                Self(($((format!("field_{}", $i), Default::default()),)*))
            }
        }
        impl<$($t,)*> Debug for TupleSchema<($((String,$t,),)*)> where $($t: Debug,)* {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.debug_tuple("TupleSchema")
                    $(.field(&(self.0).$i))*
                    .finish()
            }
        }
        impl<$($t,)*> Schema for TupleSchema<($((String,$t,),)*)> where $($t: Schema,)* {
            #[allow(unused_variables)]
            fn fmt(self_: Option<&Self>, r: Option<Repetition>, name: Option<&str>, f: &mut fmt::Formatter) -> fmt::Result {
                let mut printer = DisplaySchemaGroup::new(r, name, None, f);
                $(
                    printer.field(self_.map(|self_|&*(self_.0).$i.0), self_.map(|self_|&(self_.0).$i.1));
                )*
                printer.finish()
            }
        }
        impl<$($t,)*> Record for ($($t,)*) where $($t: Record,)* {
            type Schema = TupleSchema<($((String,$t::Schema,),)*)>;
            type Reader = TupleReader<($($t::Reader,)*)>;

            fn parse(schema: &Type, repetition: Option<Repetition>) -> Result<(String, Self::Schema)> {
                if schema.is_group() && repetition == Some(Repetition::REQUIRED) {
                    let mut fields = schema.get_fields().iter();
                    let schema_ = TupleSchema(($(fields.next().ok_or_else(|| ParquetError::General(String::from("Group missing field"))).and_then(|x|$t::parse(&**x, Some(x.get_basic_info().repetition())))?,)*));
                    if fields.next().is_none() {
                        return Ok((schema.name().to_owned(), schema_))
                    }
                }
                Err(ParquetError::General(format!("Can't parse Tuple {:?}", schema)))
            }
            #[allow(unused_variables)]
            fn reader(schema: &Self::Schema, path: &mut Vec<String>, def_level: i16, rep_level: i16, paths: &mut HashMap<ColumnPath, ColumnReader>, batch_size: usize) -> Self::Reader {
                $(
                    path.push((schema.0).$i.0.to_owned());
                    #[allow(non_snake_case)]
                    let $t = <$t as Record>::reader(&(schema.0).$i.1, path, def_level, rep_level, paths, batch_size);
                    path.pop().unwrap();
                )*
                TupleReader(($($t,)*))
            }
        }
        impl<$($t,)*> Downcast<($($t,)*)> for Value where Value: $(Downcast<$t> +)* {
            fn downcast(self) -> Result<($($t,)*)> {
                #[allow(unused_mut,unused_variables)]
                let mut fields = self.into_group()?.0.into_iter();
                if fields.len() != $len {
                    return Err(ParquetError::General(format!("Can't downcast group of length {} to tuple of length {}", fields.len(), $len)));
                }
                Ok(($({$i;fields.next().unwrap().downcast()?},)*))
            }
        }
        impl<$($t,)*> Downcast<($($t,)*)> for Group where Value: $(Downcast<$t> +)* {
            fn downcast(self) -> Result<($($t,)*)> {
                #[allow(unused_mut,unused_variables)]
                let mut fields = self.0.into_iter();
                if fields.len() != $len {
                    return Err(ParquetError::General(format!("Can't downcast group of length {} to tuple of length {}", fields.len(), $len)));
                }
                Ok(($({$i;fields.next().unwrap().downcast()?},)*))
            }
        }
        impl<$($t,)*> PartialEq<($($t,)*)> for Value where Value: $(PartialEq<$t> +)* {
            #[allow(unused_variables)]
            fn eq(&self, other: &($($t,)*)) -> bool {
                self.is_group() $(&& self.as_group().unwrap()[$i] == other.$i)*
            }
        }
        impl<$($t,)*> PartialEq<($($t,)*)> for Group where Value: $(PartialEq<$t> +)* {
            #[allow(unused_variables)]
            fn eq(&self, other: &($($t,)*)) -> bool {
                $(self[$i] == other.$i && )* true
            }
        }
        impl<$($t,)*> Downcast<TupleSchema<($((String,$t,),)*)>> for ValueSchema where ValueSchema: $(Downcast<$t> +)* {
            fn downcast(self) -> Result<TupleSchema<($((String,$t,),)*)>> {
                let group = self.into_group()?;
                #[allow(unused_mut,unused_variables)]
                let mut fields = group.0.into_iter();
                #[allow(unused_mut,unused_variables)]
                let mut names = group.1.into_iter().map(|(name,_index)|name);
                Ok(TupleSchema(($({let _ = $i;(names.next().unwrap(),fields.next().unwrap().downcast()?)},)*)))
            }
        }
    );
}

impl_parquet_record_tuple!(0);
impl_parquet_record_tuple!(1 A 0);
impl_parquet_record_tuple!(2 A 0 B 1);
impl_parquet_record_tuple!(3 A 0 B 1 C 2);
impl_parquet_record_tuple!(4 A 0 B 1 C 2 D 3);
impl_parquet_record_tuple!(5 A 0 B 1 C 2 D 3 E 4);
impl_parquet_record_tuple!(6 A 0 B 1 C 2 D 3 E 4 F 5);
impl_parquet_record_tuple!(7 A 0 B 1 C 2 D 3 E 4 F 5 G 6);
impl_parquet_record_tuple!(8 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7);
impl_parquet_record_tuple!(9 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8);
impl_parquet_record_tuple!(10 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9);
impl_parquet_record_tuple!(11 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10);
impl_parquet_record_tuple!(12 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11);
impl_parquet_record_tuple!(13 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12);
impl_parquet_record_tuple!(14 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13);
impl_parquet_record_tuple!(15 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14);
impl_parquet_record_tuple!(16 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15);
impl_parquet_record_tuple!(17 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16);
impl_parquet_record_tuple!(18 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17);
impl_parquet_record_tuple!(19 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18);
impl_parquet_record_tuple!(20 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19);
impl_parquet_record_tuple!(21 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20);
impl_parquet_record_tuple!(22 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21);
impl_parquet_record_tuple!(23 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22);
impl_parquet_record_tuple!(24 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23);
impl_parquet_record_tuple!(25 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24);
impl_parquet_record_tuple!(26 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25);
impl_parquet_record_tuple!(27 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26);
impl_parquet_record_tuple!(28 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27);
impl_parquet_record_tuple!(29 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28);
impl_parquet_record_tuple!(30 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29);
impl_parquet_record_tuple!(31 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29 AE 30);
impl_parquet_record_tuple!(32 A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29 AE 30 AF 31);
