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

use std::{
    collections::HashMap,
    fmt::{self, Debug},
    marker::PhantomData,
    vec,
};

use crate::{
    basic::Repetition,
    column::reader::ColumnReader,
    errors::ParquetError,
    record::{
        reader::{Reader, TupleReader},
        schemas::{TupleSchema, ValueSchema},
        types::{group::Group, Downcast, Value},
        Deserialize, DisplaySchema,
    },
    schema::types::{ColumnDescPtr, ColumnPath, Type},
};

macro_rules! impl_parquet_deserialize_tuple {
    ($($t:ident $i:tt)*) => (
        impl<$($t,)*> Reader for TupleReader<($($t,)*)> where $($t: Reader,)* {
            type Item = ($($t::Item,)*);

            fn read(&mut self, def_level: i16, rep_level: i16) -> Result<Self::Item, ParquetError> {
                let ret = (
                    $((self.0).$i.read(def_level, rep_level),)*
                );
                Ok((
                    $(ret.$i?,)*
                ))
            }
            fn advance_columns(&mut self) -> Result<(), ParquetError> {
                let mut res = Ok(());
                $(
                    res = res.and((self.0).$i.advance_columns());
                )*
                res
            }
            fn has_next(&self) -> bool {
                // $((self.0).$i.has_next() &&)* true
                $(if true { (self.0).$i.has_next() } else)*
                {
                    true
                }
            }
            fn current_def_level(&self) -> i16 {
                $(if true { (self.0).$i.current_def_level() } else)*
                {
                    panic!("Current definition level: empty group reader")
                }
            }
            fn current_rep_level(&self) -> i16 {
                $(if true { (self.0).$i.current_rep_level() } else)*
                {
                    panic!("Current repetition level: empty group reader")
                }
            }
        }
        // impl<$($t,)*> str::FromStr for RootSchema<($($t,)*),TupleSchema<($((String,$t::Schema,),)*)>> where $($t: Deserialize,)* {
        //   type Err = ParquetError;

        //   fn from_str(s: &str) -> Result<Self, Self::Err> {
        //     parse_message_type(s).and_then(|x|<Root<($($t,)*)> as Deserialize>::parse(&x).map_err(|err| {
        //       // let x: Type = <Root<($($t,)*)> as Deserialize>::render("", &<Root<($($t,)*)> as Deserialize>::placeholder());
        //       let a = Vec::new();
        //       // print_schema(&mut a, &x);
        //       ParquetError::General(format!(
        //         "Types don't match schema.\nSchema is:\n{}\nBut types require:\n{}\nError: {}",
        //         s,
        //         String::from_utf8(a).unwrap(),
        //         err
        //       ))
        //     })).map(|x|x.1)
        //   }
        // }
        impl<$($t,)*> Debug for TupleSchema<($((String,$t,),)*)> where $($t: Debug,)* {
            fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
                f.debug_tuple("TupleSchema")
                    $(.field(&(self.0).$i))*
                    .finish()
            }
        }
        impl<$($t,)*> DisplaySchema for TupleSchema<($((String,$t,),)*)> where $($t: DisplaySchema,)* {
            fn fmt(&self, r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
                 f.write_fmt(format_args!("{} group {} {{\n", r, name))?;
                $(
                    f.write_str("    ")?;
                    DisplaySchema::fmt(&(self.0).$i.1, Repetition::REQUIRED, stringify!(field_$i), f)?;
                    f.write_str("\n")?;
                )*
                f.write_str("}}")
            }
            fn fmt_type(r: Repetition, name: &str, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
                f.write_fmt(format_args!("{} group {} {{\n", r, name))?;
                $(
                    f.write_str("    ")?;
                    $t::fmt_type(Repetition::REQUIRED, stringify!(field_$i), f)?;
                    f.write_str("\n")?;
                )*
                f.write_str("}}")
            }
        }
        impl<$($t,)*> Deserialize for ($($t,)*) where $($t: Deserialize,)* {
            type Schema = TupleSchema<($((String,$t::Schema,),)*)>;
            type Reader = TupleReader<($($t::Reader,)*)>;

            fn parse(schema: &Type, repetition: Option<Repetition>) -> Result<(String, Self::Schema), ParquetError> {
                if schema.is_group() && repetition == Some(Repetition::REQUIRED) {
                    let mut fields = schema.get_fields().iter();
                    let schema_ = TupleSchema(($(fields.next().ok_or(ParquetError::General(String::from("Group missing field"))).and_then(|x|$t::parse(&**x, Some(x.get_basic_info().repetition())))?,)*));
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
                    let $t = <$t as Deserialize>::reader(&(schema.0).$i.1, path, def_level, rep_level, paths, batch_size);
                    path.pop().unwrap();
                )*
                TupleReader(($($t,)*))
            }
        }
        impl<$($t,)*> Downcast<($($t,)*)> for Value where Value: $(Downcast<$t> +)* {
            fn downcast(self) -> Result<($($t,)*),ParquetError> {
                #[allow(unused_mut,unused_variables)]
                let mut fields = self.into_group()?.0.into_iter();
                Ok(($({$i;fields.next().unwrap().downcast()?},)*))
            }
        }
        impl<$($t,)*> Downcast<($($t,)*)> for Group where Value: $(Downcast<$t> +)* {
            fn downcast(self) -> Result<($($t,)*),ParquetError> {
                #[allow(unused_mut,unused_variables)]
                let mut fields = self.0.into_iter();
                Ok(($({$i;fields.next().unwrap().downcast()?},)*))
            }
        }
        impl<$($t,)*> Downcast<TupleSchema<($((String,$t,),)*)>> for ValueSchema where ValueSchema: $(Downcast<$t> +)* {
            fn downcast(self) -> Result<TupleSchema<($((String,$t,),)*)>,ParquetError> {
                let group = self.into_group()?;
                #[allow(unused_mut,unused_variables)]
                let mut fields = group.0.into_iter();
                let mut names = vec![None; group.1.len()];
                for (name,&index) in group.1.iter() {
                    names[index].replace(name.to_owned());
                }
                #[allow(unused_mut,unused_variables)]
                let mut names = names.into_iter().map(Option::unwrap);
                Ok(TupleSchema(($({let _ = $i;(names.next().unwrap(),fields.next().unwrap().downcast()?)},)*)))
            }
        }
    );
}

impl_parquet_deserialize_tuple!();
impl_parquet_deserialize_tuple!(A 0);
impl_parquet_deserialize_tuple!(A 0 B 1);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29 AE 30);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29 AE 30 AF 31);
impl_parquet_deserialize_tuple!(A 0 B 1 C 2 D 3 E 4 F 5 G 6 H 7 I 8 J 9 K 10 L 11 M 12 N 13 O 14 P 15 Q 16 R 17 S 18 T 19 U 20 V 21 W 22 X 23 Y 24 Z 25 AA 26 AB 27 AC 28 AD 29 AE 30 AF 31 AG 32);
