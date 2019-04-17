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

#![recursion_limit = "128"]

extern crate proc_macro;
extern crate proc_macro2;
extern crate syn;
#[macro_use]
extern crate quote;

extern crate parquet;

use syn::{parse_macro_input, Data, DataStruct, DeriveInput};

mod parquet_field;

/// Derive flat, simple RecordWriter implementations. Works by parsing
/// a struct tagged with `#[derive(ParquetRecordWriter)]` and emitting
/// the correct writing code for each field of the struct. Column writers
/// are generated in the order they are defined.
///
/// It is up to the programmer to keep the order of the struct
/// fields lined up with the schema.
///
/// Example:
///
/// ```ignore
/// use parquet;
/// use parquet::record::RecordWriter;
//
/// #[derive(ParquetRecordWriter)]
/// struct ACompleteRecord<'a> {
///   pub a_bool: bool,
///   pub a_str: &'a str,
/// }
/// ```
///
#[proc_macro_derive(ParquetRecordWriter)]
pub fn parquet_record_writer(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input: DeriveInput = parse_macro_input!(input as DeriveInput);
    let fields = match input.data {
        Data::Struct(DataStruct { fields, .. }) => fields,
        Data::Enum(_) => unimplemented!("don't support enum"),
        Data::Union(_) => unimplemented!("don't support union"),
    };

    let field_infos: Vec<_> = fields
        .iter()
        .map(|f: &syn::Field| parquet_field::Field::from(f))
        .collect();

    let writer_snippets: Vec<proc_macro2::TokenStream> =
        field_infos.iter().map(|x| x.writer_snippet()).collect();

    let derived_for = input.ident;
    let generics = input.generics;

    (quote! {
    impl#generics RecordWriter<#derived_for#generics> for &[#derived_for#generics] {
      fn write_to_row_group(&self, row_group_writer: &mut Box<parquet::file::writer::RowGroupWriter>) {
        let mut row_group_writer = row_group_writer;
        let records = &self; // Used by all the writer snippets to be more clear

        #(
          {
              let mut column_writer = row_group_writer.next_column().unwrap().unwrap();
              #writer_snippets
              row_group_writer.close_column(column_writer).unwrap();
          }
        );*
      }
    }
  }).into()
}
