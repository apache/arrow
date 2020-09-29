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

//! Contains file reader API and provides methods to access file metadata, row group
//! readers to read individual column chunks, or access record iterator.

use std::{
  io::{Read, Seek, SeekFrom},
  rc::Rc,
};

use byteorder::{ByteOrder, LittleEndian};
use parquet_format::{ColumnOrder as TColumnOrder, FileMetaData as TFileMetaData};
use thrift::protocol::TCompactInputProtocol;

use crate::basic::ColumnOrder;

use crate::errors::{ParquetError, Result};
use crate::file::{metadata::*, reader::Length, FOOTER_SIZE, PARQUET_MAGIC};

use crate::schema::types::{self, SchemaDescriptor};

// Layout of Parquet file
// +---------------------------+---+-----+
// |      Rest of file         | B |  A  |
// +---------------------------+---+-----+
// where A: parquet footer, B: parquet metadata.
//
pub fn parse_metadata<R: Read + Seek + Length>(
  reader: &mut R,
) -> Result<ParquetMetaData> {
  let file_size = reader.len();
  if file_size < (FOOTER_SIZE as u64) {
    return Err(general_err!(
      "Invalid Parquet file. Size is smaller than footer"
    ));
  }
  let mut footer_buffer: [u8; FOOTER_SIZE] = [0; FOOTER_SIZE];
  reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
  reader.read_exact(&mut footer_buffer)?;
  if footer_buffer[4..] != PARQUET_MAGIC {
    return Err(general_err!("Invalid Parquet file. Corrupt footer"));
  }
  let metadata_len = LittleEndian::read_i32(&footer_buffer[0..4]) as i64;
  if metadata_len < 0 {
    return Err(general_err!(
      "Invalid Parquet file. Metadata length is less than zero ({})",
      metadata_len
    ));
  }
  let metadata_start: i64 = file_size as i64 - FOOTER_SIZE as i64 - metadata_len;
  if metadata_start < 0 {
    return Err(general_err!(
      "Invalid Parquet file. Metadata start is less than zero ({})",
      metadata_start
    ));
  }
  reader.seek(SeekFrom::Start(metadata_start as u64))?;
  let metadata_buf = reader.take(metadata_len as u64).into_inner();

  // TODO: row group filtering
  let mut prot = TCompactInputProtocol::new(metadata_buf);
  let t_file_metadata: TFileMetaData = TFileMetaData::read_from_in_protocol(&mut prot)
    .map_err(|e| ParquetError::General(format!("Could not parse metadata: {}", e)))?;
  let schema = types::from_thrift(&t_file_metadata.schema)?;
  let schema_descr = Rc::new(SchemaDescriptor::new(schema.clone()));
  let mut row_groups = Vec::new();
  for rg in t_file_metadata.row_groups {
    row_groups.push(RowGroupMetaData::from_thrift(schema_descr.clone(), rg)?);
  }
  let column_orders = parse_column_orders(t_file_metadata.column_orders, &schema_descr);

  let file_metadata = FileMetaData::new(
    t_file_metadata.version,
    t_file_metadata.num_rows,
    t_file_metadata.created_by,
    t_file_metadata.key_value_metadata,
    schema,
    schema_descr,
    column_orders,
  );
  Ok(ParquetMetaData::new(file_metadata, row_groups))
}

/// Parses column orders from Thrift definition.
/// If no column orders are defined, returns `None`.
fn parse_column_orders(
  t_column_orders: Option<Vec<TColumnOrder>>,
  schema_descr: &SchemaDescriptor,
) -> Option<Vec<ColumnOrder>> {
  match t_column_orders {
    Some(orders) => {
      // Should always be the case
      assert_eq!(
        orders.len(),
        schema_descr.num_columns(),
        "Column order length mismatch"
      );
      let mut res = Vec::new();
      for (i, column) in schema_descr.columns().iter().enumerate() {
        match orders[i] {
          TColumnOrder::TYPEORDER(_) => {
            let sort_order =
              ColumnOrder::get_sort_order(column.logical_type(), column.physical_type());
            res.push(ColumnOrder::TYPE_DEFINED_ORDER(sort_order));
          }
        }
      }
      Some(res)
    }
    None => None,
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  use crate::basic::SortOrder;
  use crate::basic::Type;
  use crate::schema::types::Type as SchemaType;
  use parquet_format::TypeDefinedOrder;

  #[test]
  fn test_file_reader_column_orders_parse() {
    // Define simple schema, we do not need to provide logical types.
    let mut fields = vec![
      Rc::new(
        SchemaType::primitive_type_builder("col1", Type::INT32)
          .build()
          .unwrap(),
      ),
      Rc::new(
        SchemaType::primitive_type_builder("col2", Type::FLOAT)
          .build()
          .unwrap(),
      ),
    ];
    let schema = SchemaType::group_type_builder("schema")
      .with_fields(&mut fields)
      .build()
      .unwrap();
    let schema_descr = SchemaDescriptor::new(Rc::new(schema));

    let t_column_orders = Some(vec![
      TColumnOrder::TYPEORDER(TypeDefinedOrder::new()),
      TColumnOrder::TYPEORDER(TypeDefinedOrder::new()),
    ]);

    assert_eq!(
      parse_column_orders(t_column_orders, &schema_descr),
      Some(vec![
        ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED),
        ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED)
      ])
    );

    // Test when no column orders are defined.
    assert_eq!(parse_column_orders(None, &schema_descr), None);
  }

  #[test]
  #[should_panic(expected = "Column order length mismatch")]
  fn test_file_reader_column_orders_len_mismatch() {
    let schema = SchemaType::group_type_builder("schema").build().unwrap();
    let schema_descr = SchemaDescriptor::new(Rc::new(schema));

    let t_column_orders = Some(vec![TColumnOrder::TYPEORDER(TypeDefinedOrder::new())]);

    parse_column_orders(t_column_orders, &schema_descr);
  }
}
