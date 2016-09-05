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

#ifndef PARQUET_SCAN_ALL_H
#define PARQUET_SCAN_ALL_H

#include "parquet/column/reader.h"

template <typename RType>
int64_t ScanAll(int32_t batch_size, int16_t* def_levels, int16_t* rep_levels,
    uint8_t* values, int64_t* values_buffered, parquet::ColumnReader* reader) {
  typedef typename RType::T Type;
  auto typed_reader = static_cast<RType*>(reader);
  auto vals = reinterpret_cast<Type*>(&values[0]);
  return typed_reader->ReadBatch(
      batch_size, def_levels, rep_levels, vals, values_buffered);
}

int64_t ScanAllValues(int32_t batch_size, int16_t* def_levels, int16_t* rep_levels,
    uint8_t* values, int64_t* values_buffered, parquet::ColumnReader* reader) {
  switch (reader->type()) {
    case parquet::Type::BOOLEAN:
      return ScanAll<parquet::BoolReader>(
          batch_size, def_levels, rep_levels, values, values_buffered, reader);
    case parquet::Type::INT32:
      return ScanAll<parquet::Int32Reader>(
          batch_size, def_levels, rep_levels, values, values_buffered, reader);
    case parquet::Type::INT64:
      return ScanAll<parquet::Int64Reader>(
          batch_size, def_levels, rep_levels, values, values_buffered, reader);
    case parquet::Type::INT96:
      return ScanAll<parquet::Int96Reader>(
          batch_size, def_levels, rep_levels, values, values_buffered, reader);
    case parquet::Type::FLOAT:
      return ScanAll<parquet::FloatReader>(
          batch_size, def_levels, rep_levels, values, values_buffered, reader);
    case parquet::Type::DOUBLE:
      return ScanAll<parquet::DoubleReader>(
          batch_size, def_levels, rep_levels, values, values_buffered, reader);
    case parquet::Type::BYTE_ARRAY:
      return ScanAll<parquet::ByteArrayReader>(
          batch_size, def_levels, rep_levels, values, values_buffered, reader);
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      return ScanAll<parquet::FixedLenByteArrayReader>(
          batch_size, def_levels, rep_levels, values, values_buffered, reader);
    default:
      parquet::ParquetException::NYI("type reader not implemented");
  }
  // Unreachable code, but supress compiler warning
  return 0;
}

#endif  // PARQUET_SCAN_ALL_H
