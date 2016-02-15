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

#include "parquet/column/scanner.h"

#include <cstdint>
#include <memory>

#include "parquet/column/reader.h"

namespace parquet_cpp {

std::shared_ptr<Scanner> Scanner::Make(std::shared_ptr<ColumnReader> col_reader,
    int64_t batch_size) {
  switch (col_reader->type()) {
    case Type::BOOLEAN:
      return std::make_shared<BoolScanner>(col_reader, batch_size);
    case Type::INT32:
      return std::make_shared<Int32Scanner>(col_reader, batch_size);
    case Type::INT64:
      return std::make_shared<Int64Scanner>(col_reader, batch_size);
    case Type::INT96:
      return std::make_shared<Int96Scanner>(col_reader, batch_size);
    case Type::FLOAT:
      return std::make_shared<FloatScanner>(col_reader, batch_size);
    case Type::DOUBLE:
      return std::make_shared<DoubleScanner>(col_reader, batch_size);
    case Type::BYTE_ARRAY:
      return std::make_shared<ByteArrayScanner>(col_reader, batch_size);
    case Type::FIXED_LEN_BYTE_ARRAY:
      return std::make_shared<FixedLenByteArrayScanner>(col_reader, batch_size);
    default:
      ParquetException::NYI("type reader not implemented");
  }
  // Unreachable code, but supress compiler warning
  return std::shared_ptr<Scanner>(nullptr);
}

} // namespace parquet_cpp
