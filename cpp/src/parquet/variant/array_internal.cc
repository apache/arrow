// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "parquet/variant/array_internal.h"

#include "arrow/array.h"  // IWYU pragma: keep
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "parquet/exception.h"

namespace parquet::variant::internal {

using ::arrow::Array;
using ::arrow::internal::checked_cast;

std::shared_ptr<Array> ValuesArray(const Array& array) {
  switch (array.type_id()) {
    case ::arrow::Type::LIST_VIEW:
      return checked_cast<const ::arrow::ListViewArray&>(array).values();
    case ::arrow::Type::LARGE_LIST_VIEW:
      return checked_cast<const ::arrow::LargeListViewArray&>(array).values();
    case ::arrow::Type::LIST:
      return checked_cast<const ::arrow::ListArray&>(array).values();
    case ::arrow::Type::LARGE_LIST:
      return checked_cast<const ::arrow::LargeListArray&>(array).values();
    case ::arrow::Type::FIXED_SIZE_LIST:
      return checked_cast<const ::arrow::FixedSizeListArray&>(array).values();
    case ::arrow::Type::MAP:
      return checked_cast<const ::arrow::MapArray&>(array).values();
    default:
      throw ParquetException("Expected list or map storage, got ",
                             array.type()->ToString());
  }
}

template <typename ArrayType>
std::pair<int64_t, int64_t> ValueOffsetAndLength(const Array& array, int64_t row) {
  const auto& typed_array = checked_cast<const ArrayType&>(array);
  return {typed_array.value_offset(row), typed_array.value_length(row)};
}

std::pair<int64_t, int64_t> ValuesRangeAt(const Array& array, int64_t row) {
  switch (array.type_id()) {
    case ::arrow::Type::LIST:
      return ValueOffsetAndLength<::arrow::ListArray>(array, row);
    case ::arrow::Type::LARGE_LIST:
      return ValueOffsetAndLength<::arrow::LargeListArray>(array, row);
    case ::arrow::Type::LIST_VIEW:
      return ValueOffsetAndLength<::arrow::ListViewArray>(array, row);
    case ::arrow::Type::LARGE_LIST_VIEW:
      return ValueOffsetAndLength<::arrow::LargeListViewArray>(array, row);
    case ::arrow::Type::MAP:
      return ValueOffsetAndLength<::arrow::MapArray>(array, row);
    case ::arrow::Type::FIXED_SIZE_LIST:
      return ValueOffsetAndLength<::arrow::FixedSizeListArray>(array, row);
    default:
      throw ParquetException("Expected list or map storage, got ",
                             array.type()->ToString());
  }
}

std::string_view BinaryFieldView(const Array& array, int64_t row) {
  switch (array.type_id()) {
    case ::arrow::Type::BINARY:
      return checked_cast<const ::arrow::BinaryArray&>(array).GetView(row);
    case ::arrow::Type::LARGE_BINARY:
      return checked_cast<const ::arrow::LargeBinaryArray&>(array).GetView(row);
    case ::arrow::Type::BINARY_VIEW:
      return checked_cast<const ::arrow::BinaryViewArray&>(array).GetView(row);
    default:
      throw ParquetInvalidOrCorruptedFileException("Expected binary Variant field, got ",
                                                   array.type()->ToString());
  }
}

}  // namespace parquet::variant::internal
