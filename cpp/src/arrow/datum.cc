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

#include "arrow/datum.h"

#include <cstddef>
#include <memory>
#include <sstream>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/util.h"
#include "arrow/chunked_array.h"
#include "arrow/record_batch.h"
#include "arrow/scalar.h"
#include "arrow/table.h"
#include "arrow/util/byte_size.h"
#include "arrow/util/logging.h"
#include "arrow/util/memory.h"

namespace arrow {

Datum::Datum(const Array& value) : Datum(value.data()) {}

Datum::Datum(const std::shared_ptr<Array>& value)
    : Datum(value ? value->data() : NULLPTR) {}

Datum::Datum(std::shared_ptr<ChunkedArray> value) : value(std::move(value)) {}
Datum::Datum(std::shared_ptr<RecordBatch> value) : value(std::move(value)) {}
Datum::Datum(std::shared_ptr<Table> value) : value(std::move(value)) {}

Datum::Datum(bool value) : value(std::make_shared<BooleanScalar>(value)) {}
Datum::Datum(int8_t value) : value(std::make_shared<Int8Scalar>(value)) {}
Datum::Datum(uint8_t value) : value(std::make_shared<UInt8Scalar>(value)) {}
Datum::Datum(int16_t value) : value(std::make_shared<Int16Scalar>(value)) {}
Datum::Datum(uint16_t value) : value(std::make_shared<UInt16Scalar>(value)) {}
Datum::Datum(int32_t value) : value(std::make_shared<Int32Scalar>(value)) {}
Datum::Datum(uint32_t value) : value(std::make_shared<UInt32Scalar>(value)) {}
Datum::Datum(int64_t value) : value(std::make_shared<Int64Scalar>(value)) {}
Datum::Datum(uint64_t value) : value(std::make_shared<UInt64Scalar>(value)) {}
Datum::Datum(float value) : value(std::make_shared<FloatScalar>(value)) {}
Datum::Datum(double value) : value(std::make_shared<DoubleScalar>(value)) {}
Datum::Datum(std::string value)
    : value(std::make_shared<StringScalar>(std::move(value))) {}
Datum::Datum(const char* value) : value(std::make_shared<StringScalar>(value)) {}

Datum::Datum(const ChunkedArray& value)
    : value(std::make_shared<ChunkedArray>(value.chunks(), value.type())) {}

Datum::Datum(const Table& value)
    : value(Table::Make(value.schema(), value.columns(), value.num_rows())) {}

Datum::Datum(const RecordBatch& value)
    : value(RecordBatch::Make(value.schema(), value.num_rows(), value.columns())) {}

std::shared_ptr<Array> Datum::make_array() const {
  DCHECK_EQ(Datum::ARRAY, this->kind());
  return MakeArray(std::get<std::shared_ptr<ArrayData>>(this->value));
}

const std::shared_ptr<DataType>& Datum::type() const {
  if (this->kind() == Datum::ARRAY) {
    return std::get<std::shared_ptr<ArrayData>>(this->value)->type;
  }
  if (this->kind() == Datum::CHUNKED_ARRAY) {
    return std::get<std::shared_ptr<ChunkedArray>>(this->value)->type();
  }
  if (this->kind() == Datum::SCALAR) {
    return std::get<std::shared_ptr<Scalar>>(this->value)->type;
  }
  static std::shared_ptr<DataType> no_type;
  return no_type;
}

const std::shared_ptr<Schema>& Datum::schema() const {
  if (this->kind() == Datum::RECORD_BATCH) {
    return std::get<std::shared_ptr<RecordBatch>>(this->value)->schema();
  }
  if (this->kind() == Datum::TABLE) {
    return std::get<std::shared_ptr<Table>>(this->value)->schema();
  }
  static std::shared_ptr<Schema> no_schema;
  return no_schema;
}

int64_t Datum::length() const {
  switch (this->kind()) {
    case Datum::ARRAY:
      return std::get<std::shared_ptr<ArrayData>>(this->value)->length;
    case Datum::CHUNKED_ARRAY:
      return std::get<std::shared_ptr<ChunkedArray>>(this->value)->length();
    case Datum::RECORD_BATCH:
      return std::get<std::shared_ptr<RecordBatch>>(this->value)->num_rows();
    case Datum::TABLE:
      return std::get<std::shared_ptr<Table>>(this->value)->num_rows();
    case Datum::SCALAR:
      return 1;
    default:
      return kUnknownLength;
  }
}

int64_t Datum::TotalBufferSize() const {
  switch (this->kind()) {
    case Datum::ARRAY:
      return util::TotalBufferSize(*std::get<std::shared_ptr<ArrayData>>(this->value));
    case Datum::CHUNKED_ARRAY:
      return util::TotalBufferSize(*std::get<std::shared_ptr<ChunkedArray>>(this->value));
    case Datum::RECORD_BATCH:
      return util::TotalBufferSize(*std::get<std::shared_ptr<RecordBatch>>(this->value));
    case Datum::TABLE:
      return util::TotalBufferSize(*std::get<std::shared_ptr<Table>>(this->value));
    case Datum::SCALAR:
      return 0;
    default:
      DCHECK(false);
      return 0;
  }
}

int64_t Datum::null_count() const {
  if (this->kind() == Datum::ARRAY) {
    return std::get<std::shared_ptr<ArrayData>>(this->value)->GetNullCount();
  } else if (this->kind() == Datum::CHUNKED_ARRAY) {
    return std::get<std::shared_ptr<ChunkedArray>>(this->value)->null_count();
  } else if (this->kind() == Datum::SCALAR) {
    const auto& val = *std::get<std::shared_ptr<Scalar>>(this->value);
    return val.is_valid ? 0 : 1;
  } else {
    DCHECK(false) << "This function only valid for array-like values";
    return 0;
  }
}

ArrayVector Datum::chunks() const {
  if (!this->is_arraylike()) {
    return {};
  }
  if (this->is_array()) {
    return {this->make_array()};
  }
  return this->chunked_array()->chunks();
}

bool Datum::Equals(const Datum& other) const {
  if (this->kind() != other.kind()) return false;

  switch (this->kind()) {
    case Datum::NONE:
      return true;
    case Datum::SCALAR:
      return internal::SharedPtrEquals(this->scalar(), other.scalar());
    case Datum::ARRAY:
      return internal::SharedPtrEquals(this->make_array(), other.make_array());
    case Datum::CHUNKED_ARRAY:
      return internal::SharedPtrEquals(this->chunked_array(), other.chunked_array());
    case Datum::RECORD_BATCH:
      return internal::SharedPtrEquals(this->record_batch(), other.record_batch());
    case Datum::TABLE:
      return internal::SharedPtrEquals(this->table(), other.table());
    default:
      return false;
  }
}

std::string Datum::ToString() const {
  switch (this->kind()) {
    case Datum::NONE:
      return "nullptr";
    case Datum::SCALAR:
      return "Scalar(" + scalar()->ToString() + ")";
    case Datum::ARRAY:
      return "Array(" + make_array()->ToString() + ")";
    case Datum::CHUNKED_ARRAY:
      return "ChunkedArray(" + chunked_array()->ToString() + ")";
    case Datum::RECORD_BATCH:
      return "RecordBatch(" + record_batch()->ToString() + ")";
    case Datum::TABLE:
      return "Table(" + table()->ToString() + ")";
    default:
      DCHECK(false);
      return "";
  }
}

void PrintTo(const Datum& datum, std::ostream* os) {
  switch (datum.kind()) {
    case Datum::SCALAR:
      *os << datum.scalar()->ToString();
      break;
    case Datum::ARRAY:
      *os << datum.make_array()->ToString();
      break;
    default:
      *os << datum.ToString();
  }
}

std::string ToString(Datum::Kind kind) {
  switch (kind) {
    case Datum::NONE:
      return "None";
    case Datum::SCALAR:
      return "Scalar";
    case Datum::ARRAY:
      return "Array";
    case Datum::CHUNKED_ARRAY:
      return "ChunkedArray";
    case Datum::RECORD_BATCH:
      return "RecordBatch";
    case Datum::TABLE:
      return "Table";
    default:
      DCHECK(false);
      return "";
  }
}

}  // namespace arrow
