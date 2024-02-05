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
#include <variant>
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
  return std::visit(
      [](const auto& value) -> const std::shared_ptr<DataType>& {
        using T = std::decay_t<decltype(value)>;

        if constexpr (std::is_same_v<T, std::shared_ptr<Scalar>> ||
                      std::is_same_v<T, std::shared_ptr<ArrayData>>) {
          return value->type;
        } else if constexpr (std::is_same_v<T, std::shared_ptr<ChunkedArray>>) {
          return value->type();
        } else {
          static std::shared_ptr<DataType> no_type;
          return no_type;
        }
      },
      this->value);
}

const std::shared_ptr<Schema>& Datum::schema() const {
  return std::visit(
      [](const auto& value) -> const std::shared_ptr<Schema>& {
        using T = std::decay_t<decltype(value)>;

        if constexpr (std::is_same_v<T, std::shared_ptr<RecordBatch>> ||
                      std::is_same_v<T, std::shared_ptr<Table>>) {
          return value->schema();
        } else {
          static std::shared_ptr<Schema> no_schema;
          return no_schema;
        }
      },
      this->value);
}

int64_t Datum::length() const {
  return std::visit(
      [](const auto& value) -> int64_t {
        using T = std::decay_t<decltype(value)>;

        if constexpr (std::is_same_v<T, std::shared_ptr<Scalar>>) {
          return 1;
        } else if constexpr (std::is_same_v<T, std::shared_ptr<ArrayData>>) {
          return value->length;
        } else if constexpr (std::is_same_v<T, std::shared_ptr<ChunkedArray>>) {
          return value->length();
        } else if constexpr (std::is_same_v<T, std::shared_ptr<RecordBatch>> ||  // NOLINT
                             std::is_same_v<T, std::shared_ptr<Table>>) {
          return value->num_rows();
        } else {
          return kUnknownLength;
        }
      },
      this->value);
}

int64_t Datum::TotalBufferSize() const {
  return std::visit(
      [](const auto& value) -> int64_t {
        using T = std::decay_t<decltype(value)>;

        if constexpr (std::is_same_v<T, std::shared_ptr<Scalar>>) {
          return 0;
        } else if constexpr (std::is_same_v<T, std::shared_ptr<ArrayData>> ||  // NOLINT
                             std::is_same_v<T, std::shared_ptr<ChunkedArray>> ||
                             std::is_same_v<T, std::shared_ptr<RecordBatch>> ||
                             std::is_same_v<T, std::shared_ptr<Table>>) {
          return util::TotalBufferSize(*value);
        } else {
          DCHECK(false);
          return 0;
        }
      },
      this->value);
}

int64_t Datum::null_count() const {
  return std::visit(
      [](const auto& value) -> int64_t {
        using T = std::decay_t<decltype(value)>;

        if constexpr (std::is_same_v<T, std::shared_ptr<Scalar>>) {
          return value->is_valid ? 0 : 1;
        } else if constexpr (std::is_same_v<T, std::shared_ptr<ArrayData>>) {
          return value->GetNullCount();
        } else if constexpr (std::is_same_v<T, std::shared_ptr<ChunkedArray>>) {
          return value->null_count();
        } else {
          DCHECK(false) << "This function only valid for array-like values";
          return 0;
        }
      },
      this->value);
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
  return std::visit(
      [this](const auto& value) -> std::string {
        using T = std::decay_t<decltype(value)>;

        if constexpr (std::is_same_v<T, Empty>) {
          return "nullptr";
        } else if constexpr (std::is_same_v<T, std::shared_ptr<Scalar>>) {
          return "Scalar(" + value->ToString() + ")";
        } else if constexpr (std::is_same_v<T, std::shared_ptr<ArrayData>>) {
          return "Array(" + make_array()->ToString() + ")";
        } else if constexpr (std::is_same_v<T, std::shared_ptr<ChunkedArray>>) {
          return "ChunkedArray(" + value->ToString() + ")";
        } else if constexpr (std::is_same_v<T, std::shared_ptr<RecordBatch>>) {
          return "RecordBatch(" + value->ToString() + ")";
        } else if constexpr (std::is_same_v<T, std::shared_ptr<Table>>) {
          return "Table(" + value->ToString() + ")";
        } else {
          static_assert(!std::is_same_v<T, T>, "unhandled type");
        }
      },
      this->value);
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
