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

#include "arrow/compute/kernel.h"

#include <memory>

#include "arrow/record_batch.h"
#include "arrow/table.h"

namespace arrow {
namespace compute {

std::shared_ptr<DataType> Datum::type() const {
  if (this->kind() == Datum::ARRAY) {
    return util::get<std::shared_ptr<ArrayData>>(this->value)->type;
  } else if (this->kind() == Datum::CHUNKED_ARRAY) {
    return util::get<std::shared_ptr<ChunkedArray>>(this->value)->type();
  } else if (this->kind() == Datum::SCALAR) {
    return util::get<std::shared_ptr<Scalar>>(this->value)->type;
  }
  return nullptr;
}

int64_t Datum::length() const {
  if (this->kind() == Datum::ARRAY) {
    return util::get<std::shared_ptr<ArrayData>>(this->value)->length;
  } else if (this->kind() == Datum::CHUNKED_ARRAY) {
    return util::get<std::shared_ptr<ChunkedArray>>(this->value)->length();
  } else if (this->kind() == Datum::SCALAR) {
    return 1;
  }
  return kUnknownLength;
}

static bool CollectionEquals(const std::vector<Datum>& left,
                             const std::vector<Datum>& right) {
  if (left.size() != right.size()) return false;

  for (size_t i = 0; i < left.size(); i++)
    if (!left[i].Equals(right[i])) return false;

  return true;
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
    case Datum::COLLECTION:
      return CollectionEquals(this->collection(), other.collection());
    default:
      return false;
  }
}

}  // namespace compute
}  // namespace arrow
