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

#include <algorithm>
#include <memory>

#include "parquet/exception.h"
#include "parquet/schema.h"
#include "parquet/types.h"
#include "parquet/util/comparison.h"

namespace parquet {

std::shared_ptr<Comparator> Comparator::Make(const ColumnDescriptor* descr) {
  if (SortOrder::SIGNED == descr->sort_order()) {
    switch (descr->physical_type()) {
      case Type::BOOLEAN:
        return std::make_shared<CompareDefaultBoolean>();
      case Type::INT32:
        return std::make_shared<CompareDefaultInt32>();
      case Type::INT64:
        return std::make_shared<CompareDefaultInt64>();
      case Type::FLOAT:
        return std::make_shared<CompareDefaultFloat>();
      case Type::DOUBLE:
        return std::make_shared<CompareDefaultDouble>();
      case Type::BYTE_ARRAY:
        return std::make_shared<CompareDefaultByteArray>();
      case Type::FIXED_LEN_BYTE_ARRAY:
        return std::make_shared<CompareDefaultFLBA>(descr->type_length());
      default:
        ParquetException::NYI("Signed Compare not implemented");
    }
  } else if (SortOrder::UNSIGNED == descr->sort_order()) {
    switch (descr->physical_type()) {
      case Type::INT32:
        return std::make_shared<CompareUnsignedInt32>();
      case Type::INT64:
        return std::make_shared<CompareUnsignedInt64>();
      case Type::BYTE_ARRAY:
        return std::make_shared<CompareUnsignedByteArray>();
      case Type::FIXED_LEN_BYTE_ARRAY:
        return std::make_shared<CompareUnsignedFLBA>(descr->type_length());
      default:
        ParquetException::NYI("Unsigned Compare not implemented");
    }
  } else {
    throw ParquetException("UNKNOWN Sort Order");
  }
  return nullptr;
}

bool CompareUnsignedInt32::operator()(const int32_t& a, const int32_t& b) {
  const uint32_t ua = a;
  const uint32_t ub = b;
  return (ua < ub);
}

bool CompareUnsignedInt64::operator()(const int64_t& a, const int64_t& b) {
  const uint64_t ua = a;
  const uint64_t ub = b;
  return (ua < ub);
}

bool CompareUnsignedInt96::operator()(const Int96& a, const Int96& b) {
  if (a.value[2] != b.value[2]) {
    return (a.value[2] < b.value[2]);
  } else if (a.value[1] != b.value[1]) {
    return (a.value[1] < b.value[1]);
  }
  return (a.value[0] < b.value[0]);
}

bool CompareUnsignedByteArray::operator()(const ByteArray& a, const ByteArray& b) {
  const uint8_t* aptr = reinterpret_cast<const uint8_t*>(a.ptr);
  const uint8_t* bptr = reinterpret_cast<const uint8_t*>(b.ptr);
  return std::lexicographical_compare(aptr, aptr + a.len, bptr, bptr + b.len);
}

CompareUnsignedFLBA::CompareUnsignedFLBA(int length) : CompareDefaultFLBA(length) {}

bool CompareUnsignedFLBA::operator()(const FLBA& a, const FLBA& b) {
  const uint8_t* aptr = reinterpret_cast<const uint8_t*>(a.ptr);
  const uint8_t* bptr = reinterpret_cast<const uint8_t*>(b.ptr);
  return std::lexicographical_compare(aptr, aptr + type_length_, bptr,
                                      bptr + type_length_);
}

}  // namespace parquet
