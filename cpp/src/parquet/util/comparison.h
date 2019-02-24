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

#ifndef PARQUET_UTIL_COMPARISON_H
#define PARQUET_UTIL_COMPARISON_H

#include <algorithm>
#include <memory>

#include "arrow/util/bit-util.h"

#include "parquet/exception.h"
#include "parquet/schema.h"
#include "parquet/types.h"
#include "parquet/util/visibility.h"

namespace parquet {

class PARQUET_EXPORT Comparator {
 public:
  virtual ~Comparator() {}
  static std::shared_ptr<Comparator> Make(const ColumnDescriptor* descr);
};

// The default comparison is SIGNED
template <typename DType>
class CompareDefault : public Comparator {
 public:
  using T = typename DType::c_type;

  CompareDefault() {}
  explicit CompareDefault(int length) : type_length_(length) {}

  virtual bool operator()(const T& a, const T& b) { return a < b; }

  virtual void  minmax_element(const T* a, const T* b, T& min, T& max) {
    auto batch_minmax = std::minmax_element(a, b, std::ref(*(this)));
    min = *batch_minmax.first;
    max = *batch_minmax.second;
  }

  virtual void minmax_spaced(::arrow::internal::BitmapReader& valid_bits_reader,
                             const T* values, int64_t offset, int64_t length,
                             T& min, T& max) {
    for (; offset < length; offset++) {
      if (valid_bits_reader.IsSet()) {
        if ((std::ref(*(this)))(values[offset], min)) {
          min = values[offset];
        } else if ((std::ref(*(this)))(max, values[offset])) {
          max = values[offset];
        }
      }
      valid_bits_reader.Next();
    }
  }

  int32_t type_length_;
};

template <>
bool CompareDefault<Int96Type>::operator()(const Int96& a, const Int96& b);

template <>
bool CompareDefault<ByteArrayType>::operator()(const ByteArray& a, const ByteArray& b);

template <>
bool CompareDefault<FLBAType>::operator()(const FLBA& a, const FLBA& b);

using CompareDefaultBoolean = CompareDefault<BooleanType>;
using CompareDefaultInt32 = CompareDefault<Int32Type>;
using CompareDefaultInt64 = CompareDefault<Int64Type>;
using CompareDefaultInt96 = CompareDefault<Int96Type>;
using CompareDefaultFloat = CompareDefault<FloatType>;
using CompareDefaultDouble = CompareDefault<DoubleType>;
using CompareDefaultByteArray = CompareDefault<ByteArrayType>;
using CompareDefaultFLBA = CompareDefault<FLBAType>;

// Define Unsigned Comparators
class PARQUET_EXPORT CompareUnsignedInt32 : public CompareDefaultInt32 {
 public:
  bool operator()(const int32_t& a, const int32_t& b) override;
};

class PARQUET_EXPORT CompareUnsignedInt64 : public CompareDefaultInt64 {
 public:
  bool operator()(const int64_t& a, const int64_t& b) override;
};

class PARQUET_EXPORT CompareUnsignedInt96 : public CompareDefaultInt96 {
 public:
  bool operator()(const Int96& a, const Int96& b) override;
};

class PARQUET_EXPORT CompareUnsignedByteArray : public CompareDefaultByteArray {
 public:
  bool operator()(const ByteArray& a, const ByteArray& b) override;
};

class PARQUET_EXPORT CompareUnsignedFLBA : public CompareDefaultFLBA {
 public:
  explicit CompareUnsignedFLBA(int length);
  bool operator()(const FLBA& a, const FLBA& b) override;
};

}  // namespace parquet

#endif  // PARQUET_UTIL_COMPARISON_H
