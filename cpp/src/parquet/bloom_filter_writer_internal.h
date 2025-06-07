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

#pragma once

#include "parquet/bloom_filter.h"

namespace arrow {
class Array;
}

namespace parquet {
class ColumnDescriptor;
}

namespace parquet::internal {

template <typename ParquetType>
class PARQUET_EXPORT BloomFilterWriterImpl {
 public:
  using T = typename ParquetType::c_type;

  BloomFilterWriterImpl(const ColumnDescriptor* descr, BloomFilter* bloom_filter);

  void UpdateBloomFilter(const T* values, int64_t num_values);
  void UpdateBloomFilterSpaced(const T* values, int64_t num_values,
                               const uint8_t* valid_bits, int64_t valid_bits_offset);
  void UpdateBloomFilterArray(const ::arrow::Array& values);

  bool HasBloomFilter() const;

 private:
  const ColumnDescriptor* descr_;
  BloomFilter* bloom_filter_;
};

}  // namespace parquet::internal
