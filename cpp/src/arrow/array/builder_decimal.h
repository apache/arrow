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

#include <memory>

#include "arrow/array/array_decimal.h"
#include "arrow/array/builder_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/data.h"
#include "arrow/util/decimal_type_traits.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/visibility.h"

namespace arrow {

template<uint32_t width>
class BaseDecimalBuilder : public FixedSizeBinaryBuilder {
public:
  using TypeClass = typename DecimalTypeTraits<width>::TypeClass;

  explicit BaseDecimalBuilder(const std::shared_ptr<DataType>& type,
                            MemoryPool* pool = default_memory_pool());

  using FixedSizeBinaryBuilder::Append;
  using FixedSizeBinaryBuilder::AppendValues;
  using FixedSizeBinaryBuilder::Reset;

  Status Append(typename DecimalTypeTraits<width>::ValueType val);
  void UnsafeAppend(typename DecimalTypeTraits<width>::ValueType val);
  void UnsafeAppend(util::string_view val);

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<typename DecimalTypeTraits<width>::ArrayType>* out) { return FinishTyped(out); }

  std::shared_ptr<DataType> type() const override { return decimal_type_; }

 protected:
  std::shared_ptr<typename DecimalTypeTraits<width>::TypeClass> decimal_type_;
};

/// Builder class for decimal 128-bit                                    
class ARROW_EXPORT Decimal128Builder : public BaseDecimalBuilder<128> {
  using BaseDecimalBuilder<128>::BaseDecimalBuilder;
};

/// Builder class for decimal 128-bit 
class ARROW_EXPORT Decimal256Builder : public BaseDecimalBuilder<256> {
  using BaseDecimalBuilder<256>::BaseDecimalBuilder;
};

// Backward compatibility
using DecimalBuilder = Decimal128Builder;

}  // namespace arrow
