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
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/visibility.h"

namespace arrow {

template<uint32_t width>
struct DecimalBuilderHelper;

#define DECL_DECIMAL_TYPES_HELPER(width)     \
template<>                                   \
struct DecimalBuilderHelper<width> {         \
  using type = Decimal##width##Type;         \
  using array_type = Decimal##width##Array;  \
  using value_type = Decimal##width;         \
};

DECL_DECIMAL_TYPES_HELPER(128)
DECL_DECIMAL_TYPES_HELPER(256)

#undef DECL_DECIMAL_TYPES_HELPER

template<uint32_t width>
class BaseDecimalBuilder : public FixedSizeBinaryBuilder {
public:
  using TypeClass = typename DecimalBuilderHelper<width>::type;

  explicit BaseDecimalBuilder(const std::shared_ptr<DataType>& type,
                            MemoryPool* pool = default_memory_pool());

  using FixedSizeBinaryBuilder::Append;
  using FixedSizeBinaryBuilder::AppendValues;
  using FixedSizeBinaryBuilder::Reset;

  Status Append(typename DecimalBuilderHelper<width>::value_type val);
  void UnsafeAppend(typename DecimalBuilderHelper<width>::value_type val);
  void UnsafeAppend(util::string_view val);

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<typename DecimalBuilderHelper<width>::array_type>* out) { return FinishTyped(out); }

  std::shared_ptr<DataType> type() const override { return decimal_type_; }

 protected:
  std::shared_ptr<typename DecimalBuilderHelper<width>::type> decimal_type_;
};

#define DECIMAL_BUILDER_DECL(width)                                             \
class ARROW_EXPORT Decimal##width##Builder : public BaseDecimalBuilder<width> { \
  using BaseDecimalBuilder<width>::BaseDecimalBuilder;                          \
};

DECIMAL_BUILDER_DECL(128)
DECIMAL_BUILDER_DECL(256)

#undef DECIMAL_BUILDER_DECL

using DecimalBuilder = Decimal128Builder;

}  // namespace arrow
