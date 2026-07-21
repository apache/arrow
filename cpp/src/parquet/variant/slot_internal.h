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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_set>
#include <variant>
#include <vector>

#include "arrow/type_fwd.h"
#include "parquet/variant/builder.h"
#include "parquet/variant/decoding.h"

namespace parquet::variant::internal {

enum class TypedScalarKind {
  kBoolean,
  kInt8,
  kInt16,
  kInt32,
  kInt64,
  kFloat,
  kDouble,
  kBinary,
  kString,
  kDate,
  kTimeNTZMicros,
  kTimestampMicros,
  kTimestampNanos,
  kDecimal4From32,
  kDecimal4From64,
  kDecimal8From64,
  kDecimal4From128,
  kDecimal8From128,
  kDecimal16From128,
  kUuidFixed,
  kUuidExtension,
};

struct CompiledTypedScalarPlan {
  TypedScalarKind kind;
  ::arrow::Type::type physical_type = ::arrow::Type::NA;
  uint8_t scale = 0;
  bool adjusted_to_utc = false;
};

struct CompiledVariantRowPlan {
  struct FieldGroup {
    std::shared_ptr<::arrow::Array> array;
    // Stores the child plan by index because the parent child vector may reallocate.
    size_t child_plan_index = 0;
  };

  struct ObjectField {
    // Borrows the field name owned by the typed array retained in the parent plan.
    std::string_view field_name;
    FieldGroup field_group;
  };

  struct Primitive {
    CompiledTypedScalarPlan scalar;
  };

  struct Array {
    FieldGroup element;
  };

  struct Object {
    std::vector<ObjectField> fields;
    std::unordered_set<std::string_view> field_names;
  };

  struct Typed {
    std::shared_ptr<::arrow::Array> array;
    std::variant<Primitive, Array, Object> plan;
  };

  std::shared_ptr<::arrow::Array> value_array;
  std::optional<Typed> typed;
  std::vector<CompiledVariantRowPlan> children;
};

struct BuildTarget {
  struct Row {
    std::reference_wrapper<VariantValueRowBuilder> builder;
  };

  struct ObjectField {
    std::reference_wrapper<VariantObjectBuilder> builder;
    std::string_view field_name;
  };

  struct ListElement {
    std::reference_wrapper<VariantListBuilder> builder;
  };

  using Destination = std::variant<Row, ObjectField, ListElement>;
  Destination destination;
};

CompiledVariantRowPlan CompileVariantRowPlan(
    const std::shared_ptr<::arrow::Array>& value_array,
    const std::shared_ptr<::arrow::Array>& typed_array);

template <bool strict>
void ProcessSlot(const VariantMetadataView& metadata, const CompiledVariantRowPlan& plan,
                 int64_t row, BuildTarget* target = nullptr,
                 std::string_view path = "root");

}  // namespace parquet::variant::internal
