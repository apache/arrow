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

#include "arrow/compute/kernels/pivot_internal.h"

#include <cstdint>

#include "arrow/compute/exec.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/scalar.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/visit_type_inline.h"

namespace arrow::compute::internal {

using ::arrow::util::span;

struct BasePivotKeyMapper : public PivotWiderKeyMapper {
  Status Init(const PivotWiderOptions* options) override {
    if (options->key_names.size() > static_cast<size_t>(kMaxPivotKey) + 1) {
      return Status::NotImplemented("Pivoting to more than ",
                                    static_cast<size_t>(kMaxPivotKey) + 1,
                                    " columns: got ", options->key_names.size());
    }
    key_name_map_.reserve(options->key_names.size());
    PivotWiderKeyIndex index = 0;
    for (const auto& key_name : options->key_names) {
      bool inserted =
          key_name_map_.try_emplace(std::string_view(key_name), index++).second;
      if (!inserted) {
        return Status::KeyError("Duplicate key name '", key_name,
                                "' in PivotWiderOptions");
      }
    }
    unexpected_key_behavior_ = options->unexpected_key_behavior;
    return Status::OK();
  }

 protected:
  Result<PivotWiderKeyIndex> KeyNotFound(std::string_view key_name) {
    if (unexpected_key_behavior_ == PivotWiderOptions::kIgnore) {
      return kNullPivotKey;
    }
    DCHECK_EQ(unexpected_key_behavior_, PivotWiderOptions::kRaise);
    return Status::KeyError("Unexpected pivot key: ", key_name);
  }

  Result<PivotWiderKeyIndex> LookupKey(std::string_view key_name) {
    const auto it = this->key_name_map_.find(key_name);
    if (ARROW_PREDICT_FALSE(it == this->key_name_map_.end())) {
      return KeyNotFound(key_name);
    } else {
      return it->second;
    }
  }

  Status NullKeyName() { return Status::KeyError("pivot key name cannot be null"); }

  // The strings backing the string_views should be kept alive by PivotWiderOptions.
  std::unordered_map<std::string_view, PivotWiderKeyIndex> key_name_map_;
  PivotWiderOptions::UnexpectedKeyBehavior unexpected_key_behavior_;
  TypedBufferBuilder<PivotWiderKeyIndex> key_indices_buffer_;
};

template <typename KeyType>
struct TypedPivotKeyMapper : public BasePivotKeyMapper {
  Result<span<const PivotWiderKeyIndex>> MapKeys(const ArraySpan& array) override {
    // XXX Should use a faster hashing facility than unordered_map, for example
    // Grouper or SwissTable.
    RETURN_NOT_OK(this->key_indices_buffer_.Reserve(array.length));
    PivotWiderKeyIndex* key_indices = this->key_indices_buffer_.mutable_data();
    int64_t i = 0;
    RETURN_NOT_OK(VisitArrayValuesInline<KeyType>(
        array,
        [&](std::string_view key_name) {
          ARROW_ASSIGN_OR_RAISE(key_indices[i], LookupKey(key_name));
          ++i;
          return Status::OK();
        },
        [&]() { return NullKeyName(); }));
    return span(key_indices, array.length);
  }

  Result<PivotWiderKeyIndex> MapKey(const Scalar& scalar) override {
    if (!scalar.is_valid) {
      return NullKeyName();
    }
    const auto& binary_scalar = checked_cast<const BaseBinaryScalar&>(scalar);
    return LookupKey(binary_scalar.view());
  }
};

Result<std::unique_ptr<PivotWiderKeyMapper>> PivotWiderKeyMapper::Make(
    const DataType& key_type, const PivotWiderOptions* options) {
  std::unique_ptr<PivotWiderKeyMapper> instance;

  auto visit_key_type =
      [&](auto&& key_type) -> Result<std::unique_ptr<PivotWiderKeyMapper>> {
    using T = std::decay_t<decltype(key_type)>;
    // Only binary-like keys are supported for now
    if constexpr (is_base_binary_type<T>::value) {
      instance = std::make_unique<TypedPivotKeyMapper<T>>();
      RETURN_NOT_OK(instance->Init(options));
      return std::move(instance);
    }
    return Status::NotImplemented("Pivot key type: ", key_type);
  };

  return VisitType(key_type, visit_key_type);
}

}  // namespace arrow::compute::internal
