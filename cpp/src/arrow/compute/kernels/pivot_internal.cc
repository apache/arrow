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
#include <string_view>
#include <unordered_set>

#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_binary.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/row/grouper.h"
#include "arrow/scalar.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/unreachable.h"
#include "arrow/visit_type_inline.h"

namespace arrow::compute::internal {

using ::arrow::util::span;

struct ConcretePivotWiderKeyMapper : public PivotWiderKeyMapper {
  Status Init(const DataType& key_type, const PivotWiderOptions* options,
              ExecContext* ctx) {
    if (options->key_names.size() > static_cast<size_t>(kMaxPivotKey)) {
      return Status::NotImplemented("Pivoting to more than ",
                                    static_cast<size_t>(kMaxPivotKey), " columns: got ",
                                    options->key_names.size());
    }
    unexpected_key_behavior_ = options->unexpected_key_behavior;
    ARROW_ASSIGN_OR_RAISE(grouper_, Grouper::Make({&key_type}, ctx));
    // Build a binary array of the pivot key values, and cast it to the desired key type
    BinaryBuilder builder(ctx->memory_pool());
    RETURN_NOT_OK(builder.Reserve(options->key_names.size()));
    int64_t total_length = 0;
    for (const auto& key : options->key_names) {
      total_length += static_cast<int64_t>(key.length());
    }
    RETURN_NOT_OK(builder.ReserveData(total_length));
    for (const auto& key : options->key_names) {
      builder.UnsafeAppend(key);
    }
    ARROW_ASSIGN_OR_RAISE(auto binary_key_array, builder.Finish());
    ARROW_ASSIGN_OR_RAISE(auto key_array,
                          Cast(*binary_key_array, &key_type, CastOptions::Safe(), ctx));
    // Populate the grouper with the keys from the array
    ExecSpan batch({ExecValue(*key_array->data())}, key_array->length());
    RETURN_NOT_OK(grouper_->Populate(batch));
    if (grouper_->num_groups() != options->key_names.size()) {
      // There's a duplicate key, find it to emit a nicer error message
      std::unordered_set<std::string_view> seen;
      for (const auto& key : options->key_names) {
        auto [_, inserted] = seen.emplace(key);
        if (!inserted) {
          return Status::KeyError("Duplicate key name '", key, "' in PivotWiderOptions");
        }
      }
      Unreachable("Grouper doesn't agree with std::unordered_set");
    }
    return Status::OK();
  }

  Result<std::shared_ptr<ArrayData>> MapKeys(const ArraySpan& array) override {
    if (array.GetNullCount() != 0) {
      return NullKeyName();
    }
    return MapKeysInternal(array, array.length);
  }

  Result<std::optional<PivotWiderKeyIndex>> MapKey(const Scalar& scalar) override {
    if (!scalar.is_valid) {
      return NullKeyName();
    }
    ARROW_ASSIGN_OR_RAISE(auto group_id_array, MapKeysInternal(&scalar, /*length=*/1));
    DCHECK_EQ(group_id_array->length, 1);
    if (group_id_array->GetNullCount() == 0) {
      return group_id_array->GetValues<uint32_t>(1)[0];
    } else {
      // For UnexpectedKeyBehavior::kIgnore
      return std::nullopt;
    }
  }

 protected:
  Result<std::shared_ptr<ArrayData>> MapKeysInternal(const ExecValue& values,
                                                     int64_t length) {
    ARROW_ASSIGN_OR_RAISE(auto result, grouper_->Lookup(ExecSpan({values}, length)));
    DCHECK(result.is_array());
    DCHECK_EQ(result.type()->id(), Type::UINT32);
    auto group_id_array = result.array();
    const bool has_nulls = (group_id_array->GetNullCount() != 0);
    if (ARROW_PREDICT_FALSE(has_nulls) &&
        unexpected_key_behavior_ == PivotWiderOptions::kRaise) {
      // Extract unexpected key name, to emit a nicer error message
      int64_t null_pos = 0;
      DCHECK_NE(group_id_array->buffers[0], nullptr);
      ::arrow::internal::BitRunReader bit_run_reader(group_id_array->buffers[0]->data(),
                                                     group_id_array->offset,
                                                     group_id_array->length);
      // Search the first unset validity bit, indicating the first unexpected key
      for (;;) {
        auto run = bit_run_reader.NextRun();
        if (run.length == 0 || !run.set) {
          break;
        }
        null_pos += run.length;
      }
      DCHECK_LT(null_pos, group_id_array->length);
      DCHECK_LT(null_pos, values.length());
      std::shared_ptr<Scalar> key_scalar;
      if (values.is_scalar()) {
        DCHECK_EQ(null_pos, 0);
        key_scalar = values.scalar->GetSharedPtr();
      } else {
        ARROW_ASSIGN_OR_RAISE(key_scalar, values.array.ToArray()->GetScalar(null_pos));
      }
      return Status::KeyError("Unexpected pivot key: ", key_scalar->ToString());
    }
    return group_id_array;
  }

  Status NullKeyName() { return Status::KeyError("pivot key name cannot be null"); }

  std::unique_ptr<Grouper> grouper_;
  PivotWiderOptions::UnexpectedKeyBehavior unexpected_key_behavior_;
  std::shared_ptr<Buffer> last_group_ids_;
};

Result<std::unique_ptr<PivotWiderKeyMapper>> PivotWiderKeyMapper::Make(
    const DataType& key_type, const PivotWiderOptions* options, ExecContext* ctx) {
  auto instance = std::make_unique<ConcretePivotWiderKeyMapper>();
  RETURN_NOT_OK(instance->Init(key_type, options, ctx));
  // We can remove this static_cast() once we drop support for g++
  // 7.5.0 (we require C++20).
  return static_cast<std::unique_ptr<PivotWiderKeyMapper>>(std::move(instance));
}

}  // namespace arrow::compute::internal
