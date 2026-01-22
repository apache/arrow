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

#include <cmath>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array/concatenate.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/kernels/hash_aggregate_internal.h"
#include "arrow/compute/kernels/pivot_internal.h"
#include "arrow/compute/registry_internal.h"
#include "arrow/compute/row/grouper.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/span.h"
#include "arrow/visit_type_inline.h"

namespace arrow::compute::internal {

using ::arrow::util::span;

namespace {

// ----------------------------------------------------------------------
// Pivot implementation

struct GroupedPivotAccumulator {
  Status Init(ExecContext* ctx, std::shared_ptr<DataType> value_type,
              const PivotWiderOptions* options) {
    ctx_ = ctx;
    value_type_ = std::move(value_type);
    num_keys_ = static_cast<int>(options->key_names.size());
    num_groups_ = 0;
    columns_.resize(num_keys_);
    scratch_buffer_ = BufferBuilder(ctx_->memory_pool());
    return Status::OK();
  }

  Status Consume(span<const uint32_t> groups, const std::shared_ptr<ArrayData>& keys,
                 const ArraySpan& values) {
    // To dispatch the values into the right (group, key) coordinates,
    // we first compute a vector of take indices for each output column.
    //
    // For each index #i, we set take_indices[keys[#i]][groups[#i]] = #i.
    // Unpopulated take_indices entries are null.
    //
    // For example, assuming we get:
    //   groups  |  keys
    // ===================
    //    1      |   0
    //    3      |   1
    //    1      |   1
    //    0      |   1
    //
    // We are going to compute:
    // - take_indices[key = 0] = [null, 0, null, null]
    // - take_indices[key = 1] = [3, 2, null, 1]
    //
    // Then each output column is computed by taking the values with the
    // respective take_indices for the column's keys.
    //

    DCHECK_EQ(keys->type->id(), Type::UINT32);
    DCHECK_EQ(groups.size(), static_cast<size_t>(keys->length));
    DCHECK_EQ(groups.size(), static_cast<size_t>(values.length));

    std::shared_ptr<DataType> take_index_type;
    std::vector<std::shared_ptr<Buffer>> take_indices(num_keys_);
    std::vector<std::shared_ptr<Buffer>> take_bitmaps(num_keys_);

    // A generic lambda that computes the take indices with the desired integer width
    auto compute_take_indices = [&](auto typed_index) {
      ARROW_UNUSED(typed_index);
      using TakeIndex = std::decay_t<decltype(typed_index)>;
      take_index_type = CTypeTraits<TakeIndex>::type_singleton();

      const int64_t take_indices_size =
          bit_util::RoundUpToMultipleOf64(num_groups_ * sizeof(TakeIndex));
      const int64_t take_bitmap_size =
          bit_util::RoundUpToMultipleOf64(bit_util::BytesForBits(num_groups_));
      const int64_t total_scratch_size =
          num_keys_ * (take_indices_size + take_bitmap_size);
      RETURN_NOT_OK(scratch_buffer_.Resize(total_scratch_size, /*shrink_to_fit=*/false));

      // Slice the scratch space into individual buffers for each output column's
      // take_indices array.
      std::vector<TakeIndex*> take_indices_data(num_keys_);
      std::vector<uint8_t*> take_bitmap_data(num_keys_);
      int64_t offset = 0;
      for (int i = 0; i < num_keys_; ++i) {
        take_indices[i] = std::make_shared<MutableBuffer>(
            scratch_buffer_.mutable_data() + offset, take_indices_size);
        take_indices_data[i] = take_indices[i]->mutable_data_as<TakeIndex>();
        offset += take_indices_size;
        take_bitmaps[i] = std::make_shared<MutableBuffer>(
            scratch_buffer_.mutable_data() + offset, take_bitmap_size);
        take_bitmap_data[i] = take_bitmaps[i]->mutable_data();
        memset(take_bitmap_data[i], 0, take_bitmap_size);
        offset += take_bitmap_size;
      }
      DCHECK_LE(offset, scratch_buffer_.capacity());

      // Populate the take_indices for each output column
      const uint8_t* keys_null_bitmap =
          (keys->GetNullCount() != 0) ? keys->GetValues<uint8_t>(0, 0) : nullptr;
      const uint32_t* key_values = keys->GetValues<uint32_t>(1);
      const uint8_t* values_null_bitmap =
          (values.GetNullCount() != 0) ? values.GetValues<uint8_t>(0, 0) : nullptr;
      return ::arrow::internal::VisitTwoBitBlocks(
          keys_null_bitmap, keys->offset, values_null_bitmap, values.offset,
          values.length,
          [&](int64_t i) {
            // Non-null key, non-null value
            const uint32_t group = groups[i];
            const uint32_t key = key_values[i];
            DCHECK_LT(static_cast<int>(key), num_keys_);
            if (ARROW_PREDICT_FALSE(bit_util::GetBit(take_bitmap_data[key], group))) {
              return DuplicateValue();
            }
            // For row #group in column #key, we are going to take the value at index #i
            bit_util::SetBit(take_bitmap_data[key], group);
            take_indices_data[key][group] = static_cast<TakeIndex>(i);
            return Status::OK();
          },
          [] { return Status::OK(); });
    };

    // Call compute_take_indices with the optimal integer width
    if (values.length <= static_cast<int64_t>(std::numeric_limits<uint8_t>::max())) {
      RETURN_NOT_OK(compute_take_indices(uint8_t{}));
    } else if (values.length <=
               static_cast<int64_t>(std::numeric_limits<uint16_t>::max())) {
      RETURN_NOT_OK(compute_take_indices(uint16_t{}));
    } else if (values.length <=
               static_cast<int64_t>(std::numeric_limits<uint32_t>::max())) {
      RETURN_NOT_OK(compute_take_indices(uint32_t{}));
    } else {
      RETURN_NOT_OK(compute_take_indices(uint64_t{}));
    }

    // Use take_indices to compute the output columns for this batch
    auto values_data = values.ToArrayData();
    ArrayVector new_columns(num_keys_);
    TakeOptions take_options(/*boundscheck=*/false);
    for (int i = 0; i < num_keys_; ++i) {
      auto indices_data =
          ArrayData::Make(take_index_type, num_groups_,
                          {std::move(take_bitmaps[i]), std::move(take_indices[i])});
      // If indices_data is all nulls, we can just ignore this column.
      if (indices_data->GetNullCount() != indices_data->length) {
        ARROW_ASSIGN_OR_RAISE(Datum grouped_column,
                              Take(values_data, indices_data, take_options, ctx_));
        new_columns[i] = grouped_column.make_array();
      }
    }
    // Merge them with the previous columns
    return MergeColumns(std::move(new_columns));
  }

  Status Consume(span<const uint32_t> groups, std::optional<PivotWiderKeyIndex> maybe_key,
                 const ArraySpan& values) {
    if (!maybe_key.has_value()) {
      // Nothing to update
      return Status::OK();
    }
    const auto key = maybe_key.value();
    DCHECK_LT(static_cast<int>(key), num_keys_);
    DCHECK_EQ(groups.size(), static_cast<size_t>(values.length));

    // The algorithm is simpler than in the array-taking version of Consume()
    // below, since only the column #key needs to be updated.
    std::shared_ptr<DataType> take_index_type;
    std::shared_ptr<Buffer> take_indices;
    std::shared_ptr<Buffer> take_bitmap;

    // A generic lambda that computes the take indices with the desired integer width
    auto compute_take_indices = [&](auto typed_index) {
      ARROW_UNUSED(typed_index);
      using TakeIndex = std::decay_t<decltype(typed_index)>;
      take_index_type = CTypeTraits<TakeIndex>::type_singleton();

      const int64_t take_indices_size =
          bit_util::RoundUpToMultipleOf64(num_groups_ * sizeof(TakeIndex));
      const int64_t take_bitmap_size =
          bit_util::RoundUpToMultipleOf64(bit_util::BytesForBits(num_groups_));
      const int64_t total_scratch_size = take_indices_size + take_bitmap_size;
      RETURN_NOT_OK(scratch_buffer_.Resize(total_scratch_size, /*shrink_to_fit=*/false));

      take_indices = std::make_shared<MutableBuffer>(scratch_buffer_.mutable_data(),
                                                     take_indices_size);
      take_bitmap = std::make_shared<MutableBuffer>(
          scratch_buffer_.mutable_data() + take_indices_size, take_bitmap_size);
      auto take_indices_data = take_indices->mutable_data_as<TakeIndex>();
      auto take_bitmap_data = take_bitmap->mutable_data();
      memset(take_bitmap_data, 0, take_bitmap_size);

      for (int64_t i = 0; i < values.length; ++i) {
        const uint32_t group = groups[i];
        if (!values.IsNull(i)) {
          if (bit_util::GetBit(take_bitmap_data, group)) {
            return DuplicateValue();
          }
          bit_util::SetBit(take_bitmap_data, group);
          take_indices_data[group] = static_cast<TakeIndex>(i);
        }
      }
      return Status::OK();
    };

    // Call compute_take_indices with the optimal integer width
    if (values.length <= static_cast<int64_t>(std::numeric_limits<uint8_t>::max())) {
      RETURN_NOT_OK(compute_take_indices(uint8_t{}));
    } else if (values.length <=
               static_cast<int64_t>(std::numeric_limits<uint16_t>::max())) {
      RETURN_NOT_OK(compute_take_indices(uint16_t{}));
    } else if (values.length <=
               static_cast<int64_t>(std::numeric_limits<uint32_t>::max())) {
      RETURN_NOT_OK(compute_take_indices(uint32_t{}));
    } else {
      RETURN_NOT_OK(compute_take_indices(uint64_t{}));
    }

    // Use take_indices to update column #key
    auto values_data = values.ToArrayData();
    auto indices_data = ArrayData::Make(
        take_index_type, num_groups_, {std::move(take_bitmap), std::move(take_indices)});
    TakeOptions take_options(/*boundscheck=*/false);
    ARROW_ASSIGN_OR_RAISE(Datum grouped_column,
                          Take(values_data, indices_data, take_options, ctx_));
    return MergeColumn(&columns_[key], grouped_column.make_array());
  }

  Status Resize(int64_t new_num_groups) {
    if (new_num_groups > std::numeric_limits<int32_t>::max()) {
      return Status::NotImplemented("Pivot with more 2**31 groups");
    }
    return ResizeColumns(new_num_groups);
  }

  Status Merge(GroupedPivotAccumulator&& other, const ArrayData& group_id_mapping) {
    // To merge `other` into `*this`, we simply merge their respective columns.
    // However, we must first transpose `other`'s rows using `group_id_mapping`.
    // This is a logical "scatter" operation.
    //
    // Since `scatter(indices)` is implemented as `take(inverse_permutation(indices))`,
    // we can save time by computing `inverse_permutation(indices)` once for all
    // columns.

    // Scatter/InversePermutation only accept signed indices. We checked
    // in Resize() above that we were inside the limites for int32.
    auto scatter_indices = group_id_mapping.Copy();
    scatter_indices->type = int32();
    std::shared_ptr<DataType> take_indices_type;
    if (num_groups_ - 1 <= std::numeric_limits<int8_t>::max()) {
      take_indices_type = int8();
    } else if (num_groups_ - 1 <= std::numeric_limits<int16_t>::max()) {
      take_indices_type = int16();
    } else {
      DCHECK_GE(num_groups_ - 1, std::numeric_limits<int32_t>::max());
      take_indices_type = int32();
    }
    InversePermutationOptions options(/*max_index=*/num_groups_ - 1, take_indices_type);
    ARROW_ASSIGN_OR_RAISE(auto take_indices,
                          InversePermutation(scatter_indices, options, ctx_));
    auto scatter_column =
        [&](const std::shared_ptr<Array>& column) -> Result<std::shared_ptr<Array>> {
      TakeOptions take_options(/*boundscheck=*/false);
      ARROW_ASSIGN_OR_RAISE(auto scattered,
                            Take(column, take_indices, take_options, ctx_));
      return scattered.make_array();
    };
    return MergeColumns(std::move(other.columns_), std::move(scatter_column));
  }

  Result<ArrayVector> Finalize() {
    // Ensure that columns are allocated even if num_groups_ == 0
    RETURN_NOT_OK(ResizeColumns(num_groups_));
    return std::move(columns_);
  }

 protected:
  Status ResizeColumns(int64_t new_num_groups) {
    if (new_num_groups == num_groups_ && num_groups_ != 0) {
      return Status::OK();
    }
    ARROW_ASSIGN_OR_RAISE(
        auto array_suffix,
        MakeArrayOfNull(value_type_, new_num_groups - num_groups_, ctx_->memory_pool()));
    for (auto& column : columns_) {
      if (num_groups_ != 0) {
        DCHECK_NE(column, nullptr);
        ARROW_ASSIGN_OR_RAISE(
            column, Concatenate({std::move(column), array_suffix}, ctx_->memory_pool()));
      } else {
        column = array_suffix;
      }
      DCHECK_EQ(column->length(), new_num_groups);
    }
    num_groups_ = new_num_groups;
    return Status::OK();
  }

  using ColumnTransform =
      std::function<Result<std::shared_ptr<Array>>(const std::shared_ptr<Array>&)>;

  Status MergeColumns(ArrayVector&& other_columns,
                      const ColumnTransform& transform = {}) {
    DCHECK_EQ(columns_.size(), other_columns.size());
    for (int i = 0; i < num_keys_; ++i) {
      if (other_columns[i]) {
        RETURN_NOT_OK(MergeColumn(&columns_[i], std::move(other_columns[i]), transform));
      }
    }
    return Status::OK();
  }

  Status MergeColumn(std::shared_ptr<Array>* column, std::shared_ptr<Array> other_column,
                     const ColumnTransform& transform = {}) {
    if (other_column->null_count() == other_column->length()) {
      // Avoid paying for the transform step below, since merging will be a no-op anyway.
      return Status::OK();
    }
    if (transform) {
      ARROW_ASSIGN_OR_RAISE(other_column, transform(other_column));
    }
    DCHECK_EQ(num_groups_, other_column->length());
    if (!*column) {
      *column = other_column;
      return Status::OK();
    }
    if ((*column)->null_count() == (*column)->length()) {
      *column = other_column;
      return Status::OK();
    }
    int64_t expected_non_nulls = (num_groups_ - (*column)->null_count()) +
                                 (num_groups_ - other_column->null_count());
    ARROW_ASSIGN_OR_RAISE(auto coalesced,
                          CallFunction("coalesce", {*column, other_column}, ctx_));
    // Check that all non-null values in other_column and column were kept in the result.
    if (expected_non_nulls != num_groups_ - coalesced.null_count()) {
      DCHECK_GT(expected_non_nulls, num_groups_ - coalesced.null_count());
      return DuplicateValue();
    }
    *column = coalesced.make_array();
    return Status::OK();
  }

  Status DuplicateValue() {
    return Status::Invalid(
        "Encountered more than one non-null value for the same grouped pivot key");
  }

  ExecContext* ctx_;
  std::shared_ptr<DataType> value_type_;
  int num_keys_;
  int64_t num_groups_;
  ArrayVector columns_;
  // A persistent scratch buffer to store the take indices in Consume
  BufferBuilder scratch_buffer_;
};

struct GroupedPivotImpl : public GroupedAggregator {
  Status Init(ExecContext* ctx, const KernelInitArgs& args) override {
    DCHECK_EQ(args.inputs.size(), 3);
    key_type_ = args.inputs[0].GetSharedPtr();
    options_ = checked_cast<const PivotWiderOptions*>(args.options);
    DCHECK_NE(options_, nullptr);
    auto value_type = args.inputs[1].GetSharedPtr();
    FieldVector fields;
    fields.reserve(options_->key_names.size());
    for (const auto& key_name : options_->key_names) {
      fields.push_back(field(key_name, value_type));
    }
    out_type_ = struct_(std::move(fields));
    out_struct_type_ = checked_cast<const StructType*>(out_type_.get());
    ARROW_ASSIGN_OR_RAISE(key_mapper_,
                          PivotWiderKeyMapper::Make(*key_type_, options_, ctx));
    RETURN_NOT_OK(accumulator_.Init(ctx, value_type, options_));
    return Status::OK();
  }

  Status Resize(int64_t new_num_groups) override {
    num_groups_ = new_num_groups;
    return accumulator_.Resize(new_num_groups);
  }

  Status Merge(GroupedAggregator&& raw_other,
               const ArrayData& group_id_mapping) override {
    auto other = checked_cast<GroupedPivotImpl*>(&raw_other);
    return accumulator_.Merge(std::move(other->accumulator_), group_id_mapping);
  }

  Status Consume(const ExecSpan& batch) override {
    DCHECK_EQ(batch.values.size(), 3);
    auto groups = batch[2].array.GetSpan<const uint32_t>(1, batch.length);
    if (!batch[1].is_array()) {
      return Status::NotImplemented("Consuming scalar pivot value");
    }
    if (batch[0].is_array()) {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ArrayData> keys,
                            key_mapper_->MapKeys(batch[0].array));
      return accumulator_.Consume(groups, keys, batch[1].array);
    } else {
      ARROW_ASSIGN_OR_RAISE(std::optional<PivotWiderKeyIndex> key,
                            key_mapper_->MapKey(*batch[0].scalar));
      return accumulator_.Consume(groups, key, batch[1].array);
    }
  }

  Result<Datum> Finalize() override {
    ARROW_ASSIGN_OR_RAISE(auto columns, accumulator_.Finalize());
    DCHECK_EQ(columns.size(), static_cast<size_t>(out_struct_type_->num_fields()));
    return std::make_shared<StructArray>(out_type_, num_groups_, std::move(columns),
                                         /*null_bitmap=*/nullptr,
                                         /*null_count=*/0);
  }

  std::shared_ptr<DataType> out_type() const override { return out_type_; }

  std::shared_ptr<DataType> key_type_;
  std::shared_ptr<DataType> out_type_;
  const StructType* out_struct_type_;
  const PivotWiderOptions* options_;
  std::unique_ptr<PivotWiderKeyMapper> key_mapper_;
  GroupedPivotAccumulator accumulator_;
  int64_t num_groups_ = 0;
};

// ----------------------------------------------------------------------
// Docstrings

const FunctionDoc hash_pivot_doc{
    "Pivot values according to a pivot key column",
    ("Output is a struct array with as many fields as `PivotWiderOptions.key_names`.\n"
     "All output struct fields have the same type as `pivot_values`.\n"
     "Each pivot key decides in which output field the corresponding pivot value\n"
     "is emitted. If a pivot key doesn't appear in a given group, null is emitted.\n"
     "If more than one non-null value is encountered in the same group for a\n"
     "given pivot key, Invalid is raised.\n"
     "The pivot key column can be string, binary or integer. The `key_names`\n"
     "will be cast to the pivot key column type for matching.\n"
     "Behavior of unexpected pivot keys is controlled by `unexpected_key_behavior`\n"
     "in PivotWiderOptions."),
    {"pivot_keys", "pivot_values", "group_id_array"},
    "PivotWiderOptions"};

}  // namespace

void RegisterHashAggregatePivot(FunctionRegistry* registry) {
  static const auto default_pivot_options = PivotWiderOptions::Defaults();

  {
    auto func = std::make_shared<HashAggregateFunction>(
        "hash_pivot_wider", Arity::Ternary(), hash_pivot_doc, &default_pivot_options);
    auto add_kernel = [&](InputType type) {
      // Anything that scatter() (i.e. take()) accepts can be passed as values
      auto sig = KernelSignature::Make({type, InputType::Any(), InputType(Type::UINT32)},
                                       OutputType(ResolveGroupOutputType));
      DCHECK_OK(func->AddKernel(
          MakeKernel(std::move(sig), HashAggregateInit<GroupedPivotImpl>)));
    };
    for (const auto& key_type : BaseBinaryTypes()) {
      add_kernel(key_type->id());
    }
    for (const auto& key_type : IntTypes()) {
      add_kernel(key_type->id());
    }
    add_kernel(Type::FIXED_SIZE_BINARY);
    DCHECK_OK(registry->AddFunction(std::move(func)));
  }
}

}  // namespace arrow::compute::internal
