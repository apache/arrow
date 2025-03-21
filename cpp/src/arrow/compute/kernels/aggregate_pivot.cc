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

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/kernels/pivot_internal.h"
#include "arrow/scalar.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/logging.h"

namespace arrow::compute::internal {
namespace {

using arrow::internal::VisitSetBitRunsVoid;
using arrow::util::span;

struct PivotImpl : public ScalarAggregator {
  Status Init(const PivotWiderOptions& options, const std::vector<TypeHolder>& in_types) {
    options_ = &options;
    key_type_ = in_types[0].GetSharedPtr();
    auto value_type = in_types[1].GetSharedPtr();
    FieldVector fields;
    fields.reserve(options_->key_names.size());
    values_.reserve(options_->key_names.size());
    for (const auto& key_name : options_->key_names) {
      fields.push_back(field(key_name, value_type));
      values_.push_back(MakeNullScalar(value_type));
    }
    out_type_ = struct_(std::move(fields));
    ARROW_ASSIGN_OR_RAISE(key_mapper_, PivotWiderKeyMapper::Make(*key_type_, options_));
    return Status::OK();
  }

  Status Consume(KernelContext*, const ExecSpan& batch) override {
    DCHECK_EQ(batch.num_values(), 2);
    if (batch[0].is_array()) {
      ARROW_ASSIGN_OR_RAISE(span<const PivotWiderKeyIndex> keys,
                            key_mapper_->MapKeys(batch[0].array));
      if (batch[1].is_array()) {
        // Array keys, array values
        auto values = batch[1].array.ToArray();
        for (int64_t i = 0; i < batch.length; ++i) {
          PivotWiderKeyIndex key = keys[i];
          if (key != kNullPivotKey && !values->IsNull(i)) {
            if (ARROW_PREDICT_FALSE(values_[key]->is_valid)) {
              return DuplicateValue();
            }
            ARROW_ASSIGN_OR_RAISE(values_[key], values->GetScalar(i));
            DCHECK(values_[key]->is_valid);
          }
        }
      } else {
        // Array keys, scalar value
        const Scalar* value = batch[1].scalar;
        if (value->is_valid) {
          for (int64_t i = 0; i < batch.length; ++i) {
            PivotWiderKeyIndex key = keys[i];
            if (key != kNullPivotKey) {
              if (ARROW_PREDICT_FALSE(values_[key]->is_valid)) {
                return DuplicateValue();
              }
              values_[key] = value->GetSharedPtr();
            }
          }
        }
      }
    } else {
      ARROW_ASSIGN_OR_RAISE(PivotWiderKeyIndex key,
                            key_mapper_->MapKey(*batch[0].scalar));
      if (key != kNullPivotKey) {
        if (batch[1].is_array()) {
          // Scalar key, array values
          auto values = batch[1].array.ToArray();
          for (int64_t i = 0; i < batch.length; ++i) {
            if (!values->IsNull(i)) {
              if (ARROW_PREDICT_FALSE(values_[key]->is_valid)) {
                return DuplicateValue();
              }
              ARROW_ASSIGN_OR_RAISE(values_[key], values->GetScalar(i));
              DCHECK(values_[key]->is_valid);
            }
          }
        } else {
          // Scalar key, scalar value
          const Scalar* value = batch[1].scalar;
          if (value->is_valid) {
            if (batch.length > 1 || values_[key]->is_valid) {
              return DuplicateValue();
            }
            values_[key] = value->GetSharedPtr();
          }
        }
      }
    }
    return Status::OK();
  }

  Status MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other_state = checked_cast<const PivotImpl&>(src);
    for (int64_t key = 0; key < static_cast<int64_t>(values_.size()); ++key) {
      if (other_state.values_[key]->is_valid) {
        if (ARROW_PREDICT_FALSE(values_[key]->is_valid)) {
          return DuplicateValue();
        }
        values_[key] = other_state.values_[key];
      }
    }
    return Status::OK();
  }

  Status Finalize(KernelContext* ctx, Datum* out) override {
    *out = std::make_shared<StructScalar>(std::move(values_), out_type_);
    return Status::OK();
  }

  Status DuplicateValue() {
    return Status::Invalid(
        "Encountered more than one non-null value for the same pivot key");
  }

  std::shared_ptr<DataType> out_type() const { return out_type_; }

  std::shared_ptr<DataType> key_type_;
  std::shared_ptr<DataType> out_type_;
  const PivotWiderOptions* options_;
  std::unique_ptr<PivotWiderKeyMapper> key_mapper_;
  ScalarVector values_;
};

Result<std::unique_ptr<KernelState>> PivotInit(KernelContext* ctx,
                                               const KernelInitArgs& args) {
  const auto& options = checked_cast<const PivotWiderOptions&>(*args.options);
  DCHECK_EQ(args.inputs.size(), 2);
  DCHECK(is_base_binary_like(args.inputs[0].id()));
  auto state = std::make_unique<PivotImpl>();
  RETURN_NOT_OK(state->Init(options, args.inputs));
  return std::unique_ptr<KernelState>(std::move(state));
}

Result<TypeHolder> ResolveOutputType(KernelContext* ctx, const std::vector<TypeHolder>&) {
  return checked_cast<PivotImpl*>(ctx->state())->out_type();
}

const FunctionDoc pivot_doc{
    "Pivot values according to a pivot key column",
    ("Output is a struct with as many fields as `PivotWiderOptions.key_names`.\n"
     "All output struct fields have the same type as `pivot_values`.\n"
     "Each pivot key decides in which output field the corresponding pivot value\n"
     "is emitted. If a pivot key doesn't appear, null is emitted.\n"
     "If more than one non-null value is encountered for a given pivot key,\n"
     "Invalid is raised.\n"
     "Behavior of unexpected pivot keys is controlled by `unexpected_key_behavior`\n"
     "in PivotWiderOptions."),
    {"pivot_keys", "pivot_values"},
    "PivotWiderOptions"};

}  // namespace

void RegisterScalarAggregatePivot(FunctionRegistry* registry) {
  static auto default_pivot_options = PivotWiderOptions::Defaults();

  auto func = std::make_shared<ScalarAggregateFunction>(
      "pivot_wider", Arity::Binary(), pivot_doc, &default_pivot_options);

  for (auto key_type : BaseBinaryTypes()) {
    auto sig = KernelSignature::Make({key_type->id(), InputType::Any()},
                                     OutputType(ResolveOutputType));
    AddAggKernel(std::move(sig), PivotInit, func.get());
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace arrow::compute::internal
