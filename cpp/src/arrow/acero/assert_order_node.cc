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

#include <arrow/compute/kernels/vector_sort_internal.h>
#include <parquet/types.h>
#include <sstream>

#include "arrow/acero/accumulation_queue.h"
#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/query_context.h"
#include "arrow/acero/util.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {

using internal::checked_cast;

namespace acero {
namespace {

// Compare two records in a single column (either from a batch or table)
struct ColumnComparator {
  ColumnComparator(const compute::NullPlacement& null_placement,
                   const compute::SortOrder sort_order)
      : null_placement(null_placement),
        sort_order(sort_order),
        left_is_null_order(null_placement == compute::NullPlacement::AtStart ? -1 : 1),
        right_is_null_order(null_placement == compute::NullPlacement::AtStart ? 1 : -1) {}

  virtual ~ColumnComparator() = default;

  virtual int Compare(const Array& column, const int64_t& left,
                      const int64_t& right) const = 0;

  compute::NullPlacement null_placement;
  compute::SortOrder sort_order;
  int left_is_null_order;
  int right_is_null_order;
};

template <typename Type>
struct ConcreteColumnComparator final : public ColumnComparator {
  using ColumnComparator::ColumnComparator;

  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using GetView = compute::internal::GetViewType<Type>;

  int Compare(const Array& column, const int64_t& left,
              const int64_t& right) const override {
    const ArrayType& array = ::arrow::internal::checked_cast<const ArrayType&>(column);

    const auto left_value = GetView::LogicalValue(array.GetView(left));
    const auto right_value = GetView::LogicalValue(array.GetView(right));

    auto left_is_null = column.IsNull(left);
    auto right_is_null = column.IsNull(right);

    auto orientation = sort_order == compute::SortOrder::Ascending ? 1 : -1;
    auto less = -1 * orientation;
    auto larger = 1 * orientation;

    if (left_is_null && right_is_null) {
      // both values are null (equal)
      return 0;
    } else if (left_is_null) {
      // left value is null
      return left_is_null_order;
    } else if (right_is_null) {
      // right value is null
      return right_is_null_order;
    } else if (left_value < right_value) {
      return less;
    } else if (left_value > right_value) {
      return larger;
    }

    return 0;
  }
};

template <>
struct ConcreteColumnComparator<NullType> : public ColumnComparator {
  using ColumnComparator::ColumnComparator;

  int Compare(const Array& column, const int64_t& left,
              const int64_t& right) const override {
    return 0;
  }
};

class AssertOrderNode : public ExecNode,
                        public TracedNode,
                        util::SequencingQueue::Processor {
 public:
  AssertOrderNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
                  std::shared_ptr<Schema> output_schema, const Ordering& ordering)
      : ExecNode(plan, std::move(inputs), {"input"}, std::move(output_schema)),
        TracedNode(this),
        ordering_(ordering),
        comparators_(MakeComparators(output_schema_, ordering_)),
        sort_ascs_(MakeSortAscs(ordering.sort_keys())),
        sequencing_queue_(util::SequencingQueue::Make(this)) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "AssertOrderNode"));

    const auto& assert_options = checked_cast<const AssertOrderNodeOptions&>(options);

    Ordering ordering = assert_options.ordering;

    // check output ordering
    if (ordering.is_implicit() || ordering.is_unordered()) {
      return Status::Invalid("`ordering` must be explicit");
    }

    std::shared_ptr<Schema> output_schema = inputs[0]->output_schema();

    // check sort keys exist in schema
    for (const auto& key : ordering.sort_keys()) {
      ARROW_ASSIGN_OR_RAISE(auto res, key.target.GetOneOrNone(*output_schema));
      ARROW_CHECK_NE(res, nullptr);
    }

    return plan->EmplaceNode<AssertOrderNode>(plan, std::move(inputs),
                                              std::move(output_schema), ordering);
  }

  const char* kind_name() const override { return "AssertOrderNode"; }

  const Ordering& ordering() const override { return ordering_; }

  Status Validate() const override {
    ARROW_RETURN_NOT_OK(ExecNode::Validate());
    if (inputs_[0]->ordering().is_unordered()) {
      return Status::Invalid(
          "Assert order node's input has no meaningful ordering and so asserting some "
          "order "
          "will be non-deterministic.  Please establish order in some way (e.g. by using "
          "a "
          "source node with implicit ordering)");
    }
    return Status::OK();
  }

  Status StartProducing() override {
    NoteStartProducing(ToStringExtra());
    return Status::OK();
  }

  void PauseProducing(ExecNode* output, int32_t counter) override {
    inputs_[0]->PauseProducing(this, counter);
  }

  void ResumeProducing(ExecNode* output, int32_t counter) override {
    inputs_[0]->ResumeProducing(this, counter);
  }

  Status StopProducingImpl() override { return Status::OK(); }

  Status InputReceived(ExecNode* input, ExecBatch batch) override {
    auto scope = TraceInputReceived(batch);
    DCHECK_EQ(input, inputs_[0]);

    // assert order inside batch
    ARROW_ASSIGN_OR_RAISE(
        const auto record_batch,
        batch.ToRecordBatch(output_schema_, plan_->query_context()->memory_pool()));
    ARROW_RETURN_NOT_OK(AssertInBatchOrder(*record_batch, ordering_, comparators_));

    // queue batch to assert order between that batch and its predecessor
    return sequencing_queue_->InsertBatch(std::move(batch));
  }

  Status InputFinished(ExecNode* input, int total_batches) override {
    DCHECK_EQ(input, inputs_[0]);
    EVENT_ON_CURRENT_SPAN("InputFinished", {{"batches.length", total_batches}});
    return output_->InputFinished(this, total_batches);
  }

  Result<std::optional<util::SequencingQueue::Task>> Process(ExecBatch batch) override {
    // check this batch is in order with previous batch, skip entirely for emtpy batches
    if (batch.length > 0) {
      // extract sort values of current batch
      auto current_record_batch =
          batch.ToRecordBatch(output_schema_, plan_->query_context()->memory_pool())
              .ValueOrDie();

      // compare against previous sort values
      if (previous_sort_values_.has_value()) {
        // check batches are in order
        ARROW_ASSIGN_OR_RAISE(
            auto current_sort_values,
            ExtractSortKeys(*current_record_batch, 0, ordering_.sort_keys()));
        ARROW_RETURN_NOT_OK(AssertBatchOrder(previous_sort_values_.value(),
                                             current_sort_values, sort_ascs_));
      }

      // memorize last row of record batch for next batch
      ARROW_ASSIGN_OR_RAISE(
          previous_sort_values_,
          ExtractSortKeys(*current_record_batch, current_record_batch->num_rows() - 1,
                          ordering_.sort_keys()));
    }

    // send current batch
    std::optional<util::SequencingQueue::Task> task_or_none =
        [this, batch = std::move(batch)]() mutable {
          ExecBatch batch_to_send = std::move(batch);
          return output_->InputReceived(this, std::move(batch_to_send));
        };

    return task_or_none;
  }

  void Schedule(util::SequencingQueue::Task task) override {
    plan_->query_context()->ScheduleTask(std::move(task),
                                         "AssertOrderNode::ProcessBatch");
  }

 protected:
  std::string ToStringExtra(int indent = 0) const override {
    std::stringstream ss;
    ss << "ordering=" << ordering_.ToString();
    return ss.str();
  }

  static Status AssertInBatchOrder(
      const RecordBatch& batch, const Ordering& ordering,
      const std::vector<std::unique_ptr<ColumnComparator>>& comparators) {
    auto sort_keys = ordering.sort_keys();
    ARROW_CHECK_EQ(comparators.size(), sort_keys.size());

    // trivial case: batch is empty or only a single row
    if (batch.num_rows() <= 1) {
      return Status::OK();
    }

    // extract the sort columns
    std::vector<std::shared_ptr<Array>> sort_columns(sort_keys.size());
    for (size_t i = 0; i < sort_keys.size(); ++i) {
      auto& key = sort_keys[i];
      ARROW_ASSIGN_OR_RAISE(sort_columns[i], key.target.GetOneOrNone(batch));
    }

    // assert order of sort columns
    for (int64_t row = 1; row < batch.num_rows(); ++row) {
      for (size_t col = 0; col < sort_keys.size(); ++col) {
        auto& sort_column = sort_columns[col];
        auto& comparator = comparators[col];

        auto result = comparator->Compare(*sort_column, row - 1, row);
        if (result < 0) {
          // order asserted, no more keys need to be compared, move to next row
          break;
        }
        if (result > 0) {
          // order mismatch
          return Status::ExecutionError("Data is not ordered");
        }
      }
    }

    return Status::OK();
  }

  static Status AssertBatchOrder(
      const std::vector<std::shared_ptr<Scalar>>& previous_sort_scalars,
      const std::vector<std::shared_ptr<Scalar>>& sort_scalars,
      const std::vector<bool>& sort_ascs) {
    ARROW_CHECK_EQ(previous_sort_scalars.size(), sort_scalars.size());
    ARROW_CHECK_EQ(sort_scalars.size(), sort_ascs.size());
    for (size_t i = 0; i < sort_scalars.size(); ++i) {
      const auto& previous_sort_scalar = previous_sort_scalars[i];
      const auto& sort_scalar = sort_scalars[i];
      const auto& sort_asc = sort_ascs[i];

      if (ScalarLess(*previous_sort_scalar, *sort_scalar)) {
        if (sort_asc) {
          return Status::OK();
        }
        return Status::ExecutionError("Data is not ordered");
      }
      if (ScalarLess(*sort_scalar, *previous_sort_scalar)) {
        if (!sort_asc) {
          return Status::OK();
        }
        return Status::ExecutionError("Data is not ordered");
      }
    }
    return Status::OK();
  }

  static Result<std::vector<std::shared_ptr<Scalar>>> ExtractSortKeys(
      const RecordBatch& batch, const int64_t& row,
      const std::vector<compute::SortKey>& sort_keys) {
    DCHECK_GE(row, 0);
    DCHECK_LE(row, batch.num_rows());
    std::vector<std::shared_ptr<Scalar>> sort_scalars(sort_keys.size());
    for (size_t i = 0; i < sort_keys.size(); ++i) {
      auto& key = sort_keys[i];
      ARROW_ASSIGN_OR_RAISE(const auto array, key.target.GetOneOrNone(batch));
      ARROW_ASSIGN_OR_RAISE(sort_scalars[i], array->GetScalar(row));
    }
    return sort_scalars;
  }

 private:
  Ordering ordering_;
  std::vector<std::unique_ptr<ColumnComparator>> comparators_;
  std::vector<bool> sort_ascs_;
  std::optional<std::vector<std::shared_ptr<Scalar>>> previous_sort_values_ =
      std::nullopt;
  std::unique_ptr<util::SequencingQueue> sequencing_queue_;

  static std::vector<std::unique_ptr<ColumnComparator>> MakeComparators(
      const std::shared_ptr<Schema>& schema, const Ordering& ordering) {
    ARROW_CHECK(schema);
    std::vector<std::unique_ptr<ColumnComparator>> comparators(
        ordering.sort_keys().size());
    for (size_t i = 0; i < ordering.sort_keys().size(); ++i) {
      auto& sort_key = ordering.sort_keys()[i];
      auto factory = ColumnComparatorFactory{ordering.null_placement(), sort_key.order};
      auto field_path = sort_key.target.FindOne(*schema).ValueOrDie();
      auto field = field_path.Get(*schema).ValueOrDie();
      comparators[i] = factory.Create(*field->type()).ValueOrDie();
    }
    return comparators;
  }

  static std::vector<bool> MakeSortAscs(const Ordering& ordering) {
    std::vector<bool> sort_ascs(ordering.sort_keys().size());
    for (size_t i = 0; i < ordering.sort_keys().size(); ++i) {
      auto& sort_key = ordering.sort_keys()[i];
      sort_ascs[i] = sort_key.order == compute::SortOrder::Ascending;
    }
    return sort_ascs;
  }

  struct ColumnComparatorFactory {
    Result<std::unique_ptr<ColumnComparator>> Create(const DataType& type) {
      RETURN_NOT_OK(VisitTypeInline(type, this));
      return std::move(res);
    }

#define VISIT(TYPE) \
  Status Visit(const TYPE& type) { return VisitGeneric(type); }

    VISIT_SORTABLE_PHYSICAL_TYPES(VISIT)
    VISIT(NullType)

#undef VISIT

    Status Visit(const DataType& type) {
      return Status::TypeError("Unsupported type for asserting order: ", type.ToString());
    }

    template <typename Type>
    Status VisitGeneric(const Type& type) {
      res.reset(new ConcreteColumnComparator<Type>{null_placement, sort_order});
      return Status::OK();
    }

    const compute::NullPlacement null_placement;
    const compute::SortOrder sort_order;
    std::unique_ptr<ColumnComparator> res = nullptr;
  };
};

}  // namespace

namespace internal {

void RegisterAssertOrderNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory(std::string(AssertOrderNodeOptions::kName),
                                 AssertOrderNode::Make));
}

}  // namespace internal
}  // namespace acero
}  // namespace arrow
