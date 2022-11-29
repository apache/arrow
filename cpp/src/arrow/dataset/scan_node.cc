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

#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec/util.h"
#include "arrow/dataset/scanner.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/tracing_internal.h"
#include "arrow/util/unreachable.h"

namespace cp = arrow::compute;

namespace arrow {

using internal::checked_cast;

namespace dataset {

namespace {

Result<std::shared_ptr<Schema>> OutputSchemaFromOptions(const ScanV2Options& options) {
  return FieldPath::GetAll(*options.dataset->schema(), options.columns);
}

// In the future we should support async scanning of fragments.  The
// Dataset class doesn't support this yet but we pretend it does here to
// ease future adoption of the feature.
AsyncGenerator<std::shared_ptr<Fragment>> GetFragments(Dataset* dataset,
                                                       cp::Expression predicate) {
  // In the future the dataset should be responsible for figuring out
  // the I/O context.  This will allow different I/O contexts to be used
  // when scanning different datasets.  For example, if we are scanning a
  // union of a remote dataset and a local dataset.
  const auto& io_context = io::default_io_context();
  auto io_executor = io_context.executor();
  Future<std::shared_ptr<FragmentIterator>> fragments_it_fut =
      DeferNotOk(io_executor->Submit(
          [dataset, predicate]() -> Result<std::shared_ptr<FragmentIterator>> {
            ARROW_ASSIGN_OR_RAISE(FragmentIterator fragments_iter,
                                  dataset->GetFragments(predicate));
            return std::make_shared<FragmentIterator>(std::move(fragments_iter));
          }));
  Future<AsyncGenerator<std::shared_ptr<Fragment>>> fragments_gen_fut =
      fragments_it_fut.Then([](const std::shared_ptr<FragmentIterator>& fragments_it)
                                -> Result<AsyncGenerator<std::shared_ptr<Fragment>>> {
        ARROW_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<Fragment>> fragments,
                              fragments_it->ToVector());
        return MakeVectorGenerator(std::move(fragments));
      });
  return MakeFromFuture(std::move(fragments_gen_fut));
}

/// \brief A node that scans a dataset
///
/// The scan node has three groups of io-tasks and one task.
///
/// The first io-task (listing) fetches the fragments from the dataset.  This may be a
/// simple iteration of paths or, if the dataset is described with wildcards, this may
/// involve I/O for listing and walking directory paths.  There is one listing io-task per
/// dataset.
///
/// Ths next step is to fetch the metadata for the fragment.  For some formats (e.g. CSV)
/// this may be quite simple (get the size of the file).  For other formats (e.g. parquet)
/// this is more involved and requires reading data.  There is one metadata io-task per
/// fragment.  The metadata io-task creates an AsyncGenerator<RecordBatch> from the
/// fragment.
///
/// Once the metadata io-task is done we can issue read io-tasks.  Each read io-task
/// requests a single batch of data from the disk by pulling the next Future from the
/// generator.
///
/// Finally, when the future is fulfilled, we issue a pipeline task to drive the batch
/// through the pipeline.
///
/// Most of these tasks are io-tasks.  They take very few CPU resources and they run on
/// the I/O thread pool.  These io-tasks are invisible to the exec plan and so we need to
/// do some custom scheduling.  We limit how many fragments we read from at any one time.
/// This is referred to as "fragment readahead".
///
/// Within a fragment there is usually also some amount of "row readahead".  This row
/// readahead is handled by the fragment (and not the scanner) because the exact details
/// of how it is performed depend on the underlying format.
///
/// When a scan node is aborted (StopProducing) we send a cancel signal to any active
/// fragments.  On destruction we continue consuming the fragments until they complete
/// (which should be fairly quick since we cancelled the fragment).  This ensures the
/// I/O work is completely finished before the node is destroyed.
class ScanNode : public cp::ExecNode {
 public:
  ScanNode(cp::ExecPlan* plan, ScanV2Options options,
           std::shared_ptr<Schema> output_schema)
      : cp::ExecNode(plan, {}, {}, std::move(output_schema),
                     /*num_outputs=*/1),
        options_(options) {}

  static Result<ScanV2Options> NormalizeAndValidate(const ScanV2Options& options,
                                                    compute::ExecContext* ctx) {
    ScanV2Options normalized(options);
    if (!normalized.dataset) {
      return Status::Invalid("Scan options must include a dataset");
    }

    if (options.fragment_readahead < 0) {
      return Status::Invalid(
          "Fragment readahead may not be less than 0.  Set to 0 to disable readahead");
    }

    if (options.target_bytes_readahead < 0) {
      return Status::Invalid(
          "Batch readahead may not be less than 0.  Set to 0 to disable readahead");
    }

    if (!normalized.filter.is_valid()) {
      normalized.filter = compute::literal(true);
    }

    if (normalized.filter.call() && normalized.filter.IsBound()) {
      // There is no easy way to make sure a filter was bound agaisnt the same
      // function registry as the one in ctx so we just require it to be unbound
      // FIXME - Do we care if it was bound to a different function registry?
      return Status::Invalid("Scan filter must be unbound");
    } else if (!normalized.filter.IsBound()) {
      ARROW_ASSIGN_OR_RAISE(normalized.filter,
                            normalized.filter.Bind(*options.dataset->schema(), ctx));
    }  // Else we must have some simple filter like literal(true) which might be bound
       // but we don't care

    return std::move(normalized);
  }

  static Result<cp::ExecNode*> Make(cp::ExecPlan* plan, std::vector<cp::ExecNode*> inputs,
                                    const cp::ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 0, "ScanNode"));
    const auto& scan_options = checked_cast<const ScanV2Options&>(options);
    ARROW_ASSIGN_OR_RAISE(ScanV2Options normalized_options,
                          NormalizeAndValidate(scan_options, plan->exec_context()));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Schema> output_schema,
                          OutputSchemaFromOptions(normalized_options));
    return plan->EmplaceNode<ScanNode>(plan, std::move(normalized_options),
                                       std::move(output_schema));
  }

  const char* kind_name() const override { return "ScanNode"; }

  [[noreturn]] static void NoInputs() {
    Unreachable("no inputs; this should never be called");
  }
  [[noreturn]] void InputReceived(cp::ExecNode*, cp::ExecBatch) override { NoInputs(); }
  [[noreturn]] void ErrorReceived(cp::ExecNode*, Status) override { NoInputs(); }
  [[noreturn]] void InputFinished(cp::ExecNode*, int) override { NoInputs(); }

  Status Init() override { return Status::OK(); }

  struct ScanState {
    std::mutex mutex;
    std::shared_ptr<FragmentScanner> fragment_scanner;
    std::unique_ptr<FragmentEvolutionStrategy> fragment_evolution;
    FragmentScanRequest scan_request;
  };

  struct ScanBatchTask : public util::AsyncTaskScheduler::Task {
    ScanBatchTask(ScanNode* node, ScanState* scan_state, int batch_index)
        : node_(node), scan_(scan_state), batch_index_(batch_index) {
      int64_t cost = scan_state->fragment_scanner->EstimatedDataBytes(batch_index_);
      // It's possible, though probably a bad idea, for a single batch of a fragment
      // to be larger than 2GiB.  In that case, it doesn't matter much if we underestimate
      // because the largest the throttle can be is 2GiB and thus we will be in "one batch
      // at a time" mode anyways which is the best we can do in this case.
      cost_ = static_cast<int>(
          std::min(cost, static_cast<int64_t>(std::numeric_limits<int>::max())));
    }

    Result<Future<>> operator()() override {
      // Prevent concurrent calls to ScanBatch which might not be thread safe
      std::lock_guard<std::mutex> lk(scan_->mutex);
      return scan_->fragment_scanner->ScanBatch(batch_index_)
          .Then([this](const std::shared_ptr<RecordBatch>& batch) {
            return HandleBatch(batch);
          });
    }

    Status HandleBatch(const std::shared_ptr<RecordBatch>& batch) {
      ARROW_ASSIGN_OR_RAISE(
          compute::ExecBatch evolved_batch,
          scan_->fragment_evolution->EvolveBatch(batch, node_->options_.columns,
                                                 scan_->scan_request.columns));
      return node_->plan_->ScheduleTask(
          [node = node_, evolved_batch = std::move(evolved_batch)] {
            node->outputs_[0]->InputReceived(node, std::move(evolved_batch));
            return Status::OK();
          });
    }

    int cost() const override { return cost_; }

    ScanNode* node_;
    ScanState* scan_;
    int batch_index_;
    int cost_;
  };

  struct ListFragmentTask : util::AsyncTaskScheduler::Task {
    ListFragmentTask(ScanNode* node, std::shared_ptr<Fragment> fragment)
        : node(node), fragment(std::move(fragment)) {}

    Result<Future<>> operator()() override {
      return fragment->InspectFragment().Then(
          [this](const std::shared_ptr<InspectedFragment>& inspected_fragment) {
            return BeginScan(inspected_fragment);
          });
    }

    Future<> BeginScan(const std::shared_ptr<InspectedFragment>& inspected_fragment) {
      // Now that we have an inspected fragment we need to use the dataset's evolution
      // strategy to figure out how to scan it
      scan_state->fragment_evolution =
          node->options_.dataset->evolution_strategy()->GetStrategy(
              *node->options_.dataset, *fragment, *inspected_fragment);
      ARROW_RETURN_NOT_OK(InitFragmentScanRequest());
      return fragment->BeginScan(scan_state->scan_request, *inspected_fragment)
          .Then([this](const std::shared_ptr<FragmentScanner>& fragment_scanner) {
            return AddScanTasks(fragment_scanner);
          });
    }

    Future<> AddScanTasks(const std::shared_ptr<FragmentScanner>& fragment_scanner) {
      scan_state->fragment_scanner = fragment_scanner;
      ScanState* state_view = scan_state.get();
      Future<> list_and_scan_done = Future<>::Make();
      // Finish callback keeps the scan state alive until all scan tasks done
      struct StateHolder {
        Status operator()() {
          list_and_scan_done.MarkFinished();
          return Status::OK();
        }
        Future<> list_and_scan_done;
        std::unique_ptr<ScanState> scan_state;
      };

      std::unique_ptr<util::AsyncTaskGroup> scan_tasks = util::AsyncTaskGroup::Make(
          node->batches_throttle_.get(),
          StateHolder{list_and_scan_done, std::move(scan_state)});
      for (int i = 0; i < fragment_scanner->NumBatches(); i++) {
        node->num_batches_.fetch_add(1);
        scan_tasks->AddTask(std::make_unique<ScanBatchTask>(node, state_view, i));
      }
      return Status::OK();
      // The "list fragments" task doesn't actually end until the fragments are
      // all scanned.  This allows us to enforce fragment readahead.
      return list_and_scan_done;
    }

    // Take the dataset options, and the fragment evolution, and figure out exactly how
    // we should scan the fragment itself.
    Status InitFragmentScanRequest() {
      ARROW_ASSIGN_OR_RAISE(
          scan_state->scan_request.columns,
          scan_state->fragment_evolution->DevolveSelection(node->options_.columns));
      ARROW_ASSIGN_OR_RAISE(
          compute::Expression devolution_guarantee,
          scan_state->fragment_evolution->GetGuarantee(node->options_.columns));
      ARROW_ASSIGN_OR_RAISE(
          compute::Expression simplified_filter,
          compute::SimplifyWithGuarantee(node->options_.filter, devolution_guarantee));
      ARROW_ASSIGN_OR_RAISE(
          scan_state->scan_request.filter,
          scan_state->fragment_evolution->DevolveFilter(std::move(simplified_filter)));
      scan_state->scan_request.format_scan_options = node->options_.format_options;
      return Status::OK();
    }

    ScanNode* node;
    std::shared_ptr<Fragment> fragment;
    std::unique_ptr<ScanState> scan_state = std::make_unique<ScanState>();
  };

  Status StartProducing() override {
    START_COMPUTE_SPAN(span_, std::string(kind_name()) + ":" + label(),
                       {{"node.kind", kind_name()},
                        {"node.label", label()},
                        {"node.output_schema", output_schema()->ToString()},
                        {"node.detail", ToString()}});
    END_SPAN_ON_FUTURE_COMPLETION(span_, finished_);
    batches_throttle_ = util::ThrottledAsyncTaskScheduler::Make(
        plan_->async_scheduler(), options_.target_bytes_readahead + 1);
    AsyncGenerator<std::shared_ptr<Fragment>> frag_gen =
        GetFragments(options_.dataset.get(), options_.filter);
    std::shared_ptr<util::AsyncTaskScheduler> fragment_tasks =
        util::MakeThrottledAsyncTaskGroup(
            plan_->async_scheduler(), options_.fragment_readahead + 1, /*queue=*/nullptr,
            [this]() {
              outputs_[0]->InputFinished(this, num_batches_.load());
              finished_.MarkFinished();
              return Status::OK();
            });
    fragment_tasks->AddAsyncGenerator<std::shared_ptr<Fragment>>(
        std::move(frag_gen), [this, fragment_tasks = std::move(fragment_tasks)](
                                 const std::shared_ptr<Fragment>& fragment) {
          fragment_tasks->AddTask(std::make_unique<ListFragmentTask>(this, fragment));
          return Status::OK();
        });
    return Status::OK();
  }

  void PauseProducing(ExecNode* output, int32_t counter) override {
    // TODO(ARROW-17755)
  }

  void ResumeProducing(ExecNode* output, int32_t counter) override {
    // TODO(ARROW-17755)
  }

  void StopProducing(ExecNode* output) override {
    DCHECK_EQ(output, outputs_[0]);
    StopProducing();
  }

  void StopProducing() override {}

 private:
  ScanV2Options options_;
  std::atomic<int> num_batches_{0};
  std::shared_ptr<util::ThrottledAsyncTaskScheduler> batches_throttle_;
};

}  // namespace

namespace internal {
void InitializeScannerV2(arrow::compute::ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("scan2", ScanNode::Make));
}
}  // namespace internal
}  // namespace dataset
}  // namespace arrow
