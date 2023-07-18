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
#include <future>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/query_context.h"
#include "arrow/acero/util.h"
#include "arrow/compute/expression.h"
#include "arrow/compute/expression_internal.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/scanner.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/async_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "arrow/util/tracing_internal.h"
#include "arrow/util/unreachable.h"

using namespace std::string_view_literals;  // NOLINT
using namespace std::placeholders;          // NOLINT

namespace arrow {

using internal::checked_cast;

namespace dataset {

namespace {

// How many inspection tasks we allow to run at the same time
constexpr int kNumConcurrentInspections = 4;
// How many scan tasks we need queued up to pause inspection
constexpr int kLimitQueuedScanTasks = 4;

// An interface for an object that knows all the details requires to launch a scan task
class ScanTaskLauncher {
 public:
  virtual ~ScanTaskLauncher() = default;
  virtual void LaunchTask(FragmentScanner* scanner, int scan_task_number,
                          int first_batch_index,
                          std::function<void(int num_batches)> task_complete_cb) = 0;
};

// When we have finished inspecting a fragment we can submit its scan tasks.  However,
// we only allow certain number of scan tasks at a time.  Ironically, a scan task is not
// actually a "task".  It's a collection of tasks that will run to scan the stream of
// batches.
//
// This class serves as a synchronous "throttle" which makes sure we aren't running too
// many scan tasks at once.  It also pauses the inspection process if we have a bunch
// of scan tasks ready to go.
//
// Lastly, it also serves as a sort of sequencing point where we hold off on launching
// a scan task until we know how many batches precede that scan task.
//
// There are two events.
//
// First, when a fragment finishes inspection, it is inserted into the queue (we might
// know how many batches are in the scan tasks)
//
//  * An entry is created for each scan task and added to the queue
//  * We check to see if any entries are free to run
//  * We may pause or resume the inspection throttle
//
// Second, when a scan task finishes it records that it is finished (at this point we
// definitely know how many batches were in the scan task)
//
//  * The task is removed from the queue
//  * We check to see if any entries are free to run
//  * We may resume the inspection throttle
class ScanTaskStagingArea {
 public:
  ScanTaskStagingArea(int queue_limit, int run_limit,
                      util::ThrottledAsyncTaskScheduler* inspection_throttle,
                      std::function<void()> completion_cb)
      : queue_limit_(queue_limit),
        run_limit_(run_limit),
        inspection_throttle_(inspection_throttle),
        completion_cb_(std::move(completion_cb)) {}

  void InsertInspectedFragment(std::shared_ptr<FragmentScanner> fragment_scanner,
                               int fragment_index,
                               std::unique_ptr<ScanTaskLauncher> task_launcher) {
    auto fragment_entry = std::make_unique<FragmentEntry>(
        std::move(fragment_scanner), fragment_index, std::move(task_launcher));

    std::lock_guard lg(mutex_);
    num_queued_scan_tasks_ += static_cast<int>(fragment_entry->scan_tasks.size());

    // Ordered insertion into linked list
    if (!root_) {
      root_ = std::move(fragment_entry);
      // fragment_entry.prev will be nullptr, which is correct
    } else {
      FragmentEntry* itr = root_.get();
      while (itr->next != nullptr && itr->next->fragment_index < fragment_index) {
        itr = itr->next.get();
      }
      if (itr->next != nullptr) {
        fragment_entry->next = std::move(itr->next);
        fragment_entry->next->prev = fragment_entry.get();
      }
      fragment_entry->prev = itr;
      itr->next = std::move(fragment_entry);
    }

    // Even if this isn't the first fragment it is still possible that there are tasks we
    // can now run.  For example, if we are allowed to run 4 scan tasks and the root only
    // had one.
    TryAndLaunchTasksUnlocked();

    if (num_queued_scan_tasks_ >= queue_limit_) {
      inspection_throttle_->Pause();
    }
  }

  void FinishedInsertingFragments(int num_fragments) {
    std::lock_guard lg(mutex_);
    total_num_fragments_ = num_fragments;
    if (num_fragments_processed_ == total_num_fragments_) {
      completion_cb_();
    }
  }

 private:
  struct ScanTaskEntry {
    int scan_task_index;
    int num_batches;
    bool launched;
  };
  struct FragmentEntry {
    FragmentEntry(std::shared_ptr<FragmentScanner> fragment_scanner, int fragment_index,
                  std::unique_ptr<ScanTaskLauncher> task_launcher)
        : fragment_scanner(std::move(fragment_scanner)),
          fragment_index(fragment_index),
          task_launcher(std::move(task_launcher)) {
      for (int i = 0; i < this->fragment_scanner->NumScanTasks(); i++) {
        ScanTaskEntry scan_task;
        scan_task.scan_task_index = i;
        scan_task.num_batches = this->fragment_scanner->NumBatchesInScanTask(i);
        scan_task.launched = false;
        scan_tasks.push_back(scan_task);
      }
    }

    std::shared_ptr<FragmentScanner> fragment_scanner;
    int fragment_index;
    std::unique_ptr<ScanTaskLauncher> task_launcher;

    std::unique_ptr<FragmentEntry> next;
    FragmentEntry* prev = nullptr;
    std::deque<ScanTaskEntry> scan_tasks;
  };

  void LaunchScanTaskUnlocked(FragmentEntry* entry, int scan_task_number,
                              int first_batch_index) {
    entry->task_launcher->LaunchTask(
        entry->fragment_scanner.get(), scan_task_number, first_batch_index,
        [this, entry, scan_task_number](int num_batches) {
          MarkScanTaskFinished(entry, scan_task_number, num_batches);
        });
    num_queued_scan_tasks_--;
    if (num_queued_scan_tasks_ < queue_limit_) {
      inspection_throttle_->Resume();
    }
    num_scan_tasks_running_++;
  }

  void TryAndLaunchTasksUnlocked() {
    if (!root_ || num_scan_tasks_running_ >= run_limit_) {
      return;
    }
    FragmentEntry* itr = root_.get();
    int num_preceding_batches = batches_completed_;
    while (itr != nullptr) {
      for (auto& scan_task : itr->scan_tasks) {
        if (!scan_task.launched) {
          scan_task.launched = true;
          LaunchScanTaskUnlocked(itr, scan_task.scan_task_index, num_preceding_batches);
          if (num_scan_tasks_running_ >= run_limit_) {
            // We've launched as many as we can
            return;
          }
        }
        if (scan_task.num_batches >= 0) {
          num_preceding_batches += scan_task.num_batches;
        } else {
          // A scan task is running that doesn't know how many batches it has.  We can't
          // proceed
          return;
        }
      }
      itr = itr->next.get();
    }
  }

  void MarkScanTaskFinished(FragmentEntry* fragment_entry, int scan_task_number,
                            int num_batches) {
    std::lock_guard lg(mutex_);
    batches_completed_ += num_batches;
    num_scan_tasks_running_--;
    auto itr = fragment_entry->scan_tasks.cbegin();
    std::size_t old_size = fragment_entry->scan_tasks.size();
    while (itr != fragment_entry->scan_tasks.cend()) {
      if (itr->scan_task_index == scan_task_number) {
        fragment_entry->scan_tasks.erase(itr);
        break;
      }
      itr++;
    }
    DCHECK_LT(fragment_entry->scan_tasks.size(), old_size);
    if (fragment_entry->scan_tasks.empty()) {
      FragmentEntry* prev = fragment_entry->prev;
      if (prev == nullptr) {
        // The current root has finished
        std::unique_ptr<FragmentEntry> new_root = std::move(root_->next);
        if (new_root != nullptr) {
          new_root->prev = nullptr;
        }
        // This next line will cause fragment_entry to be deleted
        root_ = std::move(new_root);
      } else {
        if (fragment_entry->next != nullptr) {
          // In this case a fragment in the middle finished
          std::unique_ptr<FragmentEntry> next = std::move(fragment_entry->next);
          next->prev = prev;
          // This next line will cause fragment_entry to be deleted
          prev->next = std::move(next);
        } else {
          // In this case a fragment at the end finished
          // This next line will cause fragment_entry to be deleted
          prev->next = nullptr;
        }
      }
      num_fragments_processed_++;
      if (num_fragments_processed_ == total_num_fragments_) {
        completion_cb_();
        return;
      }
    }
    TryAndLaunchTasksUnlocked();
  }

  int queue_limit_;
  int run_limit_;
  util::ThrottledAsyncTaskScheduler* inspection_throttle_;
  std::function<void()> completion_cb_;

  int num_queued_scan_tasks_ = 0;
  int num_scan_tasks_running_ = 0;
  int batches_completed_ = 0;
  std::unique_ptr<FragmentEntry> root_;
  std::mutex mutex_;
  int num_fragments_processed_ = 0;
  int total_num_fragments_ = -1;
};

Result<std::shared_ptr<Schema>> OutputSchemaFromOptions(const ScanV2Options& options) {
  return FieldPath::GetAll(*options.dataset->schema(), options.columns);
}

// In the future we should support async scanning of fragments.  The
// Dataset class doesn't support this yet but we pretend it does here to
// ease future adoption of the feature.
Future<AsyncGenerator<std::shared_ptr<Fragment>>> GetFragments(
    Dataset* dataset, compute::Expression predicate) {
  // In the future the dataset should be responsible for figuring out
  // the I/O context.  This will allow different I/O contexts to be used
  // when scanning different datasets.  For example, if we are scanning a
  // union of a remote dataset and a local dataset.
  const auto& io_context = io::default_io_context();
  auto io_executor = io_context.executor();
  return DeferNotOk(
             io_executor->Submit(
                 [dataset, predicate]() -> Result<std::shared_ptr<FragmentIterator>> {
                   ARROW_ASSIGN_OR_RAISE(FragmentIterator fragments_iter,
                                         dataset->GetFragments(predicate));
                   return std::make_shared<FragmentIterator>(std::move(fragments_iter));
                 }))
      .Then([](const std::shared_ptr<FragmentIterator>& fragments_it)
                -> Result<AsyncGenerator<std::shared_ptr<Fragment>>> {
        ARROW_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<Fragment>> fragments,
                              fragments_it->ToVector());
        return MakeVectorGenerator(std::move(fragments));
      });
}

/// \brief A node that scans a dataset
///
/// The scan node has three stages.
///
/// The first stage (listing) fetches the fragments from the dataset.  This may be a
/// simple iteration of paths or, if the dataset is described with wildcards, this may
/// involve I/O for listing and walking directory paths.  There is only one listing task.
///
/// Ths next step is to inspect the fragment.  At a minimum we need to know the names of
/// the columns in the fragment so that we can perform evolution.  This often involves
/// reading file metadata or the first block of the file (e.g. CSV / JSON). There is one
/// inspect task per fragment.
///
/// Once the inspect is done we can issue scan tasks.  The number of scan tasks created
/// will depend on the format and the file structure.  For example, CSV creates 1 scan
/// task per file.  Parquet creates one scan task per row group.
///
/// The creation of scan tasks is a partially sequenced operation.  We can start
/// inspecting fragments in parallel.  However, we cannot start scanning a fragment until
/// we know how many batches will come before that fragment.  This allows us to assign a
/// sequence number to scanned batches.
///
/// Each scan task is then broken up into a series of batch scans which scan a single
/// batch from the disk.  For example, in parquet, we might have a very large row group.
/// That single row group will have one scan task.  That scan task might generate hundreds
/// of batch scans.
///
/// Finally, when the batch scan is complete, we issue a pipeline task to drive the batch
/// through the plan.
///
/// Most of these tasks are I/O tasks.  They take very few CPU resources and they run on
/// the I/O thread pool.
///
/// In order to manage the disk load and our running memory requirements we limit the
/// number of scan tasks that run at any one time.
///
/// If a fragment has a guarantee we may use that expression to reduce the columns that
/// we need to load from disk.  For example, if the guarantee is x==7 then we don't need
/// to load the column x from disk and can instead populate x with the scalar 7.  If the
/// fragment on disk actually had a column x, and the value was not 7, then we will prefer
/// the guarantee in this invalid case.
///
/// When a scan node is aborted (StopProducing) we send a cancel signal to any active
/// fragments.  On destruction we continue consuming the fragments until they complete
/// (which should be fairly quick since we cancelled the fragment).  This ensures the
/// I/O work is completely finished before the node is destroyed.
class ScanNode : public acero::ExecNode, public acero::TracedNode {
 public:
  ScanNode(acero::ExecPlan* plan, ScanV2Options options,
           std::shared_ptr<Schema> output_schema)
      : acero::ExecNode(plan, {}, {}, std::move(output_schema)),
        acero::TracedNode(this),
        options_(std::move(options)) {}

  static Result<ScanV2Options> NormalizeAndValidate(const ScanV2Options& options,
                                                    compute::ExecContext* ctx) {
    ScanV2Options normalized(options);
    if (!normalized.dataset) {
      return Status::Invalid("Scan options must include a dataset");
    }

    if (options.scan_task_readahead < 0) {
      return Status::Invalid(
          "Scan task readahead may not be less than 0.  Set to 0 to disable readahead");
    }

    if (!normalized.filter.is_valid()) {
      normalized.filter = compute::literal(true);
    }

    if (normalized.filter.call() && normalized.filter.IsBound()) {
      // There is no easy way to make sure a filter was bound agaisnt the same
      // function registry as the one in ctx so we just require it to be unbound
      // FIXME - Do we care if it was bound to a different function registry?
      return Status::Invalid("Scan filter must be unbound");
    } else {
      ARROW_ASSIGN_OR_RAISE(normalized.filter,
                            normalized.filter.Bind(*options.dataset->schema(), ctx));
      ARROW_ASSIGN_OR_RAISE(normalized.filter,
                            compute::RemoveNamedRefs(std::move(normalized.filter)));
    }  // Else we must have some simple filter like literal(true) which might be bound
       // but we don't care

    if (normalized.filter.type()->id() != Type::BOOL) {
      return Status::Invalid("A scan filter must be a boolean expression");
    }

    return std::move(normalized);
  }

  static Result<acero::ExecNode*> Make(acero::ExecPlan* plan,
                                       std::vector<acero::ExecNode*> inputs,
                                       const acero::ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 0, "ScanNode"));
    const auto& scan_options = checked_cast<const ScanV2Options&>(options);
    ARROW_ASSIGN_OR_RAISE(
        ScanV2Options normalized_options,
        NormalizeAndValidate(scan_options, plan->query_context()->exec_context()));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Schema> output_schema,
                          OutputSchemaFromOptions(normalized_options));
    return plan->EmplaceNode<ScanNode>(plan, std::move(normalized_options),
                                       std::move(output_schema));
  }

  const char* kind_name() const override { return "ScanNode"; }

  [[noreturn]] static void NoInputs() {
    Unreachable("no inputs; this should never be called");
  }
  [[noreturn]] Status InputReceived(acero::ExecNode*, compute::ExecBatch) override {
    NoInputs();
  }
  [[noreturn]] Status InputFinished(acero::ExecNode*, int) override { NoInputs(); }

  Status Init() override { return Status::OK(); }

  struct KnownValue {
    std::size_t index;
    Datum value;
  };

  // The staging area and the fragment scanner don't want to know any details about
  // evolution Those details are encapsulated here.  The inspection task calculates
  // exactly how to evolve outgoing batches and creates a template that is used by all
  // scan tasks in the fragment
  class ScanTaskLauncherImpl : public ScanTaskLauncher {
   public:
    ScanTaskLauncherImpl(ScanNode* node,
                         std::unique_ptr<FragmentEvolutionStrategy> fragment_evolution,
                         std::vector<KnownValue> known_values,
                         FragmentScanRequest scan_request)
        : node_(node),
          fragment_evolution_(std::move(fragment_evolution)),
          known_values_(std::move(known_values)),
          scan_request_(std::move(scan_request)) {}

    void LaunchTask(FragmentScanner* scanner, int scan_task_number, int first_batch_index,
                    std::function<void(int num_batches)> task_complete_cb) override {
      AsyncGenerator<std::shared_ptr<RecordBatch>> batch_gen =
          scanner->RunScanTask(scan_task_number);
      auto batch_count = std::make_shared<int>(0);
      int* batch_count_view = batch_count.get();
      node_->plan_->query_context()
          ->async_scheduler()
          ->AddAsyncGenerator<std::shared_ptr<RecordBatch>>(
              std::move(batch_gen),
              [this, batch_count_view](const std::shared_ptr<RecordBatch>& batch) {
                (*batch_count_view)++;
                return HandleBatch(batch);
              },
              "ScanNode::ScanBatch::Next",
              [this, task_complete_cb = std::move(task_complete_cb),
               batch_count = std::move(batch_count)]() {
                node_->plan_->query_context()->ScheduleTask(
                    [task_complete_cb, batch_count] {
                      task_complete_cb(*batch_count);
                      return Status::OK();
                    },
                    "ScanTaskWrapUp");
                return Status::OK();
              });
    }

    const FragmentScanRequest& scan_request() const { return scan_request_; }

   private:
    compute::ExecBatch AddKnownValues(compute::ExecBatch batch) {
      if (known_values_.empty()) {
        return batch;
      }
      std::vector<Datum> with_known_values;
      int num_combined_cols =
          static_cast<int>(batch.values.size() + known_values_.size());
      with_known_values.reserve(num_combined_cols);
      auto known_values_itr = known_values_.cbegin();
      auto batch_itr = batch.values.begin();
      for (int i = 0; i < num_combined_cols; i++) {
        if (known_values_itr != known_values_.end() &&
            static_cast<int>(known_values_itr->index) == i) {
          with_known_values.push_back(known_values_itr->value);
          known_values_itr++;
        } else {
          with_known_values.push_back(std::move(*batch_itr));
          batch_itr++;
        }
      }
      return compute::ExecBatch(std::move(with_known_values), batch.length);
    }

    Status HandleBatch(const std::shared_ptr<RecordBatch>& batch) {
      ARROW_ASSIGN_OR_RAISE(
          compute::ExecBatch evolved_batch,
          fragment_evolution_->EvolveBatch(batch, node_->options_.columns,
                                           *scan_request_.fragment_selection));
      compute::ExecBatch with_known_values = AddKnownValues(std::move(evolved_batch));
      node_->plan_->query_context()->ScheduleTask(
          [node = node_, output_batch = std::move(with_known_values)] {
            node->batch_counter_++;
            return node->output_->InputReceived(node, output_batch);
          },
          "ScanNode::ProcessMorsel");
      return Status::OK();
    }

    ScanNode* node_;
    const std::unique_ptr<FragmentEvolutionStrategy> fragment_evolution_;
    const std::vector<KnownValue> known_values_;
    const FragmentScanRequest scan_request_;
  };

  struct InspectFragmentTask : util::AsyncTaskScheduler::Task {
    InspectFragmentTask(ScanNode* node, std::shared_ptr<Fragment> fragment,
                        int fragment_index)
        : node_(node), fragment_(std::move(fragment)), fragment_index_(fragment_index) {
      name_ = "ScanNode::InspectFragment::" + fragment_->ToString();
    }

    Result<Future<>> operator()() override {
      return fragment_
          ->InspectFragment(node_->options_.format_options,
                            node_->plan_->query_context()->exec_context(),
                            node_->options_.cache_fragment_inspection)
          .Then([this](const std::shared_ptr<InspectedFragment>& inspected_fragment) {
            return OnInspectionComplete(inspected_fragment);
          });
    }

    std::string_view name() const override { return name_; }

    struct ExtractedKnownValues {
      // Columns that must be loaded from the fragment
      std::vector<FieldPath> remaining_columns;
      // Columns whose value is already known from the partition guarantee
      std::vector<KnownValue> known_values;
    };

    Result<ExtractedKnownValues> ExtractKnownValuesFromGuarantee(
        const compute::Expression& guarantee) {
      ARROW_ASSIGN_OR_RAISE(
          compute::KnownFieldValues part_values,
          compute::ExtractKnownFieldValues(fragment_->partition_expression()));
      ExtractedKnownValues extracted;
      for (std::size_t i = 0; i < node_->options_.columns.size(); i++) {
        const auto& field_path = node_->options_.columns[i];
        FieldRef field_ref(field_path);
        auto existing = part_values.map.find(FieldRef(field_path));
        if (existing == part_values.map.end()) {
          // Column not in our known values, we must load from fragment
          extracted.remaining_columns.push_back(field_path);
        } else {
          ARROW_ASSIGN_OR_RAISE(const std::shared_ptr<Field>& field,
                                field_path.Get(*node_->options_.dataset->schema()));
          Result<Datum> maybe_casted = compute::Cast(existing->second, field->type());
          if (!maybe_casted.ok()) {
            // TODO(weston) In theory this should be preventable.  The dataset and
            // partitioning schemas are known at the beginning of the scan and we could
            // compare the two and discover that they are incompatible.
            //
            // Provide some context here as this error can be confusing
            return Status::Invalid(
                "The dataset schema defines the field ", field_path, " as type ",
                field->type()->ToString(), " but a partition column was found with type ",
                existing->second.type()->ToString(), " which cannot be safely cast");
          }
          extracted.known_values.push_back({i, *maybe_casted});
        }
      }
      return std::move(extracted);
    }

    Future<> OnInspectionComplete(
        const std::shared_ptr<InspectedFragment>& inspected_fragment) {
      // Based on the fragment's guarantee we may not need to retrieve all the columns
      compute::Expression fragment_filter = node_->options_.filter;
      ARROW_ASSIGN_OR_RAISE(
          compute::Expression filter_minus_part,
          compute::SimplifyWithGuarantee(std::move(fragment_filter),
                                         fragment_->partition_expression()));

      ARROW_ASSIGN_OR_RAISE(
          ExtractedKnownValues extracted,
          ExtractKnownValuesFromGuarantee(fragment_->partition_expression()));

      // Now that we have an inspected fragment we need to use the dataset's evolution
      // strategy to figure out how to scan it
      std::unique_ptr<FragmentEvolutionStrategy> fragment_evolution =
          node_->options_.dataset->evolution_strategy()->GetStrategy(
              *node_->options_.dataset, *fragment_, *inspected_fragment);
      ARROW_ASSIGN_OR_RAISE(
          std::unique_ptr<ScanTaskLauncherImpl> task_launcher,
          CreateTaskLauncher(std::move(fragment_evolution), extracted.remaining_columns,
                             filter_minus_part, std::move(extracted.known_values)));

      return fragment_
          ->BeginScan(task_launcher->scan_request(), inspected_fragment.get(),
                      node_->plan_->query_context()->exec_context())
          .Then([this, task_launcher = std::move(task_launcher)](
                    const std::shared_ptr<FragmentScanner>& fragment_scanner) mutable {
            node_->staging_area_->InsertInspectedFragment(
                fragment_scanner, fragment_index_, std::move(task_launcher));
          });
    }

    // Take the dataset options, and the fragment evolution, and figure out exactly how
    // we should scan the fragment itself.
    Result<std::unique_ptr<ScanTaskLauncherImpl>> CreateTaskLauncher(
        std::unique_ptr<FragmentEvolutionStrategy> fragment_evolution,
        const std::vector<FieldPath>& desired_columns, const compute::Expression& filter,
        std::vector<KnownValue> known_values) {
      FragmentScanRequest scan_request;
      ARROW_ASSIGN_OR_RAISE(scan_request.fragment_selection,
                            fragment_evolution->DevolveSelection(desired_columns));
      ARROW_ASSIGN_OR_RAISE(compute::Expression devolution_guarantee,
                            fragment_evolution->GetGuarantee(desired_columns));
      ARROW_ASSIGN_OR_RAISE(compute::Expression simplified_filter,
                            compute::SimplifyWithGuarantee(filter, devolution_guarantee));
      ARROW_ASSIGN_OR_RAISE(scan_request.filter, fragment_evolution->DevolveFilter(
                                                     std::move(simplified_filter)));
      scan_request.format_scan_options = node_->options_.format_options;
      auto task_launcher = std::make_unique<ScanTaskLauncherImpl>(
          node_, std::move(fragment_evolution), std::move(known_values),
          std::move(scan_request));
      return task_launcher;
    }

    ScanNode* node_;
    std::shared_ptr<Fragment> fragment_;
    int fragment_index_;
    std::string name_;
  };

  void InspectFragments(const AsyncGenerator<std::shared_ptr<Fragment>>& frag_gen) {
    plan_->query_context()
        ->async_scheduler()
        ->AddAsyncGenerator<std::shared_ptr<Fragment>>(
            std::move(frag_gen),
            [this](const std::shared_ptr<Fragment>& fragment) {
              inspection_throttle_->AddTask(std::make_unique<InspectFragmentTask>(
                  this, fragment, fragment_index_++));
              return Status::OK();
            },
            "ScanNode::ListDataset::Next",
            [this] {
              staging_area_->FinishedInsertingFragments(fragment_index_);
              return Status::OK();
            });
  }

  Status StartProducing() override {
    NoteStartProducing(ToStringExtra());
    inspection_throttle_ = util::ThrottledAsyncTaskScheduler::Make(
        plan_->query_context()->async_scheduler(), kNumConcurrentInspections);
    auto completion = [this] {
      plan_->query_context()->ScheduleTask(
          [this] { return output_->InputFinished(this, batch_counter_.load()); },
          "ScanNode::Finished");
    };
    // For ease of use we treat readahead=1 as "only scan one thing at a time"
    int scan_task_readahead = std::max(options_.scan_task_readahead, 1);
    staging_area_ = std::make_unique<ScanTaskStagingArea>(
        kLimitQueuedScanTasks, scan_task_readahead, inspection_throttle_.get(),
        std::move(completion));
    plan_->query_context()->async_scheduler()->AddSimpleTask(
        [this] {
          return GetFragments(options_.dataset.get(), options_.filter)
              .Then([this](const AsyncGenerator<std::shared_ptr<Fragment>>& frag_gen) {
                InspectFragments(frag_gen);
              });
        },
        "ScanNode::StartListing"sv);
    return Status::OK();
  }

  void PauseProducing(ExecNode* output, int32_t counter) override {
    // TODO(ARROW-17755)
  }

  void ResumeProducing(ExecNode* output, int32_t counter) override {
    // TODO(ARROW-17755)
  }

  Status StopProducingImpl() override { return Status::OK(); }

 private:
  ScanV2Options options_;
  std::shared_ptr<util::ThrottledAsyncTaskScheduler> inspection_throttle_;
  std::unique_ptr<ScanTaskStagingArea> staging_area_;
  int fragment_index_ = 0;
  std::atomic<int32_t> batch_counter_{0};
};

}  // namespace

namespace internal {
void InitializeScannerV2(arrow::acero::ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("scan2", ScanNode::Make));
}
}  // namespace internal
}  // namespace dataset
}  // namespace arrow
