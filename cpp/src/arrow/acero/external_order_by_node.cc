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

#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <utility>
#include <vector>

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/query_context.h"
#include "arrow/acero/util.h"
#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/io/file.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/tracing_internal.h"
#include "arrow/util/type_fwd.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/async_util.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/future.h"
#include "arrow/io/util_internal.h"
#include "parquet/arrow/writer.h"
#include "parquet/arrow/reader.h"

namespace arrow {

using internal::checked_cast;

using compute::TakeOptions;
using parquet::ArrowWriterProperties;
using parquet::WriterProperties;

namespace acero {
namespace {

class OrderedSpillingAccumulationQueue {
 public:
  OrderedSpillingAccumulationQueue(ExecPlan* plan, std::shared_ptr<Schema> output_schema,
                                   Ordering new_ordering, int64_t buffer_size,
                                   std::string path_to_folder)
      : plan_(plan),
        output_schema_(output_schema),
        ordering_(new_ordering),
        buffer_size_(buffer_size),
        path_to_folder_(path_to_folder),

        accumulation_queue_size_(0),
        spill_count_(0),
        exec_batch_in_memory_(nullptr),
        has_only_memory_spill_count_(false) {}

  // Inserts a batch into the queue.  This may trigger a write to disk if enough data is
  // accumulated If it does, then SpillCount should be incremented before this method
  // returns (but the write can happen in the background, asynchronously)
  Status push_back(std::shared_ptr<RecordBatch> record_batch) {
    mutex_.lock();
    accumulation_queue_.push_back(record_batch);
    accumulation_queue_size_ += record_batch->num_rows();

    if (accumulation_queue_size_ >= buffer_size_) {
      spill_count_++;
      int64_t spill_index = spill_count_ - 1;
      ARROW_ASSIGN_OR_RAISE(
          auto table,
          Table::FromRecordBatches(
              output_schema_, std::move(accumulation_queue_)));  // todo check batches_
      accumulation_queue_size_ = 0;
      accumulation_queue_ = std::vector<std::shared_ptr<RecordBatch>>();
      mutex_.unlock();

      ARROW_ASSIGN_OR_RAISE(auto sorted_table, sort_table(table));
      ARROW_RETURN_NOT_OK(schedule_write_task(sorted_table, spill_index));
    } else {
      mutex_.unlock();
    }
    return Status::OK();
  }

  Status push_finshed() {
    if(accumulation_queue_size_>0){
      spill_count_++;
    }
    batch_size_ = buffer_size_ / spill_count_;

    if (accumulation_queue_size_ > 0) {
      ARROW_ASSIGN_OR_RAISE(
          auto table,
          Table::FromRecordBatches(
              output_schema_, std::move(accumulation_queue_)));  // todo check batches_

      if (accumulation_queue_size_ > batch_size_) { // output oversize part of data
        int64_t spill_index = spill_count_ - 1;
        int64_t output_size = accumulation_queue_size_ - batch_size_;

        TableBatchReader reader(table);
        reader.set_chunksize(std::move(output_size));
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> next, reader.Next());
        std::vector<std::shared_ptr<RecordBatch>> batches;
        batches.push_back(std::move(next));
        ARROW_ASSIGN_OR_RAISE(auto output_table, Table::FromRecordBatches(
                                                     output_schema_, std::move(batches)));
        ARROW_ASSIGN_OR_RAISE(auto sorted_table, sort_table(output_table));
        ARROW_RETURN_NOT_OK(schedule_write_task(sorted_table, spill_index));

        reader.set_chunksize(batch_size_);
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> record_batch, reader.Next());
        exec_batch_in_memory_ = std::make_shared<ExecBatch>(*record_batch);
      } else {//in memory
        has_only_memory_spill_count_ = true;

        TableBatchReader reader(table);
        reader.set_chunksize(batch_size_);
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> record_batch, reader.Next());
        exec_batch_in_memory_ = std::make_shared<ExecBatch>(*record_batch);
      }
      accumulation_queue_size_ = 0;
    }

    //init AsyncGenerators
    int64_t count=spill_count_;
    if(has_only_memory_spill_count_){
      count--;
    }
    for (int i = 0; i <count ; i++) {
      std::string path_to_file = get_path_to_file(i);
      ARROW_ASSIGN_OR_RAISE(auto generator, MakeGenerator(path_to_file, batch_size_));
      asyncGenerators_.push_back(generator);
    }
    return Status::OK();
  }

  // The number of files that have been written to disk.  This should also include any
  // data in memory so it will be the number of files written to disk + 1 if there is
  // in-memory data.
  int SpillCount() {
      return spill_count_;    
  }

  // This should only be called after all calls to InsertBatch have been completed.  This
  // starts reading the data that was spilled. It will grab the next batch of data from
  // the given spilled file.  If spill_index
  // == SpillCount() - 1 then this might be data that is already in-memory.
  Future<std::optional<ExecBatch>> FetchNextBatch(int spill_index) {
    if (spill_index == spill_count_ - 1 && exec_batch_in_memory_ != nullptr) {
      auto future = Future<std::optional<ExecBatch>>::MakeFinished(
          *(std::move(exec_batch_in_memory_)));
      exec_batch_in_memory_ = nullptr;
      return future;
    }
    if (spill_index >= 0 &&
        (spill_index < spill_count_ - 1 ||
         (spill_index == spill_count_ - 1 && !has_only_memory_spill_count_))) {
      return asyncGenerators_.at(spill_index)();
    }

    return Future<std::optional<ExecBatch>>::MakeFinished(std::nullopt);
  }

 protected:
  inline std::string get_path_to_file(int spill_index) {
    return path_to_folder_ + "/sort_" + std::to_string(spill_index) + ".parquet";
  }

  Result<std::shared_ptr<arrow::Table>> sort_table(std::shared_ptr<arrow::Table> table) {
    SortOptions sort_options(ordering_.sort_keys(), ordering_.null_placement());
    ExecContext* ctx = plan_->query_context()->exec_context();
    ARROW_ASSIGN_OR_RAISE(auto indices, SortIndices(table, sort_options, ctx));
    ARROW_ASSIGN_OR_RAISE(auto sorted_table,
                          Take(table, indices, TakeOptions::NoBoundsCheck(), ctx));
    return sorted_table.table();
  }

  Status schedule_write_task(std::shared_ptr<arrow::Table> table, int spill_index) {
    std::string path_to_file = get_path_to_file(spill_index);
    std::shared_ptr<WriterProperties> props =
        WriterProperties::Builder().compression(arrow::Compression::SNAPPY)->build();
    std::shared_ptr<ArrowWriterProperties> arrow_props =
        ArrowWriterProperties::Builder().store_schema()->build();
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::io::FileOutputStream> outfile,
                          arrow::io::FileOutputStream::Open(path_to_file));
    plan_->query_context()->ScheduleIOTask(
        [table, outfile, props, arrow_props]() mutable {
          ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
              *(table.get()), arrow::default_memory_pool(), outfile,
              /*chunk_size=*/3, props, arrow_props));
          return Status::OK();
        },
        "OrderByNode::OrderedSpillingAccumulationQueue::Spillover");
    return Status::OK();
  }

  static Result<arrow::AsyncGenerator<std::optional<ExecBatch>>> MakeGenerator(
      const std::string& path_to_file, int64_t batch_size) {
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    // Configure general Parquet reader settings
    auto reader_properties = parquet::ReaderProperties(pool);
    reader_properties.set_buffer_size(4096 * 4);
    reader_properties.enable_buffered_stream();

    // Configure Arrow-specific Parquet reader settings
    auto arrow_reader_props = parquet::ArrowReaderProperties();
    arrow_reader_props.set_batch_size(batch_size);

    parquet::arrow::FileReaderBuilder reader_builder;
    ARROW_RETURN_NOT_OK(
        reader_builder.OpenFile(path_to_file, /*memory_map=*/false, reader_properties));
    reader_builder.memory_pool(pool);
    reader_builder.properties(arrow_reader_props);

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ARROW_ASSIGN_OR_RAISE(arrow_reader, reader_builder.Build());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    ARROW_RETURN_NOT_OK(arrow_reader->GetRecordBatchReader(&rb_reader));

    return MakeGenerator(rb_reader, io::internal::GetIOThreadPool());
  }

  static Result<arrow::AsyncGenerator<std::optional<ExecBatch>>> MakeGenerator(
      const std::shared_ptr<RecordBatchReader>& reader,
      arrow::internal::Executor* io_executor) {
    auto to_exec_batch =
        [](const std::shared_ptr<RecordBatch>& batch) -> std::optional<ExecBatch> {
      if (batch == NULLPTR) {
        return std::nullopt;
      }
      return std::optional<ExecBatch>(ExecBatch(*batch));
    };
    Iterator<std::shared_ptr<RecordBatch>> batch_it = MakeIteratorFromReader(reader);
    auto exec_batch_it = MakeMapIterator(to_exec_batch, std::move(batch_it));
    if (io_executor == nullptr) {
      return MakeBlockingGenerator(std::move(exec_batch_it));
    }
    return MakeBackgroundGenerator(std::move(exec_batch_it), io_executor);
  }

 private:
  ExecPlan* plan_;
  std::shared_ptr<Schema> output_schema_;
  Ordering ordering_;
  int64_t buffer_size_;
  std::string path_to_folder_;
  int64_t accumulation_queue_size_;
  int64_t spill_count_;
  std::shared_ptr<ExecBatch> exec_batch_in_memory_;
  bool has_only_memory_spill_count_;

  std::mutex mutex_;
  std::vector<std::shared_ptr<RecordBatch>> accumulation_queue_;
  std::vector<arrow::AsyncGenerator<std::optional<ExecBatch>>> asyncGenerators_;
  int64_t batch_size_;
};

class ExternalOrderByNode : public ExecNode, public TracedNode {
 public:
  ExternalOrderByNode(ExecPlan* plan, std::vector<ExecNode*> inputs,
                      std::shared_ptr<Schema> output_schema, Ordering new_ordering,
                      int64_t buffer_size, std::string path_to_folder)
      : ExecNode(plan, std::move(inputs), {"input"}, std::move(output_schema)),
        TracedNode(this),
        ordering_(std::move(new_ordering)),
        buffer_size_(std::move(buffer_size)),
        path_to_folder_(std::move(path_to_folder)),
        accumulation_queue_(plan, output_schema_, ordering_, buffer_size_,
                            path_to_folder_) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "FetchNode"));
    const auto& order_options = checked_cast<const ExternalOrderByNodeOptions&>(options);

    if (order_options.ordering.is_implicit() || order_options.ordering.is_unordered()) {
      return Status::Invalid("`ordering` must be an explicit non-empty ordering");
    }
    
    //todo check buffer_size && path_to_folder

    std::shared_ptr<Schema> output_schema = inputs[0]->output_schema();
    return plan->EmplaceNode<ExternalOrderByNode>(
        plan, std::move(inputs), std::move(output_schema), order_options.ordering,
        order_options.buffer_size, order_options.path_to_folder);
  }

  const char* kind_name() const override { return "ExternalOrderByNode"; }

  const Ordering& ordering() const override { return ordering_; }

  Status InputFinished(ExecNode* input, int total_batches) override {
    DCHECK_EQ(input, inputs_[0]);
    EVENT_ON_CURRENT_SPAN("InputFinished", {{"batches.length", total_batches}});
    // We can't send InputFinished downstream because we might change the # of batches
    // when we sort it.  So that happens later in DoFinish
    if (counter_.SetTotal(total_batches)) {
      return DoFinish();
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

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> record_batch,
                          batch.ToRecordBatch(output_schema_));
    RETURN_NOT_OK(accumulation_queue_.push_back(std::move(record_batch)));

    if (counter_.Increment()) {
      return DoFinish();
    }
    return Status::OK();
  }

  Status DoFinish() {
    ARROW_RETURN_NOT_OK(accumulation_queue_.push_finshed());
    std::vector<ExecBatch> batches;
    return Status::OK();
  }

 protected:
  std::string ToStringExtra(int indent = 0) const override {
    std::stringstream ss;
    ss << "external ordering=" << ordering_.ToString();
    return ss.str();
  }

 private:
  AtomicCounter counter_;
  Ordering ordering_;
  int64_t buffer_size_;
  std::string path_to_folder_;
  OrderedSpillingAccumulationQueue accumulation_queue_;
};

}  // namespace

namespace internal {

void RegisterExternalOrderByNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(
      registry->AddFactory(std::string(ExternalOrderByNodeOptions::kName), ExternalOrderByNode::Make));
}

}  // namespace internal
}  // namespace acero
}  // namespace arrow
