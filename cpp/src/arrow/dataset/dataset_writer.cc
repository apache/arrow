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

#include "arrow/dataset/dataset_writer.h"

#include <deque>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "arrow/filesystem/path_util.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/map.h"
#include "arrow/util/string.h"
#include "arrow/util/tracing_internal.h"

using namespace std::string_view_literals;  // NOLINT

namespace arrow {

using internal::Executor;
using internal::ToChars;

namespace dataset {
namespace internal {

namespace {

constexpr std::string_view kIntegerToken = "{i}";

class Throttle {
 public:
  explicit Throttle(uint64_t max_value) : max_value_(max_value) {}

  bool Unthrottled() const { return max_value_ <= 0; }

  Future<> Acquire(uint64_t values) {
    if (Unthrottled()) {
      return Future<>::MakeFinished();
    }
    std::lock_guard<std::mutex> lg(mutex_);
    if (values + current_value_ > max_value_) {
      in_waiting_ = values;
      backpressure_ = Future<>::Make();
    } else {
      current_value_ += values;
    }
    return backpressure_;
  }

  void Release(uint64_t values) {
    if (Unthrottled()) {
      return;
    }
    Future<> to_complete;
    {
      std::lock_guard<std::mutex> lg(mutex_);
      current_value_ -= values;
      if (in_waiting_ > 0 && in_waiting_ + current_value_ <= max_value_) {
        in_waiting_ = 0;
        to_complete = backpressure_;
      }
    }
    if (to_complete.is_valid()) {
      to_complete.MarkFinished();
    }
  }

 private:
  Future<> backpressure_ = Future<>::MakeFinished();
  uint64_t max_value_;
  uint64_t in_waiting_ = 0;
  uint64_t current_value_ = 0;
  std::mutex mutex_;
};

struct DatasetWriterState {
  DatasetWriterState(uint64_t rows_in_flight, uint64_t max_open_files,
                     uint64_t max_rows_staged)
      : rows_in_flight_throttle(rows_in_flight),
        open_files_throttle(max_open_files),
        staged_rows_count(0),
        max_rows_staged(max_rows_staged) {}

  bool StagingFull() const { return staged_rows_count.load() >= max_rows_staged; }

  // Throttle for how many rows the dataset writer will allow to be in process memory
  // When this is exceeded the dataset writer will pause / apply backpressure
  Throttle rows_in_flight_throttle;
  // Control for how many files the dataset writer will open.  When this is exceeded
  // the dataset writer will pause and it will also close the largest open file.
  Throttle open_files_throttle;
  // Control for how many rows the dataset writer will allow to be staged.  A row is
  // staged if it is waiting for more rows to reach minimum_batch_size.  If this is
  // exceeded then the largest staged batch is unstaged (no backpressure is applied)
  std::atomic<uint64_t> staged_rows_count;
  // If too many rows get staged we will end up with poor performance and, if more rows
  // are staged than max_rows_queued we will end up with deadlock.  To avoid this, once
  // we have too many staged rows we just ignore min_rows_per_group
  const uint64_t max_rows_staged;
  // Mutex to guard access to the file visitors in the writer options
  std::mutex visitors_mutex;
};

Result<std::shared_ptr<FileWriter>> OpenWriter(
    const FileSystemDatasetWriteOptions& write_options, std::shared_ptr<Schema> schema,
    const std::string& filename) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<io::OutputStream> out_stream,
                        write_options.filesystem->OpenOutputStream(filename));
  return write_options.format()->MakeWriter(std::move(out_stream), std::move(schema),
                                            write_options.file_write_options,
                                            {write_options.filesystem, filename});
}

class DatasetWriterFileQueue {
 public:
  explicit DatasetWriterFileQueue(const std::shared_ptr<Schema>& schema,
                                  const FileSystemDatasetWriteOptions& options,
                                  DatasetWriterState* writer_state)
      : options_(options), schema_(schema), writer_state_(writer_state) {}

  void Start(util::AsyncTaskScheduler* file_tasks, const std::string& filename) {
    file_tasks_ = std::move(file_tasks);
    // Because the scheduler runs one task at a time we know the writer will
    // be opened before any attempt to write
    file_tasks_->AddSimpleTask(
        [this, filename] {
          Executor* io_executor = options_.filesystem->io_context().executor();
          return DeferNotOk(io_executor->Submit([this, filename]() {
            ARROW_ASSIGN_OR_RAISE(writer_, OpenWriter(options_, schema_, filename));
            return Status::OK();
          }));
        },
        "DatasetWriter::OpenWriter"sv);
  }

  Result<std::shared_ptr<RecordBatch>> PopStagedBatch() {
    std::vector<std::shared_ptr<RecordBatch>> batches_to_write;
    uint64_t num_rows = 0;
    while (!staged_batches_.empty()) {
      std::shared_ptr<RecordBatch> next = std::move(staged_batches_.front());
      staged_batches_.pop_front();
      if (num_rows + next->num_rows() <= options_.max_rows_per_group) {
        num_rows += next->num_rows();
        batches_to_write.push_back(std::move(next));
        if (num_rows == options_.max_rows_per_group) {
          break;
        }
      } else {
        uint64_t remaining = options_.max_rows_per_group - num_rows;
        std::shared_ptr<RecordBatch> next_partial =
            next->Slice(0, static_cast<int64_t>(remaining));
        batches_to_write.push_back(std::move(next_partial));
        std::shared_ptr<RecordBatch> next_remainder =
            next->Slice(static_cast<int64_t>(remaining));
        staged_batches_.push_front(std::move(next_remainder));
        break;
      }
    }
    DCHECK_GT(batches_to_write.size(), 0);
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Table> table,
                          Table::FromRecordBatches(batches_to_write));
    return table->CombineChunksToBatch();
  }

  void ScheduleBatch(std::shared_ptr<RecordBatch> batch) {
    file_tasks_->AddSimpleTask(
        [self = this, batch = std::move(batch)]() {
          return self->WriteNext(std::move(batch));
        },
        "DatasetWriter::WriteBatch"sv);
  }

  Result<int64_t> PopAndDeliverStagedBatch() {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> next_batch, PopStagedBatch());
    int64_t rows_popped = next_batch->num_rows();
    rows_currently_staged_ -= next_batch->num_rows();
    ScheduleBatch(std::move(next_batch));
    return rows_popped;
  }

  // Stage batches, popping and delivering batches if enough data has arrived
  Status Push(std::shared_ptr<RecordBatch> batch) {
    uint64_t delta_staged = batch->num_rows();
    rows_currently_staged_ += delta_staged;
    staged_batches_.push_back(std::move(batch));
    while (!staged_batches_.empty() &&
           (writer_state_->StagingFull() ||
            rows_currently_staged_ >= options_.min_rows_per_group)) {
      ARROW_ASSIGN_OR_RAISE(int64_t rows_popped, PopAndDeliverStagedBatch());
      delta_staged -= rows_popped;
    }
    // Note, delta_staged may be negative if we were able to deliver some data
    writer_state_->staged_rows_count += delta_staged;
    return Status::OK();
  }

  Status Finish() {
    writer_state_->staged_rows_count -= rows_currently_staged_;
    while (!staged_batches_.empty()) {
      RETURN_NOT_OK(PopAndDeliverStagedBatch());
    }
    // At this point all write tasks have been added.  Because the scheduler
    // is a 1-task FIFO we know this task will run at the very end and can
    // add it now.
    file_tasks_->AddSimpleTask([this] { return DoFinish(); },
                               "DatasetWriter::FinishFile"sv);
    return Status::OK();
  }

 private:
  Future<> WriteNext(std::shared_ptr<RecordBatch> next) {
    // May want to prototype / measure someday pushing the async write down further
    return DeferNotOk(options_.filesystem->io_context().executor()->Submit(
        [self = this, batch = std::move(next)]() {
          int64_t rows_to_release = batch->num_rows();
          Status status = self->writer_->Write(batch);
          self->writer_state_->rows_in_flight_throttle.Release(rows_to_release);
          return status;
        }));
  }

  Future<> DoFinish() {
    {
      std::lock_guard<std::mutex> lg(writer_state_->visitors_mutex);
      RETURN_NOT_OK(options_.writer_pre_finish(writer_.get()));
    }
    return writer_->Finish().Then([this]() {
      std::lock_guard<std::mutex> lg(writer_state_->visitors_mutex);
      return options_.writer_post_finish(writer_.get());
    });
  }

  const FileSystemDatasetWriteOptions& options_;
  const std::shared_ptr<Schema>& schema_;
  DatasetWriterState* writer_state_;
  std::shared_ptr<FileWriter> writer_;
  // Batches are accumulated here until they are large enough to write out at which
  // point they are merged together and added to write_queue_
  std::deque<std::shared_ptr<RecordBatch>> staged_batches_;
  uint64_t rows_currently_staged_ = 0;
  util::AsyncTaskScheduler* file_tasks_ = nullptr;
};

struct WriteTask {
  std::string filename;
  uint64_t num_rows;
};

class DatasetWriterDirectoryQueue {
 public:
  DatasetWriterDirectoryQueue(util::AsyncTaskScheduler* scheduler, std::string directory,
                              std::string prefix, std::shared_ptr<Schema> schema,
                              const FileSystemDatasetWriteOptions& write_options,
                              DatasetWriterState* writer_state)
      : scheduler_(std::move(scheduler)),
        directory_(std::move(directory)),
        prefix_(std::move(prefix)),
        schema_(std::move(schema)),
        write_options_(write_options),
        writer_state_(writer_state) {}

  Result<std::shared_ptr<RecordBatch>> NextWritableChunk(
      std::shared_ptr<RecordBatch> batch, std::shared_ptr<RecordBatch>* remainder,
      bool* will_open_file) const {
    DCHECK_GT(batch->num_rows(), 0);
    uint64_t rows_available = std::numeric_limits<uint64_t>::max();
    *will_open_file = rows_written_ == 0;
    if (write_options_.max_rows_per_file > 0) {
      rows_available = write_options_.max_rows_per_file - rows_written_;
    }

    std::shared_ptr<RecordBatch> to_queue;
    if (rows_available < static_cast<uint64_t>(batch->num_rows())) {
      to_queue = batch->Slice(0, static_cast<int64_t>(rows_available));
      *remainder = batch->Slice(static_cast<int64_t>(rows_available));
    } else {
      to_queue = std::move(batch);
    }
    return to_queue;
  }

  Status StartWrite(const std::shared_ptr<RecordBatch>& batch) {
    rows_written_ += batch->num_rows();
    WriteTask task{current_filename_, static_cast<uint64_t>(batch->num_rows())};
    if (!latest_open_file_) {
      ARROW_RETURN_NOT_OK(OpenFileQueue(current_filename_));
    }
    return latest_open_file_->Push(batch);
  }

  Result<std::string> GetNextFilename() {
    std::optional<std::string> basename;
    if (write_options_.basename_template_functor == nullptr) {
      basename = ::arrow::internal::Replace(write_options_.basename_template,
                                            kIntegerToken, ToChars(file_counter_++));
    } else {
      basename = ::arrow::internal::Replace(
          write_options_.basename_template, kIntegerToken,
          write_options_.basename_template_functor(file_counter_++));
    }
    if (!basename) {
      return Status::Invalid("string interpolation of basename template failed");
    }
    if (!used_filenames_.insert(*basename).second) {
      return Status::Invalid("filename ", *basename,
                             " is already used before. Check basename_template_functor");
    }
    return fs::internal::ConcatAbstractPath(directory_, prefix_ + *basename);
  }

  Status FinishCurrentFile() {
    if (latest_open_file_) {
      ARROW_RETURN_NOT_OK(latest_open_file_->Finish());
      latest_open_file_tasks_.reset();
      latest_open_file_ = nullptr;
    }
    rows_written_ = 0;
    return GetNextFilename().Value(&current_filename_);
  }

  Status OpenFileQueue(const std::string& filename) {
    auto file_queue =
        std::make_unique<DatasetWriterFileQueue>(schema_, write_options_, writer_state_);
    latest_open_file_ = file_queue.get();
    // Create a dedicated throttle for write jobs to this file and keep it alive until we
    // are finished and have closed the file.
    auto file_finish_task = [this, file_queue = std::move(file_queue)] {
      writer_state_->open_files_throttle.Release(1);
      return Status::OK();
    };
    latest_open_file_tasks_ = util::MakeThrottledAsyncTaskGroup(
        scheduler_, 1, /*queue=*/nullptr, std::move(file_finish_task));
    if (init_future_.is_valid()) {
      latest_open_file_tasks_->AddSimpleTask(
          [init_future = init_future_]() { return init_future; },
          "DatasetWriter::WaitForDirectoryInit"sv);
    }
    latest_open_file_->Start(latest_open_file_tasks_.get(), filename);
    return Status::OK();
  }

  uint64_t rows_written() const { return rows_written_; }

  void PrepareDirectory() {
    if (directory_.empty() || !write_options_.create_dir) {
      return;
    }
    init_future_ = Future<>::Make();
    auto create_dir_cb = [this] {
      return DeferNotOk(write_options_.filesystem->io_context().executor()->Submit(
          [this]() { return write_options_.filesystem->CreateDir(directory_); }));
    };
    // We need to notify waiters whether the directory succeeded or failed.
    auto notify_waiters_cb = [this] { init_future_.MarkFinished(); };
    auto notify_waiters_on_err_cb = [this](const Status& err) {
      // If there is an error the scheduler will abort but that takes some time
      // and we don't want to start writing in the meantime so we send an error to the
      // file writing queue and return the error.
      init_future_.MarkFinished(err);
      return err;
    };
    std::function<Future<>()> init_task;
    if (write_options_.existing_data_behavior ==
        ExistingDataBehavior::kDeleteMatchingPartitions) {
      init_task = [this, create_dir_cb, notify_waiters_cb, notify_waiters_on_err_cb] {
        return write_options_.filesystem
            ->DeleteDirContentsAsync(directory_,
                                     /*missing_dir_ok=*/true)
            .Then(create_dir_cb)
            .Then(notify_waiters_cb, notify_waiters_on_err_cb);
      };
    } else {
      init_task = [create_dir_cb, notify_waiters_cb, notify_waiters_on_err_cb] {
        return create_dir_cb().Then(notify_waiters_cb, notify_waiters_on_err_cb);
      };
    }
    scheduler_->AddSimpleTask(std::move(init_task),
                              "DatasetWriter::InitializeDirectory"sv);
  }

  static Result<std::unique_ptr<DatasetWriterDirectoryQueue>> Make(
      util::AsyncTaskScheduler* scheduler,
      const FileSystemDatasetWriteOptions& write_options,
      DatasetWriterState* writer_state, std::shared_ptr<Schema> schema,
      std::string directory, std::string prefix) {
    auto dir_queue = std::make_unique<DatasetWriterDirectoryQueue>(
        scheduler, std::move(directory), std::move(prefix), std::move(schema),
        write_options, writer_state);
    dir_queue->PrepareDirectory();
    ARROW_ASSIGN_OR_RAISE(dir_queue->current_filename_, dir_queue->GetNextFilename());
    // std::move required to make RTools 3.5 mingw compiler happy
    return std::move(dir_queue);
  }

  Status Finish() {
    if (latest_open_file_) {
      ARROW_RETURN_NOT_OK(latest_open_file_->Finish());
      latest_open_file_tasks_.reset();
      latest_open_file_ = nullptr;
    }
    used_filenames_.clear();
    return Status::OK();
  }

 private:
  util::AsyncTaskScheduler* scheduler_ = nullptr;
  std::string directory_;
  std::string prefix_;
  std::shared_ptr<Schema> schema_;
  const FileSystemDatasetWriteOptions& write_options_;
  DatasetWriterState* writer_state_;
  Future<> init_future_;
  std::string current_filename_;
  std::unordered_set<std::string> used_filenames_;
  DatasetWriterFileQueue* latest_open_file_ = nullptr;
  std::unique_ptr<util::ThrottledAsyncTaskScheduler> latest_open_file_tasks_;
  uint64_t rows_written_ = 0;
  uint32_t file_counter_ = 0;
};

Status ValidateBasenameTemplate(std::string_view basename_template) {
  if (basename_template.find(fs::internal::kSep) != std::string_view::npos) {
    return Status::Invalid("basename_template contained '/'");
  }
  size_t token_start = basename_template.find(kIntegerToken);
  if (token_start == std::string_view::npos) {
    return Status::Invalid("basename_template did not contain '", kIntegerToken, "'");
  }
  size_t next_token_start = basename_template.find(kIntegerToken, token_start + 1);
  if (next_token_start != std::string_view::npos) {
    return Status::Invalid("basename_template contained '", kIntegerToken,
                           "' more than once");
  }
  return Status::OK();
}

Status ValidateOptions(const FileSystemDatasetWriteOptions& options) {
  ARROW_RETURN_NOT_OK(ValidateBasenameTemplate(options.basename_template));
  if (!options.file_write_options) {
    return Status::Invalid("Must provide file_write_options");
  }
  if (!options.filesystem) {
    return Status::Invalid("Must provide filesystem");
  }
  if (options.max_rows_per_group <= 0) {
    return Status::Invalid("max_rows_per_group must be a positive number");
  }
  if (options.max_rows_per_group < options.min_rows_per_group) {
    return Status::Invalid(
        "min_rows_per_group must be less than or equal to max_rows_per_group");
  }
  if (options.max_rows_per_file > 0 &&
      options.max_rows_per_file < options.max_rows_per_group) {
    return Status::Invalid(
        "max_rows_per_group must be less than or equal to max_rows_per_file");
  }
  return Status::OK();
}

Status EnsureDestinationValid(const FileSystemDatasetWriteOptions& options) {
  if (options.existing_data_behavior == ExistingDataBehavior::kError) {
    fs::FileSelector selector;
    selector.base_dir = options.base_dir;
    selector.recursive = true;
    Result<std::vector<fs::FileInfo>> maybe_files =
        options.filesystem->GetFileInfo(selector);
    if (!maybe_files.ok()) {
      // If the path doesn't exist then continue
      return Status::OK();
    }
    if (maybe_files->size() > 0) {
      return Status::Invalid(
          "Could not write to ", options.base_dir,
          " as the directory is not empty and existing_data_behavior is to error");
    }
  }
  return Status::OK();
}

// Rule of thumb for the max rows to stage.  It will grow with max_rows_queued until
// max_rows_queued starts to get too large and then it caps out at 8 million rows.
// Feel free to replace with something more meaningful, this is just a random heuristic.
uint64_t CalculateMaxRowsStaged(uint64_t max_rows_queued) {
  return std::min(static_cast<uint64_t>(1 << 23), max_rows_queued / 4);
}

}  // namespace

class DatasetWriter::DatasetWriterImpl {
 public:
  DatasetWriterImpl(FileSystemDatasetWriteOptions write_options,
                    util::AsyncTaskScheduler* scheduler,
                    std::function<void()> pause_callback,
                    std::function<void()> resume_callback,
                    std::function<void()> finish_callback, uint64_t max_rows_queued)
      : scheduler_(scheduler),
        write_tasks_(util::MakeThrottledAsyncTaskGroup(
            scheduler_, 1, /*queue=*/nullptr,
            [finish_callback = std::move(finish_callback)] {
              finish_callback();
              return Status::OK();
            })),
        write_options_(std::move(write_options)),
        writer_state_(max_rows_queued, write_options_.max_open_files,
                      CalculateMaxRowsStaged(max_rows_queued)),
        pause_callback_(std::move(pause_callback)),
        resume_callback_(std::move(resume_callback)) {}

  Future<> WriteAndCheckBackpressure(std::shared_ptr<RecordBatch> batch,
                                     const std::string& directory,
                                     const std::string& prefix) {
    if (batch->num_rows() == 0) {
      return Future<>::MakeFinished();
    }
    if (!directory.empty()) {
      auto full_path =
          fs::internal::ConcatAbstractPath(write_options_.base_dir, directory);
      return DoWriteRecordBatch(std::move(batch), full_path, prefix);
    } else {
      return DoWriteRecordBatch(std::move(batch), write_options_.base_dir, prefix);
    }
  }

  void WriteRecordBatch(std::shared_ptr<RecordBatch> batch, const std::string& directory,
                        const std::string& prefix) {
    write_tasks_->AddSimpleTask(
        [this, batch = std::move(batch), directory, prefix]() mutable {
          Future<> has_room =
              WriteAndCheckBackpressure(std::move(batch), directory, prefix);
          if (!has_room.is_finished()) {
            // We don't have to worry about sequencing backpressure here since
            // task_group_ serves as our sequencer.  If batches continue to arrive after
            // we pause they will queue up in task_group_ until we free up and call
            // Resume
            pause_callback_();
            return has_room.Then([this] { resume_callback_(); });
          }
          return has_room;
        },
        "DatasetWriter::WriteAndCheckBackpressure"sv);
  }

  void Finish() {
    write_tasks_->AddSimpleTask(
        [this]() -> Result<Future<>> {
          for (const auto& directory_queue : directory_queues_) {
            ARROW_RETURN_NOT_OK(directory_queue.second->Finish());
          }
          // This task is purely synchronous but we add it to write_tasks_ for the
          // throttling task group benefits.
          return Future<>::MakeFinished();
        },
        "DatasetWriter::FinishAll"sv);
    write_tasks_.reset();
  }

 protected:
  Status CloseLargestFile() {
    std::shared_ptr<DatasetWriterDirectoryQueue> largest = nullptr;
    uint64_t largest_num_rows = 0;
    for (auto& dir_queue : directory_queues_) {
      if (dir_queue.second->rows_written() > largest_num_rows) {
        largest_num_rows = dir_queue.second->rows_written();
        largest = dir_queue.second;
      }
    }
    DCHECK_NE(largest, nullptr);
    return largest->FinishCurrentFile();
  }

  Future<> DoWriteRecordBatch(std::shared_ptr<RecordBatch> batch,
                              const std::string& directory, const std::string& prefix) {
    ARROW_ASSIGN_OR_RAISE(
        auto dir_queue_itr,
        ::arrow::internal::GetOrInsertGenerated(
            &directory_queues_, directory + prefix,
            [this, &batch, &directory, &prefix](const std::string& key) {
              return DatasetWriterDirectoryQueue::Make(scheduler_, write_options_,
                                                       &writer_state_, batch->schema(),
                                                       directory, prefix);
            }));
    std::shared_ptr<DatasetWriterDirectoryQueue> dir_queue = dir_queue_itr->second;
    Future<> backpressure;
    while (batch) {
      // Keep opening new files until batch is done.
      std::shared_ptr<RecordBatch> remainder;
      bool will_open_file = false;
      ARROW_ASSIGN_OR_RAISE(auto next_chunk, dir_queue->NextWritableChunk(
                                                 batch, &remainder, &will_open_file));

      backpressure =
          writer_state_.rows_in_flight_throttle.Acquire(next_chunk->num_rows());
      if (!backpressure.is_finished()) {
        EVENT_ON_CURRENT_SPAN("DatasetWriter::Backpressure::TooManyRowsQueued");
        break;
      }
      if (will_open_file) {
        backpressure = writer_state_.open_files_throttle.Acquire(1);
        if (!backpressure.is_finished()) {
          EVENT_ON_CURRENT_SPAN("DatasetWriter::Backpressure::TooManyOpenFiles");
          RETURN_NOT_OK(CloseLargestFile());
          break;
        }
      }
      RETURN_NOT_OK(dir_queue->StartWrite(next_chunk));
      batch = std::move(remainder);
      if (batch) {
        RETURN_NOT_OK(dir_queue->FinishCurrentFile());
      }
    }

    if (batch) {
      return backpressure.Then([this, batch, directory, prefix] {
        return DoWriteRecordBatch(batch, directory, prefix);
      });
    }
    return Future<>::MakeFinished();
  }

  util::AsyncTaskScheduler* scheduler_ = nullptr;
  std::unique_ptr<util::AsyncTaskScheduler> write_tasks_;
  Future<> finish_fut_ = Future<>::Make();
  FileSystemDatasetWriteOptions write_options_;
  DatasetWriterState writer_state_;
  std::function<void()> pause_callback_;
  std::function<void()> resume_callback_;
  std::unordered_map<std::string, std::shared_ptr<DatasetWriterDirectoryQueue>>
      directory_queues_;
  std::mutex mutex_;
  Status err_;
};

DatasetWriter::DatasetWriter(FileSystemDatasetWriteOptions write_options,
                             util::AsyncTaskScheduler* scheduler,
                             std::function<void()> pause_callback,
                             std::function<void()> resume_callback,
                             std::function<void()> finish_callback,
                             uint64_t max_rows_queued)
    : impl_(std::make_unique<DatasetWriterImpl>(
          std::move(write_options), scheduler, std::move(pause_callback),
          std::move(resume_callback), std::move(finish_callback), max_rows_queued)) {}

Result<std::unique_ptr<DatasetWriter>> DatasetWriter::Make(
    FileSystemDatasetWriteOptions write_options, util::AsyncTaskScheduler* scheduler,
    std::function<void()> pause_callback, std::function<void()> resume_callback,
    std::function<void()> finish_callback, uint64_t max_rows_queued) {
  RETURN_NOT_OK(ValidateOptions(write_options));
  RETURN_NOT_OK(EnsureDestinationValid(write_options));
  return std::unique_ptr<DatasetWriter>(new DatasetWriter(
      std::move(write_options), scheduler, std::move(pause_callback),
      std::move(resume_callback), std::move(finish_callback), max_rows_queued));
}

DatasetWriter::~DatasetWriter() = default;

void DatasetWriter::WriteRecordBatch(std::shared_ptr<RecordBatch> batch,
                                     const std::string& directory,
                                     const std::string& prefix) {
  return impl_->WriteRecordBatch(std::move(batch), directory, prefix);
}

void DatasetWriter::Finish() { impl_->Finish(); }

}  // namespace internal
}  // namespace dataset
}  // namespace arrow
