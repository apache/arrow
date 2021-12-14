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
#include <mutex>
#include <unordered_map>

#include "arrow/filesystem/path_util.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/map.h"
#include "arrow/util/string.h"

namespace arrow {
namespace dataset {
namespace internal {

namespace {

constexpr util::string_view kIntegerToken = "{i}";

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

class DatasetWriterFileQueue : public util::AsyncDestroyable {
 public:
  explicit DatasetWriterFileQueue(const Future<std::shared_ptr<FileWriter>>& writer_fut,
                                  const FileSystemDatasetWriteOptions& options,
                                  DatasetWriterState* writer_state)
      : options_(options), writer_state_(writer_state) {
    // If this AddTask call fails (e.g. we're given an already failing future) then we
    // will get the error later when we try and write to it.
    ARROW_UNUSED(file_tasks_.AddTask([this, writer_fut] {
      return writer_fut.Then(
          [this](const std::shared_ptr<FileWriter>& writer) { writer_ = writer; });
    }));
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

  Status ScheduleBatch(std::shared_ptr<RecordBatch> batch) {
    struct WriteTask {
      Future<> operator()() { return self->WriteNext(std::move(batch)); }
      DatasetWriterFileQueue* self;
      std::shared_ptr<RecordBatch> batch;
    };
    return file_tasks_.AddTask(WriteTask{this, std::move(batch)});
  }

  Result<int64_t> PopAndDeliverStagedBatch() {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> next_batch, PopStagedBatch());
    int64_t rows_popped = next_batch->num_rows();
    rows_currently_staged_ -= next_batch->num_rows();
    ARROW_RETURN_NOT_OK(ScheduleBatch(std::move(next_batch)));
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

  Future<> DoDestroy() override {
    writer_state_->staged_rows_count -= rows_currently_staged_;
    while (!staged_batches_.empty()) {
      RETURN_NOT_OK(PopAndDeliverStagedBatch());
    }
    return file_tasks_.End().Then([this] { return DoFinish(); });
  }

 private:
  Future<> WriteNext(std::shared_ptr<RecordBatch> next) {
    struct WriteTask {
      Status operator()() {
        int64_t rows_to_release = batch->num_rows();
        Status status = self->writer_->Write(batch);
        self->writer_state_->rows_in_flight_throttle.Release(rows_to_release);
        return status;
      }
      DatasetWriterFileQueue* self;
      std::shared_ptr<RecordBatch> batch;
    };
    // May want to prototype / measure someday pushing the async write down further
    return DeferNotOk(
        io::default_io_context().executor()->Submit(WriteTask{this, std::move(next)}));
  }

  Status DoFinish() {
    {
      std::lock_guard<std::mutex> lg(writer_state_->visitors_mutex);
      RETURN_NOT_OK(options_.writer_pre_finish(writer_.get()));
    }
    RETURN_NOT_OK(writer_->Finish());
    {
      std::lock_guard<std::mutex> lg(writer_state_->visitors_mutex);
      return options_.writer_post_finish(writer_.get());
    }
  }

  const FileSystemDatasetWriteOptions& options_;
  DatasetWriterState* writer_state_;
  std::shared_ptr<FileWriter> writer_;
  // Batches are accumulated here until they are large enough to write out at which
  // point they are merged together and added to write_queue_
  std::deque<std::shared_ptr<RecordBatch>> staged_batches_;
  uint64_t rows_currently_staged_ = 0;
  util::SerializedAsyncTaskGroup file_tasks_;
};

struct WriteTask {
  std::string filename;
  uint64_t num_rows;
};

class DatasetWriterDirectoryQueue : public util::AsyncDestroyable {
 public:
  DatasetWriterDirectoryQueue(std::string directory, std::shared_ptr<Schema> schema,
                              const FileSystemDatasetWriteOptions& write_options,
                              DatasetWriterState* writer_state)
      : directory_(std::move(directory)),
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
      ARROW_ASSIGN_OR_RAISE(latest_open_file_, OpenFileQueue(current_filename_));
    }
    return latest_open_file_->Push(batch);
  }

  Result<std::string> GetNextFilename() {
    auto basename = ::arrow::internal::Replace(
        write_options_.basename_template, kIntegerToken, std::to_string(file_counter_++));
    if (!basename) {
      return Status::Invalid("string interpolation of basename template failed");
    }

    return fs::internal::ConcatAbstractPath(directory_, *basename);
  }

  Status FinishCurrentFile() {
    if (latest_open_file_) {
      latest_open_file_ = nullptr;
    }
    rows_written_ = 0;
    return GetNextFilename().Value(&current_filename_);
  }

  Result<std::shared_ptr<FileWriter>> OpenWriter(const std::string& filename) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<io::OutputStream> out_stream,
                          write_options_.filesystem->OpenOutputStream(filename));
    return write_options_.format()->MakeWriter(std::move(out_stream), schema_,
                                               write_options_.file_write_options,
                                               {write_options_.filesystem, filename});
  }

  Result<std::shared_ptr<DatasetWriterFileQueue>> OpenFileQueue(
      const std::string& filename) {
    Future<std::shared_ptr<FileWriter>> file_writer_fut =
        init_future_.Then([this, filename] {
          ::arrow::internal::Executor* io_executor =
              write_options_.filesystem->io_context().executor();
          return DeferNotOk(
              io_executor->Submit([this, filename]() { return OpenWriter(filename); }));
        });
    auto file_queue = util::MakeSharedAsync<DatasetWriterFileQueue>(
        file_writer_fut, write_options_, writer_state_);
    RETURN_NOT_OK(task_group_.AddTask(file_queue->on_closed().Then(
        [this] { writer_state_->open_files_throttle.Release(1); })));
    return file_queue;
  }

  uint64_t rows_written() const { return rows_written_; }

  void PrepareDirectory() {
    init_future_ =
        DeferNotOk(write_options_.filesystem->io_context().executor()->Submit([this] {
          RETURN_NOT_OK(write_options_.filesystem->CreateDir(directory_));
          if (write_options_.existing_data_behavior ==
              ExistingDataBehavior::kDeleteMatchingPartitions) {
            return write_options_.filesystem->DeleteDirContents(directory_);
          }
          return Status::OK();
        }));
  }

  static Result<std::unique_ptr<DatasetWriterDirectoryQueue,
                                util::DestroyingDeleter<DatasetWriterDirectoryQueue>>>
  Make(util::AsyncTaskGroup* task_group,
       const FileSystemDatasetWriteOptions& write_options,
       DatasetWriterState* writer_state, std::shared_ptr<Schema> schema,
       std::string dir) {
    auto dir_queue = util::MakeUniqueAsync<DatasetWriterDirectoryQueue>(
        std::move(dir), std::move(schema), write_options, writer_state);
    RETURN_NOT_OK(task_group->AddTask(dir_queue->on_closed()));
    dir_queue->PrepareDirectory();
    ARROW_ASSIGN_OR_RAISE(dir_queue->current_filename_, dir_queue->GetNextFilename());
    // std::move required to make RTools 3.5 mingw compiler happy
    return std::move(dir_queue);
  }

  Future<> DoDestroy() override {
    latest_open_file_.reset();
    return task_group_.End();
  }

 private:
  util::AsyncTaskGroup task_group_;
  std::string directory_;
  std::shared_ptr<Schema> schema_;
  const FileSystemDatasetWriteOptions& write_options_;
  DatasetWriterState* writer_state_;
  Future<> init_future_;
  std::string current_filename_;
  std::shared_ptr<DatasetWriterFileQueue> latest_open_file_;
  uint64_t rows_written_ = 0;
  uint32_t file_counter_ = 0;
};

Status ValidateBasenameTemplate(util::string_view basename_template) {
  if (basename_template.find(fs::internal::kSep) != util::string_view::npos) {
    return Status::Invalid("basename_template contained '/'");
  }
  size_t token_start = basename_template.find(kIntegerToken);
  if (token_start == util::string_view::npos) {
    return Status::Invalid("basename_template did not contain '", kIntegerToken, "'");
  }
  size_t next_token_start = basename_template.find(kIntegerToken, token_start + 1);
  if (next_token_start != util::string_view::npos) {
    return Status::Invalid("basename_template contained '", kIntegerToken,
                           "' more than once");
  }
  return Status::OK();
}

Status ValidateOptions(const FileSystemDatasetWriteOptions& options) {
  ARROW_RETURN_NOT_OK(ValidateBasenameTemplate(options.basename_template));
  if (options.max_rows_per_group <= 0) {
    return Status::Invalid("max_rows_per_group must be a positive number");
  }
  if (options.max_rows_per_group < options.min_rows_per_group) {
    return Status::Invalid("max_rows_per_group must be less than min_rows_per_group");
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
    if (maybe_files->size() > 1) {
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

class DatasetWriter::DatasetWriterImpl : public util::AsyncDestroyable {
 public:
  DatasetWriterImpl(FileSystemDatasetWriteOptions write_options, uint64_t max_rows_queued)
      : write_options_(std::move(write_options)),
        writer_state_(max_rows_queued, write_options_.max_open_files,
                      CalculateMaxRowsStaged(max_rows_queued)) {}

  Future<> WriteRecordBatch(std::shared_ptr<RecordBatch> batch,
                            const std::string& directory) {
    RETURN_NOT_OK(CheckError());
    if (batch->num_rows() == 0) {
      return Future<>::MakeFinished();
    }
    if (!directory.empty()) {
      auto full_path =
          fs::internal::ConcatAbstractPath(write_options_.base_dir, directory);
      return DoWriteRecordBatch(std::move(batch), full_path);
    } else {
      return DoWriteRecordBatch(std::move(batch), write_options_.base_dir);
    }
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
                              const std::string& directory) {
    ARROW_ASSIGN_OR_RAISE(
        auto dir_queue_itr,
        ::arrow::internal::GetOrInsertGenerated(
            &directory_queues_, directory, [this, &batch](const std::string& dir) {
              return DatasetWriterDirectoryQueue::Make(
                  &task_group_, write_options_, &writer_state_, batch->schema(), dir);
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
        break;
      }
      if (will_open_file) {
        backpressure = writer_state_.open_files_throttle.Acquire(1);
        if (!backpressure.is_finished()) {
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
      return backpressure.Then(
          [this, batch, directory] { return DoWriteRecordBatch(batch, directory); });
    }
    return Future<>::MakeFinished();
  }

  void SetError(Status st) {
    std::lock_guard<std::mutex> lg(mutex_);
    err_ = std::move(st);
  }

  Status CheckError() {
    std::lock_guard<std::mutex> lg(mutex_);
    return err_;
  }

  Future<> DoDestroy() override {
    directory_queues_.clear();
    return task_group_.End().Then([this] { return err_; });
  }

  util::AsyncTaskGroup task_group_;
  FileSystemDatasetWriteOptions write_options_;
  DatasetWriterState writer_state_;
  std::unordered_map<std::string, std::shared_ptr<DatasetWriterDirectoryQueue>>
      directory_queues_;
  std::mutex mutex_;
  Status err_;
};

DatasetWriter::DatasetWriter(FileSystemDatasetWriteOptions write_options,
                             uint64_t max_rows_queued)
    : impl_(util::MakeUniqueAsync<DatasetWriterImpl>(std::move(write_options),
                                                     max_rows_queued)) {}

Result<std::unique_ptr<DatasetWriter>> DatasetWriter::Make(
    FileSystemDatasetWriteOptions write_options, uint64_t max_rows_queued) {
  RETURN_NOT_OK(ValidateOptions(write_options));
  RETURN_NOT_OK(EnsureDestinationValid(write_options));
  return std::unique_ptr<DatasetWriter>(
      new DatasetWriter(std::move(write_options), max_rows_queued));
}

DatasetWriter::~DatasetWriter() = default;

Future<> DatasetWriter::WriteRecordBatch(std::shared_ptr<RecordBatch> batch,
                                         const std::string& directory) {
  return impl_->WriteRecordBatch(std::move(batch), directory);
}

Future<> DatasetWriter::Finish() {
  Future<> finished = impl_->on_closed();
  impl_.reset();
  return finished;
}

}  // namespace internal
}  // namespace dataset
}  // namespace arrow
