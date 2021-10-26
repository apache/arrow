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

#include <list>
#include <mutex>
#include <unordered_map>

#include "arrow/filesystem/path_util.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
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

class DatasetWriterFileQueue : public util::AsyncDestroyable {
 public:
  explicit DatasetWriterFileQueue(const Future<std::shared_ptr<FileWriter>>& writer_fut,
                                  const FileSystemDatasetWriteOptions& options,
                                  std::mutex* visitors_mutex)
      : options_(options), visitors_mutex_(visitors_mutex) {
    running_task_ = Future<>::Make();
    writer_fut.AddCallback(
        [this](const Result<std::shared_ptr<FileWriter>>& maybe_writer) {
          if (maybe_writer.ok()) {
            writer_ = *maybe_writer;
            Flush();
          } else {
            Abort(maybe_writer.status());
          }
        });
  }

  Future<uint64_t> Push(std::shared_ptr<RecordBatch> batch) {
    std::unique_lock<std::mutex> lk(mutex);
    write_queue_.push_back(std::move(batch));
    Future<uint64_t> write_future = Future<uint64_t>::Make();
    write_futures_.push_back(write_future);
    if (!running_task_.is_valid()) {
      running_task_ = Future<>::Make();
      FlushUnlocked(std::move(lk));
    }
    return write_future;
  }

  Future<> DoDestroy() override {
    std::lock_guard<std::mutex> lg(mutex);
    if (!running_task_.is_valid()) {
      RETURN_NOT_OK(DoFinish());
      return Future<>::MakeFinished();
    }
    return running_task_.Then([this] { return DoFinish(); });
  }

 private:
  Future<uint64_t> WriteNext() {
    // May want to prototype / measure someday pushing the async write down further
    return DeferNotOk(
        io::default_io_context().executor()->Submit([this]() -> Result<uint64_t> {
          DCHECK(running_task_.is_valid());
          std::unique_lock<std::mutex> lk(mutex);
          const std::shared_ptr<RecordBatch>& to_write = write_queue_.front();
          Future<uint64_t> on_complete = write_futures_.front();
          uint64_t rows_to_write = to_write->num_rows();
          lk.unlock();
          Status status = writer_->Write(to_write);
          lk.lock();
          write_queue_.pop_front();
          write_futures_.pop_front();
          lk.unlock();
          if (!status.ok()) {
            on_complete.MarkFinished(status);
          } else {
            on_complete.MarkFinished(rows_to_write);
          }
          return rows_to_write;
        }));
  }

  Status DoFinish() {
    {
      std::lock_guard<std::mutex> lg(*visitors_mutex_);
      RETURN_NOT_OK(options_.writer_pre_finish(writer_.get()));
    }
    RETURN_NOT_OK(writer_->Finish());
    {
      std::lock_guard<std::mutex> lg(*visitors_mutex_);
      return options_.writer_post_finish(writer_.get());
    }
  }

  void Abort(Status err) {
    std::vector<Future<uint64_t>> futures_to_abort;
    Future<> old_running_task = running_task_;
    {
      std::lock_guard<std::mutex> lg(mutex);
      write_queue_.clear();
      futures_to_abort =
          std::vector<Future<uint64_t>>(write_futures_.begin(), write_futures_.end());
      write_futures_.clear();
      running_task_ = Future<>();
    }
    for (auto& fut : futures_to_abort) {
      fut.MarkFinished(err);
    }
    old_running_task.MarkFinished(std::move(err));
  }

  void Flush() {
    std::unique_lock<std::mutex> lk(mutex);
    FlushUnlocked(std::move(lk));
  }

  void FlushUnlocked(std::unique_lock<std::mutex> lk) {
    if (write_queue_.empty()) {
      Future<> old_running_task = running_task_;
      running_task_ = Future<>();
      lk.unlock();
      old_running_task.MarkFinished();
      return;
    }
    WriteNext().AddCallback([this](const Result<uint64_t>& res) {
      if (res.ok()) {
        Flush();
      } else {
        Abort(res.status());
      }
    });
  }

  const FileSystemDatasetWriteOptions& options_;
  std::mutex* visitors_mutex_;
  std::shared_ptr<FileWriter> writer_;
  std::mutex mutex;
  std::list<std::shared_ptr<RecordBatch>> write_queue_;
  std::list<Future<uint64_t>> write_futures_;
  Future<> running_task_;
};

struct WriteTask {
  std::string filename;
  uint64_t num_rows;
};

class DatasetWriterDirectoryQueue : public util::AsyncDestroyable {
 public:
  DatasetWriterDirectoryQueue(std::string directory, std::shared_ptr<Schema> schema,
                              const FileSystemDatasetWriteOptions& write_options,
                              Throttle* open_files_throttle, std::mutex* visitors_mutex)
      : directory_(std::move(directory)),
        schema_(std::move(schema)),
        write_options_(write_options),
        open_files_throttle_(open_files_throttle),
        visitors_mutex_(visitors_mutex) {}

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

  Future<WriteTask> StartWrite(const std::shared_ptr<RecordBatch>& batch) {
    rows_written_ += batch->num_rows();
    WriteTask task{current_filename_, static_cast<uint64_t>(batch->num_rows())};
    if (!latest_open_file_) {
      ARROW_ASSIGN_OR_RAISE(latest_open_file_, OpenFileQueue(current_filename_));
    }
    return latest_open_file_->Push(batch).Then([task] { return task; });
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
        file_writer_fut, write_options_, visitors_mutex_);
    RETURN_NOT_OK(task_group_.AddTask(
        file_queue->on_closed().Then([this] { open_files_throttle_->Release(1); })));
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
       const FileSystemDatasetWriteOptions& write_options, Throttle* open_files_throttle,
       std::shared_ptr<Schema> schema, std::string dir, std::mutex* visitors_mutex) {
    auto dir_queue = util::MakeUniqueAsync<DatasetWriterDirectoryQueue>(
        std::move(dir), std::move(schema), write_options, open_files_throttle,
        visitors_mutex);
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
  Throttle* open_files_throttle_;
  std::mutex* visitors_mutex_;
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

}  // namespace

class DatasetWriter::DatasetWriterImpl : public util::AsyncDestroyable {
 public:
  DatasetWriterImpl(FileSystemDatasetWriteOptions write_options, uint64_t max_rows_queued)
      : write_options_(std::move(write_options)),
        rows_in_flight_throttle_(max_rows_queued),
        open_files_throttle_(write_options_.max_open_files) {}

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
                  &task_group_, write_options_, &open_files_throttle_, batch->schema(),
                  dir, &visitors_mutex_);
            }));
    std::shared_ptr<DatasetWriterDirectoryQueue> dir_queue = dir_queue_itr->second;
    std::vector<Future<WriteTask>> scheduled_writes;
    Future<> backpressure;
    while (batch) {
      // Keep opening new files until batch is done.
      std::shared_ptr<RecordBatch> remainder;
      bool will_open_file = false;
      ARROW_ASSIGN_OR_RAISE(auto next_chunk, dir_queue->NextWritableChunk(
                                                 batch, &remainder, &will_open_file));

      backpressure = rows_in_flight_throttle_.Acquire(next_chunk->num_rows());
      if (!backpressure.is_finished()) {
        break;
      }
      if (will_open_file) {
        backpressure = open_files_throttle_.Acquire(1);
        if (!backpressure.is_finished()) {
          RETURN_NOT_OK(CloseLargestFile());
          break;
        }
      }
      scheduled_writes.push_back(dir_queue->StartWrite(next_chunk));
      batch = std::move(remainder);
      if (batch) {
        RETURN_NOT_OK(dir_queue->FinishCurrentFile());
      }
    }

    for (auto& scheduled_write : scheduled_writes) {
      RETURN_NOT_OK(task_group_.AddTask(scheduled_write.Then(
          [this](const WriteTask& write) {
            rows_in_flight_throttle_.Release(write.num_rows);
          },
          [this](const Status& err) { SetError(err); })));
      // The previously added callback could run immediately and set err_ so we check
      // it each time through the loop
      RETURN_NOT_OK(CheckError());
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
  Throttle rows_in_flight_throttle_;
  Throttle open_files_throttle_;
  std::unordered_map<std::string, std::shared_ptr<DatasetWriterDirectoryQueue>>
      directory_queues_;
  std::mutex mutex_;
  // A mutex to guard access to the visitor callbacks
  std::mutex visitors_mutex_;
  Status err_;
};

DatasetWriter::DatasetWriter(FileSystemDatasetWriteOptions write_options,
                             uint64_t max_rows_queued)
    : impl_(util::MakeUniqueAsync<DatasetWriterImpl>(std::move(write_options),
                                                     max_rows_queued)) {}

Result<std::unique_ptr<DatasetWriter>> DatasetWriter::Make(
    FileSystemDatasetWriteOptions write_options, uint64_t max_rows_queued) {
  RETURN_NOT_OK(ValidateBasenameTemplate(write_options.basename_template));
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
