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

namespace {

constexpr util::string_view kIntegerToken = "{i}";

class DatasetWriterStatistics {
 public:
  DatasetWriterStatistics(uint64_t max_rows_in_flight, uint32_t max_files_in_flight)
      : max_rows_in_flight_(max_rows_in_flight),
        max_files_in_flight_(max_files_in_flight) {}

  bool CanWrite(const std::shared_ptr<RecordBatch>& record_batch,
                const std::string& filename) {
    std::lock_guard<std::mutex> lg(mutex_);
    uint64_t rows = record_batch->num_rows();
    DCHECK_LT(rows, max_rows_in_flight_);

    if (rows_in_flight_ + rows > max_rows_in_flight_) {
      rows_in_waiting_ = rows;
      backpressure_ = Future<>::Make();
      return false;
    }
    return true;
  }

  bool CanOpenFile() {
    std::lock_guard<std::mutex> lg(mutex_);
    if (files_in_flight_ == max_files_in_flight_) {
      waiting_on_file_ = true;
      backpressure_ = Future<>::Make();
      return false;
    }
    return true;
  }

  void RecordWriteStart(uint64_t num_rows) {
    std::lock_guard<std::mutex> lg(mutex_);
    rows_in_flight_ += num_rows;
  }

  void RecordFileStart() {
    std::lock_guard<std::mutex> lg(mutex_);
    files_in_flight_++;
  }

  void RecordFileFinished() {
    std::unique_lock<std::mutex> lk(mutex_);
    files_in_flight_--;
    FreeBackpressureIfPossible(std::move(lk));
  }

  void RecordWriteFinish(uint64_t num_rows) {
    std::unique_lock<std::mutex> lk(mutex_);
    rows_in_flight_ -= num_rows;
    FreeBackpressureIfPossible(std::move(lk));
  }

  Future<> backpressure() {
    std::lock_guard<std::mutex> lg(mutex_);
    return backpressure_;
  }

 private:
  void FreeBackpressureIfPossible(std::unique_lock<std::mutex>&& lk) {
    if (waiting_on_file_) {
      if (files_in_flight_ < max_files_in_flight_) {
        waiting_on_file_ = false;
      }
    }

    bool waiting_on_rows = true;
    if (rows_in_flight_ > 0) {
      if (rows_in_flight_ + rows_in_waiting_ < max_rows_in_flight_) {
        rows_in_waiting_ = 0;
        waiting_on_rows = false;
      }
    } else {
      waiting_on_rows = false;
    }

    if (backpressure_.is_valid() && !waiting_on_rows && !waiting_on_file_) {
      Future<> old_backpressure = backpressure_;
      backpressure_ = Future<>();
      lk.unlock();
      old_backpressure.MarkFinished();
    }
  }

  uint64_t max_rows_in_flight_;
  uint32_t max_files_in_flight_;

  Future<> backpressure_;
  uint64_t rows_in_flight_ = 0;
  uint64_t rows_in_waiting_ = 0;
  uint32_t files_in_flight_ = 0;
  bool waiting_on_file_ = false;
  std::mutex mutex_;
};

class DatasetWriterFileQueue : public util::AsyncCloseable {
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

  Future<> DoClose() override {
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

class DatasetWriterDirectoryQueue : public util::AsyncCloseable {
 public:
  DatasetWriterDirectoryQueue(util::AsyncCloseable* parent, std::string directory,
                              std::shared_ptr<Schema> schema,
                              const FileSystemDatasetWriteOptions& write_options,
                              DatasetWriterStatistics* write_statistics,
                              std::mutex* visitors_mutex)
      : util::AsyncCloseable(parent),
        directory_(std::move(directory)),
        schema_(std::move(schema)),
        write_options_(write_options),
        write_statistics_(write_statistics),
        visitors_mutex_(visitors_mutex) {}

  Result<std::shared_ptr<RecordBatch>> NextWritableChunk(
      std::shared_ptr<RecordBatch> batch, std::shared_ptr<RecordBatch>* remainder,
      bool* will_open_file) {
    RETURN_NOT_OK(CheckClosed());
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
    RETURN_NOT_OK(CheckClosed());
    rows_written_ += batch->num_rows();
    WriteTask task{current_filename_, static_cast<uint64_t>(batch->num_rows())};
    if (!latest_open_file_) {
      latest_open_file_ = OpenFileQueue(current_filename_);
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

  std::shared_ptr<DatasetWriterFileQueue> OpenFileQueue(const std::string& filename) {
    write_statistics_->RecordFileStart();
    Future<std::shared_ptr<FileWriter>> file_writer_fut =
        init_future_.Then([this, filename] {
          ::arrow::internal::Executor* io_executor =
              write_options_.filesystem->io_context().executor();
          return DeferNotOk(
              io_executor->Submit([this, filename]() { return OpenWriter(filename); }));
        });
    auto file_queue = nursery_->MakeSharedCloseable<DatasetWriterFileQueue>(
        file_writer_fut, write_options_, visitors_mutex_);
    AddDependentTask(
        file_queue->OnClosed().Then([this] { write_statistics_->RecordFileFinished(); }));
    return file_queue;
  }

  const std::string& current_filename() const { return current_filename_; }
  uint64_t rows_written() const { return rows_written_; }

  void PrepareDirectory() {
    init_future_ =
        DeferNotOk(write_options_.filesystem->io_context().executor()->Submit([this] {
          RETURN_NOT_OK(write_options_.filesystem->CreateDir(directory_));
          if (write_options_.existing_data_behavior == kDeleteMatchingPartitions) {
            fs::FileSelector selector;
            selector.base_dir = directory_;
            selector.recursive = true;
            return write_options_.filesystem->DeleteFiles(selector);
          }
          return Status::OK();
        }));
  }

  Future<> DoClose() override { return FinishCurrentFile(); }

  static Result<std::shared_ptr<DatasetWriterDirectoryQueue>> Make(
      util::AsyncCloseable* parent, util::Nursery* nursery,
      const FileSystemDatasetWriteOptions& write_options,
      DatasetWriterStatistics* write_statistics, std::shared_ptr<Schema> schema,
      std::string dir, std::mutex* visitors_mutex) {
    auto dir_queue = nursery->MakeSharedCloseable<DatasetWriterDirectoryQueue>(
        parent, std::move(dir), std::move(schema), write_options, write_statistics,
        visitors_mutex);
    dir_queue->PrepareDirectory();
    ARROW_ASSIGN_OR_RAISE(dir_queue->current_filename_, dir_queue->GetNextFilename());
    return dir_queue;
  }

 private:
  std::string directory_;
  std::shared_ptr<Schema> schema_;
  const FileSystemDatasetWriteOptions& write_options_;
  DatasetWriterStatistics* write_statistics_;
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
  return Status::OK();
}

Status EnsureDestinationValid(const FileSystemDatasetWriteOptions& options) {
  if (options.existing_data_behavior == kError) {
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
          " as the directory is not empty and existing_data_behavior is kError");
    }
  }
  return Status::OK();
}

}  // namespace

class DatasetWriter::DatasetWriterImpl : public util::AsyncCloseable {
 public:
  DatasetWriterImpl(FileSystemDatasetWriteOptions write_options, uint64_t max_rows_queued)
      : write_options_(std::move(write_options)),
        statistics_(max_rows_queued, write_options.max_open_files) {}

  Future<> WriteRecordBatch(std::shared_ptr<RecordBatch> batch,
                            const std::string& directory) {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckError());
    if (!directory.empty()) {
      auto full_path =
          fs::internal::ConcatAbstractPath(write_options_.base_dir, directory);
      return DoWriteRecordBatch(std::move(batch), full_path);
    } else {
      return DoWriteRecordBatch(std::move(batch), write_options_.base_dir);
    }
  }

  Future<> DoClose() override {
    directory_queues_.clear();
    return CheckError();
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
              return DatasetWriterDirectoryQueue::Make(this, nursery_, write_options_,
                                                       &statistics_, batch->schema(), dir,
                                                       &visitors_mutex_);
            }));
    std::shared_ptr<DatasetWriterDirectoryQueue> dir_queue = dir_queue_itr->second;
    std::vector<Future<WriteTask>> scheduled_writes;
    bool hit_backpressure = false;
    while (batch) {
      // Keep opening new files until batch is done.
      std::shared_ptr<RecordBatch> remainder;
      bool will_open_file = false;
      ARROW_ASSIGN_OR_RAISE(auto next_chunk, dir_queue->NextWritableChunk(
                                                 batch, &remainder, &will_open_file));

      if (statistics_.CanWrite(next_chunk, dir_queue->current_filename()) &&
          (!will_open_file || statistics_.CanOpenFile())) {
        statistics_.RecordWriteStart(next_chunk->num_rows());
        scheduled_writes.push_back(dir_queue->StartWrite(next_chunk));
        batch = std::move(remainder);
        if (batch) {
          ARROW_RETURN_NOT_OK(dir_queue->FinishCurrentFile());
        }
      } else {
        if (!statistics_.CanOpenFile()) {
          ARROW_RETURN_NOT_OK(CloseLargestFile());
        }
        hit_backpressure = true;
        break;
      }
    }

    for (auto& scheduled_write : scheduled_writes) {
      // One of the below callbacks could run immediately and set err_ so we check
      // it each time through the loop
      RETURN_NOT_OK(CheckError());
      AddDependentTask(scheduled_write.Then(
          [this](const WriteTask& write) {
            statistics_.RecordWriteFinish(write.num_rows);
          },
          [this](const Status& err) { SetError(err); }));
    }
    if (hit_backpressure) {
      Future<> maybe_backpressure = statistics_.backpressure();
      // It's possible the backpressure was relieved since we last checked
      if (maybe_backpressure.is_valid()) {
        return maybe_backpressure.Then(
            [this, batch, directory] { return DoWriteRecordBatch(batch, directory); });
      } else {
        return DoWriteRecordBatch(batch, directory);
      }
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

  FileSystemDatasetWriteOptions write_options_;
  DatasetWriterStatistics statistics_;
  std::unordered_map<std::string, std::shared_ptr<DatasetWriterDirectoryQueue>>
      directory_queues_;
  std::mutex mutex_;
  // A mutex to guard access to the visitor callbacks
  std::mutex visitors_mutex_;
  Status err_;
};

DatasetWriter::DatasetWriter(FileSystemDatasetWriteOptions write_options,
                             uint64_t max_rows_queued)
    : AsyncCloseablePimpl(),
      impl_(new DatasetWriterImpl(std::move(write_options), max_rows_queued)) {
  AsyncCloseablePimpl::Init(impl_.get());
}

Result<std::unique_ptr<DatasetWriter, util::DestroyingDeleter<DatasetWriter>>>
DatasetWriter::Make(util::Nursery* nursery, FileSystemDatasetWriteOptions write_options,
                    uint64_t max_rows_queued) {
  RETURN_NOT_OK(ValidateBasenameTemplate(write_options.basename_template));
  RETURN_NOT_OK(EnsureDestinationValid(write_options));
  return nursery->MakeUniqueCloseable<DatasetWriter>(std::move(write_options),
                                                     max_rows_queued);
}

DatasetWriter::~DatasetWriter() = default;

Future<> DatasetWriter::WriteRecordBatch(std::shared_ptr<RecordBatch> batch,
                                         const std::string& directory) {
  return impl_->WriteRecordBatch(std::move(batch), directory);
}

}  // namespace dataset
}  // namespace arrow
