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

#include "arrow/dataset/file_base.h"

#include <algorithm>
#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/scanner_internal.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/map.h"
#include "arrow/util/mutex.h"
#include "arrow/util/string.h"
#include "arrow/util/task_group.h"

namespace arrow {
namespace dataset {

Result<std::shared_ptr<io::RandomAccessFile>> FileSource::Open() const {
  if (filesystem_) {
    return filesystem_->OpenInputFile(file_info_);
  }

  if (buffer_) {
    return std::make_shared<io::BufferReader>(buffer_);
  }

  return custom_open_();
}

Result<std::shared_ptr<FileFragment>> FileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Schema> physical_schema) {
  return MakeFragment(std::move(source), scalar(true), std::move(physical_schema));
}

Result<std::shared_ptr<FileFragment>> FileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Expression> partition_expression) {
  return MakeFragment(std::move(source), std::move(partition_expression), nullptr);
}

Result<std::shared_ptr<FileFragment>> FileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Expression> partition_expression,
    std::shared_ptr<Schema> physical_schema) {
  return std::shared_ptr<FileFragment>(
      new FileFragment(std::move(source), shared_from_this(),
                       std::move(partition_expression), std::move(physical_schema)));
}

Result<std::shared_ptr<Schema>> FileFragment::ReadPhysicalSchemaImpl() {
  return format_->Inspect(source_);
}

Result<ScanTaskIterator> FileFragment::Scan(std::shared_ptr<ScanOptions> options,
                                            std::shared_ptr<ScanContext> context) {
  return format_->ScanFile(std::move(options), std::move(context), this);
}

FileSystemDataset::FileSystemDataset(std::shared_ptr<Schema> schema,
                                     std::shared_ptr<Expression> root_partition,
                                     std::shared_ptr<FileFormat> format,
                                     std::shared_ptr<fs::FileSystem> filesystem,
                                     std::vector<std::shared_ptr<FileFragment>> fragments)
    : Dataset(std::move(schema), std::move(root_partition)),
      format_(std::move(format)),
      filesystem_(std::move(filesystem)),
      fragments_(std::move(fragments)) {}

Result<std::shared_ptr<FileSystemDataset>> FileSystemDataset::Make(
    std::shared_ptr<Schema> schema, std::shared_ptr<Expression> root_partition,
    std::shared_ptr<FileFormat> format, std::shared_ptr<fs::FileSystem> filesystem,
    std::vector<std::shared_ptr<FileFragment>> fragments) {
  return std::shared_ptr<FileSystemDataset>(new FileSystemDataset(
      std::move(schema), std::move(root_partition), std::move(format),
      std::move(filesystem), std::move(fragments)));
}

Result<std::shared_ptr<Dataset>> FileSystemDataset::ReplaceSchema(
    std::shared_ptr<Schema> schema) const {
  RETURN_NOT_OK(CheckProjectable(*schema_, *schema));
  return std::shared_ptr<Dataset>(new FileSystemDataset(
      std::move(schema), partition_expression_, format_, filesystem_, fragments_));
}

std::vector<std::string> FileSystemDataset::files() const {
  std::vector<std::string> files;

  for (const auto& fragment : fragments_) {
    files.push_back(fragment->source().path());
  }

  return files;
}

std::string FileSystemDataset::ToString() const {
  std::string repr = "FileSystemDataset:";

  if (fragments_.empty()) {
    return repr + " []";
  }

  for (const auto& fragment : fragments_) {
    repr += "\n" + fragment->source().path();

    const auto& partition = fragment->partition_expression();
    if (!partition->Equals(true)) {
      repr += ": " + partition->ToString();
    }
  }

  return repr;
}

FragmentIterator FileSystemDataset::GetFragmentsImpl(
    std::shared_ptr<Expression> predicate) {
  FragmentVector fragments;

  for (const auto& fragment : fragments_) {
    if (predicate->IsSatisfiableWith(fragment->partition_expression())) {
      fragments.push_back(fragment);
    }
  }

  return MakeVectorIterator(std::move(fragments));
}

Status FileWriter::Write(RecordBatchReader* batches) {
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto batch, batches->Next());
    if (batch == nullptr) break;
    RETURN_NOT_OK(Write(batch));
  }
  return Status::OK();
}

constexpr util::string_view kIntegerToken = "{i}";

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

/// WriteQueue allows batches to be pushed from multiple threads while another thread
/// flushes some to disk.
class WriteQueue {
 public:
  WriteQueue(std::string partition_expression, size_t index,
             std::shared_ptr<Schema> schema)
      : partition_expression_(std::move(partition_expression)),
        index_(index),
        schema_(std::move(schema)) {}

  // Push a batch into the writer's queue of pending writes.
  void Push(std::shared_ptr<RecordBatch> batch) {
    auto push_lock = push_mutex_.Lock();
    pending_.push_back(std::move(batch));
  }

  // Flush all pending batches, or return immediately if another thread is already
  // flushing this queue.
  Status Flush(const FileSystemDatasetWriteOptions& write_options) {
    if (auto writer_lock = writer_mutex_.TryLock()) {
      if (writer_ == nullptr) {
        // FileWriters are opened lazily to avoid blocking access to a scan-wide queue set
        RETURN_NOT_OK(OpenWriter(write_options));
      }

      while (true) {
        std::shared_ptr<RecordBatch> batch;
        {
          auto push_lock = push_mutex_.Lock();
          if (pending_.empty()) {
            // Ensure the writer_lock is released before the push_lock. Otherwise another
            // thread might successfully Push() a batch but then fail to Flush() it since
            // the writer_lock is still held, leaving an unflushed batch in pending_.
            writer_lock.Unlock();
            break;
          }
          batch = std::move(pending_.front());
          pending_.pop_front();
        }
        RETURN_NOT_OK(writer_->Write(batch));
      }
    }
    return Status::OK();
  }

  const std::shared_ptr<FileWriter>& writer() const { return writer_; }

 private:
  Status OpenWriter(const FileSystemDatasetWriteOptions& write_options) {
    auto dir =
        fs::internal::EnsureTrailingSlash(write_options.base_dir) + partition_expression_;

    auto basename = internal::Replace(write_options.basename_template, kIntegerToken,
                                      std::to_string(index_));
    if (!basename) {
      return Status::Invalid("string interpolation of basename template failed");
    }

    auto path = fs::internal::ConcatAbstractPath(dir, *basename);

    RETURN_NOT_OK(write_options.filesystem->CreateDir(dir));
    ARROW_ASSIGN_OR_RAISE(auto destination,
                          write_options.filesystem->OpenOutputStream(path));

    ARROW_ASSIGN_OR_RAISE(
        writer_, write_options.format()->MakeWriter(std::move(destination), schema_,
                                                    write_options.file_write_options));
    return Status::OK();
  }

  util::Mutex writer_mutex_;
  std::shared_ptr<FileWriter> writer_;

  util::Mutex push_mutex_;
  std::deque<std::shared_ptr<RecordBatch>> pending_;

  // The (formatted) partition expression to which this queue corresponds
  std::string partition_expression_;

  size_t index_;

  std::shared_ptr<Schema> schema_;
};

Status FileSystemDataset::Write(const FileSystemDatasetWriteOptions& write_options,
                                std::shared_ptr<Scanner> scanner) {
  RETURN_NOT_OK(ValidateBasenameTemplate(write_options.basename_template));

  auto task_group = scanner->context()->TaskGroup();

  // Things we'll un-lazy for the sake of simplicity, with the tradeoff they represent:
  //
  // - Fragment iteration. Keeping this lazy would allow us to start partitioning/writing
  //   any fragments we have before waiting for discovery to complete. This isn't
  //   currently implemented for FileSystemDataset anyway: ARROW-8613
  //
  // - ScanTask iteration. Keeping this lazy would save some unnecessary blocking when
  //   writing Fragments which produce scan tasks slowly. No Fragments do this.
  //
  // NB: neither of these will have any impact whatsoever on the common case of writing
  //     an in-memory table to disk.
  ARROW_ASSIGN_OR_RAISE(FragmentVector fragments, scanner->GetFragments().ToVector());
  ScanTaskVector scan_tasks;
  std::vector<const Fragment*> fragment_for_task;

  // Avoid contention with multithreaded readers
  auto context = std::make_shared<ScanContext>(*scanner->context());
  context->use_threads = false;

  for (const auto& fragment : fragments) {
    auto options = std::make_shared<ScanOptions>(*scanner->options());
    ARROW_ASSIGN_OR_RAISE(auto scan_task_it,
                          Scanner(fragment, std::move(options), context).Scan());
    for (auto maybe_scan_task : scan_task_it) {
      ARROW_ASSIGN_OR_RAISE(auto scan_task, maybe_scan_task);
      scan_tasks.push_back(std::move(scan_task));
      fragment_for_task.push_back(fragment.get());
    }
  }

  // Store a mapping from partitions (represened by their formatted partition expressions)
  // to a WriteQueue which flushes batches into that partition's output file. In principle
  // any thread could produce a batch for any partition, so each task alternates between
  // pushing batches and flushing them to disk.
  util::Mutex queues_mutex;
  std::unordered_map<std::string, std::unique_ptr<WriteQueue>> queues;

  auto fragment_for_task_it = fragment_for_task.begin();
  for (const auto& scan_task : scan_tasks) {
    const Fragment* fragment = *fragment_for_task_it++;

    task_group->Append([&, scan_task, fragment] {
      ARROW_ASSIGN_OR_RAISE(auto batches, scan_task->Execute());

      for (auto maybe_batch : batches) {
        ARROW_ASSIGN_OR_RAISE(auto batch, maybe_batch);
        ARROW_ASSIGN_OR_RAISE(auto groups, write_options.partitioning->Partition(batch));
        batch.reset();  // drop to hopefully conserve memory

        std::unordered_set<WriteQueue*> need_flushed;
        for (size_t i = 0; i < groups.batches.size(); ++i) {
          AndExpression partition_expression(std::move(groups.expressions[i]),
                                             fragment->partition_expression());
          auto batch = std::move(groups.batches[i]);

          ARROW_ASSIGN_OR_RAISE(auto part,
                                write_options.partitioning->Format(partition_expression));

          WriteQueue* queue;
          {
            // lookup the queue to which batch should be appended
            auto queues_lock = queues_mutex.Lock();

            queue = internal::GetOrInsertGenerated(
                        &queues, std::move(part),
                        [&](const std::string& emplaced_part) {
                          // lookup in `queues` also failed,
                          // generate a new WriteQueue
                          size_t queue_index = queues.size() - 1;

                          return internal::make_unique<WriteQueue>(
                              emplaced_part, queue_index, batch->schema());
                        })
                        ->second.get();
          }

          queue->Push(std::move(batch));
          need_flushed.insert(queue);
        }

        // flush all touched WriteQueues
        for (auto queue : need_flushed) {
          RETURN_NOT_OK(queue->Flush(write_options));
        }
      }

      return Status::OK();
    });
  }
  RETURN_NOT_OK(task_group->Finish());

  task_group = scanner->context()->TaskGroup();
  for (const auto& part_queue : queues) {
    task_group->Append([&] { return part_queue.second->writer()->Finish(); });
  }
  return task_group->Finish();
}

}  // namespace dataset
}  // namespace arrow
