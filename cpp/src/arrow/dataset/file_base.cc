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
#include "arrow/dataset/forest_internal.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/scanner_internal.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/io/compressed.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/util/compression.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/map.h"
#include "arrow/util/mutex.h"
#include "arrow/util/string.h"
#include "arrow/util/task_group.h"
#include "arrow/util/variant.h"

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

Result<std::shared_ptr<io::InputStream>> FileSource::OpenCompressed(
    util::optional<Compression::type> compression) const {
  ARROW_ASSIGN_OR_RAISE(auto file, Open());
  auto actual_compression = Compression::type::UNCOMPRESSED;
  if (!compression.has_value()) {
    // Guess compression from file extension
    auto extension = fs::internal::GetAbstractPathExtension(path());
    util::string_view file_path(path());
    if (extension == "gz") {
      actual_compression = Compression::type::GZIP;
    } else {
      auto maybe_compression = util::Codec::GetCompressionType(extension);
      if (maybe_compression.ok()) {
        ARROW_ASSIGN_OR_RAISE(actual_compression, maybe_compression);
      }
    }
  } else {
    actual_compression = compression.value();
  }
  if (actual_compression == Compression::type::UNCOMPRESSED) {
    return file;
  }
  ARROW_ASSIGN_OR_RAISE(auto codec, util::Codec::Create(actual_compression));
  return io::CompressedInputStream::Make(codec.get(), std::move(file));
}

Result<std::shared_ptr<FileFragment>> FileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Schema> physical_schema) {
  return MakeFragment(std::move(source), literal(true), std::move(physical_schema));
}

Result<std::shared_ptr<FileFragment>> FileFormat::MakeFragment(
    FileSource source, Expression partition_expression) {
  return MakeFragment(std::move(source), std::move(partition_expression), nullptr);
}

Result<std::shared_ptr<FileFragment>> FileFormat::MakeFragment(
    FileSource source, Expression partition_expression,
    std::shared_ptr<Schema> physical_schema) {
  return std::shared_ptr<FileFragment>(
      new FileFragment(std::move(source), shared_from_this(),
                       std::move(partition_expression), std::move(physical_schema)));
}

// TODO(ARROW-12355[CSV], ARROW-11772[IPC], ARROW-11843[Parquet]) The following
// implementation of ScanBatchesAsync is both ugly and terribly ineffecient.  Each of the
// formats should provide their own efficient implementation.
Result<RecordBatchGenerator> FileFormat::ScanBatchesAsync(
    const std::shared_ptr<ScanOptions>& scan_options,
    const std::shared_ptr<FileFragment>& file) {
  ARROW_ASSIGN_OR_RAISE(auto scan_task_it, ScanFile(scan_options, file));
  struct State {
    State(std::shared_ptr<ScanOptions> scan_options, ScanTaskIterator scan_task_it)
        : scan_options(std::move(scan_options)),
          scan_task_it(std::move(scan_task_it)),
          current_rb_it(),
          finished(false) {}

    std::shared_ptr<ScanOptions> scan_options;
    ScanTaskIterator scan_task_it;
    RecordBatchIterator current_rb_it;
    bool finished;
  };
  struct Generator {
    Future<std::shared_ptr<RecordBatch>> operator()() {
      while (!state->finished) {
        if (!state->current_rb_it) {
          RETURN_NOT_OK(PumpScanTask());
          if (state->finished) {
            return AsyncGeneratorEnd<std::shared_ptr<RecordBatch>>();
          }
        }
        ARROW_ASSIGN_OR_RAISE(auto next_batch, state->current_rb_it.Next());
        if (IsIterationEnd(next_batch)) {
          state->current_rb_it = RecordBatchIterator();
        } else {
          return Future<std::shared_ptr<RecordBatch>>::MakeFinished(next_batch);
        }
      }
      return AsyncGeneratorEnd<std::shared_ptr<RecordBatch>>();
    }
    Status PumpScanTask() {
      ARROW_ASSIGN_OR_RAISE(auto next_task, state->scan_task_it.Next());
      if (IsIterationEnd(next_task)) {
        state->finished = true;
      } else {
        ARROW_ASSIGN_OR_RAISE(state->current_rb_it, next_task->Execute());
      }
      return Status::OK();
    }
    std::shared_ptr<State> state;
  };
  return Generator{std::make_shared<State>(scan_options, std::move(scan_task_it))};
}

Result<std::shared_ptr<Schema>> FileFragment::ReadPhysicalSchemaImpl() {
  return format_->Inspect(source_);
}

Result<ScanTaskIterator> FileFragment::Scan(std::shared_ptr<ScanOptions> options) {
  auto self = std::dynamic_pointer_cast<FileFragment>(shared_from_this());
  return format_->ScanFile(options, self);
}

Result<RecordBatchGenerator> FileFragment::ScanBatchesAsync(
    const std::shared_ptr<ScanOptions>& options) {
  auto self = std::dynamic_pointer_cast<FileFragment>(shared_from_this());
  return format_->ScanBatchesAsync(options, self);
}

struct FileSystemDataset::FragmentSubtrees {
  // Forest for skipping fragments based on extracted subtree expressions
  Forest forest;
  // fragment indices and subtree expressions in forest order
  std::vector<util::Variant<int, Expression>> fragments_and_subtrees;
};

Result<std::shared_ptr<FileSystemDataset>> FileSystemDataset::Make(
    std::shared_ptr<Schema> schema, Expression root_partition,
    std::shared_ptr<FileFormat> format, std::shared_ptr<fs::FileSystem> filesystem,
    std::vector<std::shared_ptr<FileFragment>> fragments) {
  std::shared_ptr<FileSystemDataset> out(
      new FileSystemDataset(std::move(schema), std::move(root_partition)));
  out->format_ = std::move(format);
  out->filesystem_ = std::move(filesystem);
  out->fragments_ = std::move(fragments);
  out->SetupSubtreePruning();
  return out;
}

Result<std::shared_ptr<Dataset>> FileSystemDataset::ReplaceSchema(
    std::shared_ptr<Schema> schema) const {
  RETURN_NOT_OK(CheckProjectable(*schema_, *schema));
  return Make(std::move(schema), partition_expression_, format_, filesystem_, fragments_);
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
    if (partition != literal(true)) {
      repr += ": " + partition.ToString();
    }
  }

  return repr;
}

void FileSystemDataset::SetupSubtreePruning() {
  subtrees_ = std::make_shared<FragmentSubtrees>();
  SubtreeImpl impl;

  auto encoded = impl.EncodeFragments(fragments_);

  std::sort(encoded.begin(), encoded.end(),
            [](const SubtreeImpl::Encoded& l, const SubtreeImpl::Encoded& r) {
              const auto cmp = l.partition_expression.compare(r.partition_expression);
              if (cmp != 0) {
                return cmp < 0;
              }
              // Equal partition expressions; sort encodings with fragment indices after
              // encodings without
              return (l.fragment_index ? 1 : 0) < (r.fragment_index ? 1 : 0);
            });

  for (const auto& e : encoded) {
    if (e.fragment_index) {
      subtrees_->fragments_and_subtrees.emplace_back(*e.fragment_index);
    } else {
      subtrees_->fragments_and_subtrees.emplace_back(impl.GetSubtreeExpression(e));
    }
  }

  subtrees_->forest = Forest(static_cast<int>(encoded.size()), [&](int l, int r) {
    if (encoded[l].fragment_index) {
      // Fragment: not an ancestor.
      return false;
    }

    const auto& ancestor = encoded[l].partition_expression;
    const auto& descendant = encoded[r].partition_expression;

    if (descendant.size() >= ancestor.size()) {
      return std::equal(ancestor.begin(), ancestor.end(), descendant.begin());
    }
    return false;
  });
}

Result<FragmentIterator> FileSystemDataset::GetFragmentsImpl(Expression predicate) {
  if (predicate == literal(true)) {
    // trivial predicate; skip subtree pruning
    return MakeVectorIterator(FragmentVector(fragments_.begin(), fragments_.end()));
  }

  std::vector<int> fragment_indices;

  std::vector<Expression> predicates{predicate};
  RETURN_NOT_OK(subtrees_->forest.Visit(
      [&](Forest::Ref ref) -> Result<bool> {
        if (auto fragment_index =
                util::get_if<int>(&subtrees_->fragments_and_subtrees[ref.i])) {
          fragment_indices.push_back(*fragment_index);
          return false;
        }

        const auto& subtree_expr =
            util::get<Expression>(subtrees_->fragments_and_subtrees[ref.i]);
        ARROW_ASSIGN_OR_RAISE(auto simplified,
                              SimplifyWithGuarantee(predicates.back(), subtree_expr));

        if (!simplified.IsSatisfiable()) {
          return false;
        }

        predicates.push_back(std::move(simplified));
        return true;
      },
      [&](Forest::Ref ref) { predicates.pop_back(); }));

  std::sort(fragment_indices.begin(), fragment_indices.end());

  FragmentVector fragments(fragment_indices.size());
  std::transform(fragment_indices.begin(), fragment_indices.end(), fragments.begin(),
                 [this](int i) { return fragments_[i]; });

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

Status FileWriter::Finish() {
  RETURN_NOT_OK(FinishInternal());
  return destination_->Close();
}

namespace {

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

struct WriteState {
  explicit WriteState(FileSystemDatasetWriteOptions write_options)
      : write_options(std::move(write_options)) {}

  FileSystemDatasetWriteOptions write_options;
  util::Mutex mutex;
  std::unordered_map<std::string, std::unique_ptr<WriteQueue>> queues;
};

Status WriteNextBatch(WriteState& state, const std::shared_ptr<Fragment>& fragment,
                      std::shared_ptr<RecordBatch> batch) {
  ARROW_ASSIGN_OR_RAISE(auto groups, state.write_options.partitioning->Partition(batch));
  batch.reset();  // drop to hopefully conserve memory

  if (groups.batches.size() > static_cast<size_t>(state.write_options.max_partitions)) {
    return Status::Invalid("Fragment would be written into ", groups.batches.size(),
                           " partitions. This exceeds the maximum of ",
                           state.write_options.max_partitions);
  }

  std::unordered_set<WriteQueue*> need_flushed;
  for (size_t i = 0; i < groups.batches.size(); ++i) {
    auto partition_expression =
        and_(std::move(groups.expressions[i]), fragment->partition_expression());
    auto batch = std::move(groups.batches[i]);

    ARROW_ASSIGN_OR_RAISE(auto part,
                          state.write_options.partitioning->Format(partition_expression));

    WriteQueue* queue;
    {
      // lookup the queue to which batch should be appended
      auto queues_lock = state.mutex.Lock();

      queue = internal::GetOrInsertGenerated(
                  &state.queues, std::move(part),
                  [&](const std::string& emplaced_part) {
                    // lookup in `queues` also failed,
                    // generate a new WriteQueue
                    size_t queue_index = state.queues.size() - 1;

                    return internal::make_unique<WriteQueue>(emplaced_part, queue_index,
                                                             batch->schema());
                  })
                  ->second.get();
    }

    queue->Push(std::move(batch));
    need_flushed.insert(queue);
  }

  // flush all touched WriteQueues
  for (auto queue : need_flushed) {
    RETURN_NOT_OK(queue->Flush(state.write_options));
  }
  return Status::OK();
}

Future<> WriteInternal(const ScanOptions& scan_options, WriteState& state,
                       ScanTaskVector scan_tasks, internal::Executor* cpu_executor) {
  // Store a mapping from partitions (represened by their formatted partition expressions)
  // to a WriteQueue which flushes batches into that partition's output file. In principle
  // any thread could produce a batch for any partition, so each task alternates between
  // pushing batches and flushing them to disk.
  std::vector<Future<>> scan_futs;
  auto task_group = scan_options.TaskGroup();

  for (const auto& scan_task : scan_tasks) {
    task_group->Append([&, scan_task] {
      ARROW_ASSIGN_OR_RAISE(auto batches, scan_task->Execute());

      for (auto maybe_batch : batches) {
        ARROW_ASSIGN_OR_RAISE(auto batch, maybe_batch);
        RETURN_NOT_OK(WriteNextBatch(state, scan_task->fragment(), std::move(batch)));
      }

      return Status::OK();
    });
  }
  scan_futs.push_back(task_group->FinishAsync());
  return AllComplete(scan_futs);
}

}  // namespace

Status FileSystemDataset::Write(const FileSystemDatasetWriteOptions& write_options,
                                std::shared_ptr<Scanner> scanner) {
  RETURN_NOT_OK(ValidateBasenameTemplate(write_options.basename_template));

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

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#elif defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4996)
#endif

  // TODO: (ARROW-11782/ARROW-12288) Remove calls to Scan()
  ARROW_ASSIGN_OR_RAISE(auto scan_task_it, scanner->Scan());
  ARROW_ASSIGN_OR_RAISE(ScanTaskVector scan_tasks, scan_task_it.ToVector());

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#elif defined(_MSC_VER)
#pragma warning(pop)
#endif

  WriteState state(write_options);
  auto res = internal::RunSynchronously<arrow::detail::Empty>(
      [&](internal::Executor* cpu_executor) -> Future<> {
        return WriteInternal(*scanner->options(), state, std::move(scan_tasks),
                             cpu_executor);
      },
      scanner->options()->use_threads);
  RETURN_NOT_OK(res);

  auto task_group = scanner->options()->TaskGroup();
  for (const auto& part_queue : state.queues) {
    task_group->Append([&] { return part_queue.second->writer()->Finish(); });
  }
  return task_group->Finish();
}

}  // namespace dataset
}  // namespace arrow
