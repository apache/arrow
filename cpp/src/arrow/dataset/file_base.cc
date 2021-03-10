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
#include "arrow/dataset/expression_internal.h"
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
#include "arrow/util/optional.h"
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

Result<std::shared_ptr<Schema>> FileFragment::ReadPhysicalSchemaImpl() {
  return format_->Inspect(source_);
}

Result<ScanTaskIterator> FileFragment::Scan(std::shared_ptr<ScanOptions> options,
                                            std::shared_ptr<ScanContext> context) {
  auto self = std::dynamic_pointer_cast<FileFragment>(shared_from_this());
  return format_->ScanFile(std::move(options), std::move(context), self);
}

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

namespace {

// Helper class for efficiently detecting subtrees given fragment partition expressions.
// Partition expressions are broken into conjunction members and each member dictionary
// encoded to impose a sortable ordering. In addition, subtrees are generated which span
// groups of fragments and nested subtrees. After encoding each fragment is guaranteed to
// be a descendant of at least one subtree. For example, given fragments in a
// HivePartitioning with paths:
//
//   /num=0/al=eh/dat.par
//   /num=0/al=be/dat.par
//   /num=1/al=eh/dat.par
//   /num=1/al=be/dat.par
//
// The following subtrees will be introduced:
//
//   /num=0/
//   /num=0/al=eh/
//   /num=0/al=eh/dat.par
//   /num=0/al=be/
//   /num=0/al=be/dat.par
//   /num=1/
//   /num=1/al=eh/
//   /num=1/al=eh/dat.par
//   /num=1/al=be/
//   /num=1/al=be/dat.par
struct SubtreeImpl {
  using expression_code = char32_t;
  using expression_codes = std::basic_string<expression_code>;

  std::unordered_map<Expression, expression_code, Expression::Hash> expr_to_code_;
  std::vector<Expression> code_to_expr_;
  std::unordered_set<expression_codes> subtree_exprs_;

  // An encoded fragment (if fragment_index is set) or subtree.
  struct Encoded {
    util::optional<int> fragment_index;
    expression_codes partition_expression;
  };

  expression_code GetOrInsert(const Expression& expr) {
    auto next_code = static_cast<int>(expr_to_code_.size());
    auto it_success = expr_to_code_.emplace(expr, next_code);

    if (it_success.second) {
      code_to_expr_.push_back(expr);
    }
    return it_success.first->second;
  }

  void EncodeConjunctionMembers(const Expression& expr, expression_codes* codes) {
    if (auto call = expr.call()) {
      if (call->function_name == "and_kleene") {
        // expr is a conjunction, encode its arguments
        EncodeConjunctionMembers(call->arguments[0], codes);
        EncodeConjunctionMembers(call->arguments[1], codes);
        return;
      }
    }
    // expr is not a conjunction, encode it whole
    codes->push_back(GetOrInsert(expr));
  }

  Expression GetSubtreeExpression(const Encoded& encoded_subtree) {
    // Filters will already be simplified by all of a subtree's ancestors, so
    // we only need to simplify the filter by the trailing conjunction member
    // of each subtree.
    return code_to_expr_[encoded_subtree.partition_expression.back()];
  }

  void GenerateSubtrees(expression_codes partition_expression,
                        std::vector<Encoded>* encoded) {
    while (!partition_expression.empty()) {
      if (subtree_exprs_.insert(partition_expression).second) {
        Encoded encoded_subtree{/*fragment_index=*/util::nullopt, partition_expression};
        encoded->push_back(std::move(encoded_subtree));
      }
      partition_expression.resize(partition_expression.size() - 1);
    }
  }

  void EncodeOneFragment(int fragment_index, const Fragment& fragment,
                         std::vector<Encoded>* encoded) {
    Encoded encoded_fragment{fragment_index, {}};

    EncodeConjunctionMembers(fragment.partition_expression(),
                             &encoded_fragment.partition_expression);

    GenerateSubtrees(encoded_fragment.partition_expression, encoded);

    encoded->push_back(std::move(encoded_fragment));
  }

  template <typename Fragments>
  std::vector<Encoded> EncodeFragments(const Fragments& fragments) {
    std::vector<Encoded> encoded;
    for (size_t i = 0; i < fragments.size(); ++i) {
      EncodeOneFragment(static_cast<int>(i), *fragments[i], &encoded);
    }
    return encoded;
  }
};

}  // namespace

void FileSystemDataset::SetupSubtreePruning() {
  SubtreeImpl impl;

  auto encoded = impl.EncodeFragments(fragments_);

  std::sort(encoded.begin(), encoded.end(),
            [](const SubtreeImpl::Encoded& l, const SubtreeImpl::Encoded& r) {
              return l.partition_expression < r.partition_expression;
            });

  for (const auto& e : encoded) {
    if (e.fragment_index) {
      fragments_and_subtrees_.emplace_back(*e.fragment_index);
    } else {
      fragments_and_subtrees_.emplace_back(impl.GetSubtreeExpression(e));
    }
  }

  forest_ = Forest(static_cast<int>(encoded.size()), [&](int l, int r) {
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
  RETURN_NOT_OK(forest_.Visit(
      [&](Forest::Ref ref) -> Result<bool> {
        if (auto fragment_index = util::get_if<int>(&fragments_and_subtrees_[ref.i])) {
          fragment_indices.push_back(*fragment_index);
          return false;
        }

        const auto& subtree_expr = util::get<Expression>(fragments_and_subtrees_[ref.i]);
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

}  // namespace

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
  ARROW_ASSIGN_OR_RAISE(auto fragment_it, scanner->GetFragments());
  ARROW_ASSIGN_OR_RAISE(FragmentVector fragments, fragment_it.ToVector());
  ScanTaskVector scan_tasks;

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
    }
  }

  // Store a mapping from partitions (represened by their formatted partition expressions)
  // to a WriteQueue which flushes batches into that partition's output file. In principle
  // any thread could produce a batch for any partition, so each task alternates between
  // pushing batches and flushing them to disk.
  util::Mutex queues_mutex;
  std::unordered_map<std::string, std::unique_ptr<WriteQueue>> queues;

  for (const auto& scan_task : scan_tasks) {
    task_group->Append([&, scan_task] {
      ARROW_ASSIGN_OR_RAISE(auto batches, scan_task->Execute());

      for (auto maybe_batch : batches) {
        ARROW_ASSIGN_OR_RAISE(auto batch, maybe_batch);
        ARROW_ASSIGN_OR_RAISE(auto groups, write_options.partitioning->Partition(batch));
        batch.reset();  // drop to hopefully conserve memory

        if (groups.batches.size() > static_cast<size_t>(write_options.max_partitions)) {
          return Status::Invalid("Fragment would be written into ", groups.batches.size(),
                                 " partitions. This exceeds the maximum of ",
                                 write_options.max_partitions);
        }

        std::unordered_set<WriteQueue*> need_flushed;
        for (size_t i = 0; i < groups.batches.size(); ++i) {
          auto partition_expression = and_(std::move(groups.expressions[i]),
                                           scan_task->fragment()->partition_expression());
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
