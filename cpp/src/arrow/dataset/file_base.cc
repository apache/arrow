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
#include <unordered_map>
#include <vector>

#include "arrow/compute/exec/forest_internal.h"
#include "arrow/compute/exec/subtree_internal.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/dataset_writer.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/scanner_internal.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/io/compressed.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/util/compression.h"
#include "arrow/util/iterator.h"
#include "arrow/util/macros.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/map.h"
#include "arrow/util/string.h"
#include "arrow/util/task_group.h"
#include "arrow/util/variant.h"

namespace arrow {

using internal::checked_pointer_cast;

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

Future<util::optional<int64_t>> FileFormat::CountRows(
    const std::shared_ptr<FileFragment>&, compute::Expression,
    const std::shared_ptr<ScanOptions>&) {
  return Future<util::optional<int64_t>>::MakeFinished(util::nullopt);
}

Result<std::shared_ptr<FileFragment>> FileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Schema> physical_schema) {
  return MakeFragment(std::move(source), compute::literal(true),
                      std::move(physical_schema));
}

Result<std::shared_ptr<FileFragment>> FileFormat::MakeFragment(
    FileSource source, compute::Expression partition_expression) {
  return MakeFragment(std::move(source), std::move(partition_expression), nullptr);
}

Result<std::shared_ptr<FileFragment>> FileFormat::MakeFragment(
    FileSource source, compute::Expression partition_expression,
    std::shared_ptr<Schema> physical_schema) {
  return std::shared_ptr<FileFragment>(
      new FileFragment(std::move(source), shared_from_this(),
                       std::move(partition_expression), std::move(physical_schema)));
}

// The following implementation of ScanBatchesAsync is both ugly and terribly inefficient.
// Each of the formats should provide their own efficient implementation.  However, this
// is a reasonable starting point or implementation for a dummy/mock format.
Result<RecordBatchGenerator> FileFormat::ScanBatchesAsync(
    const std::shared_ptr<ScanOptions>& scan_options,
    const std::shared_ptr<FileFragment>& file) const {
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

Future<util::optional<int64_t>> FileFragment::CountRows(
    compute::Expression predicate, const std::shared_ptr<ScanOptions>& options) {
  ARROW_ASSIGN_OR_RAISE(predicate, compute::SimplifyWithGuarantee(std::move(predicate),
                                                                  partition_expression_));
  if (!predicate.IsSatisfiable()) {
    return Future<util::optional<int64_t>>::MakeFinished(0);
  }
  auto self = checked_pointer_cast<FileFragment>(shared_from_this());
  return format()->CountRows(self, std::move(predicate), options);
}

struct FileSystemDataset::FragmentSubtrees {
  // Forest for skipping fragments based on extracted subtree expressions
  compute::Forest forest;
  // fragment indices and subtree expressions in forest order
  std::vector<util::Variant<int, compute::Expression>> fragments_and_subtrees;
};

Result<std::shared_ptr<FileSystemDataset>> FileSystemDataset::Make(
    std::shared_ptr<Schema> schema, compute::Expression root_partition,
    std::shared_ptr<FileFormat> format, std::shared_ptr<fs::FileSystem> filesystem,
    std::vector<std::shared_ptr<FileFragment>> fragments,
    std::shared_ptr<Partitioning> partitioning) {
  std::shared_ptr<FileSystemDataset> out(
      new FileSystemDataset(std::move(schema), std::move(root_partition)));
  out->format_ = std::move(format);
  out->filesystem_ = std::move(filesystem);
  out->fragments_ = std::move(fragments);
  out->partitioning_ = std::move(partitioning);
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
    if (partition != compute::literal(true)) {
      repr += ": " + partition.ToString();
    }
  }

  return repr;
}

void FileSystemDataset::SetupSubtreePruning() {
  subtrees_ = std::make_shared<FragmentSubtrees>();
  compute::SubtreeImpl impl;

  auto encoded = impl.EncodeGuarantees(
      [&](int index) { return fragments_[index]->partition_expression(); },
      static_cast<int>(fragments_.size()));

  std::sort(encoded.begin(), encoded.end(), compute::SubtreeImpl::ByGuarantee());

  for (const auto& e : encoded) {
    if (e.index) {
      subtrees_->fragments_and_subtrees.emplace_back(*e.index);
    } else {
      subtrees_->fragments_and_subtrees.emplace_back(impl.GetSubtreeExpression(e));
    }
  }

  subtrees_->forest = compute::Forest(static_cast<int>(encoded.size()),
                                      compute::SubtreeImpl::IsAncestor{encoded});
}

Result<FragmentIterator> FileSystemDataset::GetFragmentsImpl(
    compute::Expression predicate) {
  if (predicate == compute::literal(true)) {
    // trivial predicate; skip subtree pruning
    return MakeVectorIterator(FragmentVector(fragments_.begin(), fragments_.end()));
  }

  std::vector<int> fragment_indices;

  std::vector<compute::Expression> predicates{predicate};
  RETURN_NOT_OK(subtrees_->forest.Visit(
      [&](compute::Forest::Ref ref) -> Result<bool> {
        if (auto fragment_index =
                util::get_if<int>(&subtrees_->fragments_and_subtrees[ref.i])) {
          fragment_indices.push_back(*fragment_index);
          return false;
        }

        const auto& subtree_expr =
            util::get<compute::Expression>(subtrees_->fragments_and_subtrees[ref.i]);
        ARROW_ASSIGN_OR_RAISE(auto simplified,
                              SimplifyWithGuarantee(predicates.back(), subtree_expr));

        if (!simplified.IsSatisfiable()) {
          return false;
        }

        predicates.push_back(std::move(simplified));
        return true;
      },
      [&](compute::Forest::Ref ref) { predicates.pop_back(); }));

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

Future<> WriteNextBatch(internal::DatasetWriter* dataset_writer, TaggedRecordBatch batch,
                        const FileSystemDatasetWriteOptions& write_options) {
  ARROW_ASSIGN_OR_RAISE(auto groups,
                        write_options.partitioning->Partition(batch.record_batch));
  batch.record_batch.reset();  // drop to hopefully conserve memory

  if (groups.batches.size() > static_cast<size_t>(write_options.max_partitions)) {
    return Status::Invalid("Fragment would be written into ", groups.batches.size(),
                           " partitions. This exceeds the maximum of ",
                           write_options.max_partitions);
  }

  std::shared_ptr<size_t> counter = std::make_shared<size_t>(0);
  std::shared_ptr<Fragment> fragment = std::move(batch.fragment);

  AsyncGenerator<std::shared_ptr<RecordBatch>> partitioned_batch_gen =
      [groups, counter, fragment, &write_options,
       dataset_writer]() -> Future<std::shared_ptr<RecordBatch>> {
    auto index = *counter;
    if (index >= groups.batches.size()) {
      return AsyncGeneratorEnd<std::shared_ptr<RecordBatch>>();
    }
    auto partition_expression =
        and_(groups.expressions[index], fragment->partition_expression());
    auto next_batch = groups.batches[index];
    ARROW_ASSIGN_OR_RAISE(std::string destination,
                          write_options.partitioning->Format(partition_expression));
    (*counter)++;
    return dataset_writer->WriteRecordBatch(next_batch, destination).Then([next_batch] {
      return next_batch;
    });
  };

  return VisitAsyncGenerator(
      std::move(partitioned_batch_gen),
      [](const std::shared_ptr<RecordBatch>&) -> Status { return Status::OK(); });
}

}  // namespace

Status FileSystemDataset::Write(const FileSystemDatasetWriteOptions& write_options,
                                std::shared_ptr<Scanner> scanner) {
  ARROW_ASSIGN_OR_RAISE(auto batch_gen, scanner->ScanBatchesAsync());
  ARROW_ASSIGN_OR_RAISE(auto dataset_writer,
                        internal::DatasetWriter::Make(write_options));

  AsyncGenerator<std::shared_ptr<int>> queued_batch_gen =
      [batch_gen, &dataset_writer, &write_options]() -> Future<std::shared_ptr<int>> {
    Future<TaggedRecordBatch> next_batch_fut = batch_gen();
    return next_batch_fut.Then(
        [&dataset_writer, &write_options](const TaggedRecordBatch& batch) {
          if (IsIterationEnd(batch)) {
            return AsyncGeneratorEnd<std::shared_ptr<int>>();
          }
          return WriteNextBatch(dataset_writer.get(), batch, write_options).Then([] {
            return std::make_shared<int>(0);
          });
        });
  };
  Future<> queue_fut =
      VisitAsyncGenerator(std::move(queued_batch_gen),
                          [&](const std::shared_ptr<int>&) { return Status::OK(); });

  ARROW_RETURN_NOT_OK(queue_fut.status());
  return dataset_writer->Finish().status();
}

}  // namespace dataset
}  // namespace arrow
