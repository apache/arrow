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

#include <arrow/compute/exec/exec_plan.h>

#include <algorithm>
#include <unordered_map>
#include <vector>

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/exec/forest_internal.h"
#include "arrow/compute/exec/subtree_internal.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/dataset_writer.h"
#include "arrow/dataset/scanner.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/io/compressed.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/compression.h"
#include "arrow/util/iterator.h"
#include "arrow/util/macros.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/map.h"
#include "arrow/util/string.h"
#include "arrow/util/task_group.h"
#include "arrow/util/variant.h"

namespace arrow {

using internal::checked_cast;
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

Result<std::shared_ptr<Schema>> FileFragment::ReadPhysicalSchemaImpl() {
  return format_->Inspect(source_);
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

class DatasetWritingSinkNodeConsumer : public compute::SinkNodeConsumer {
 public:
  DatasetWritingSinkNodeConsumer(std::shared_ptr<Schema> schema,
                                 std::unique_ptr<internal::DatasetWriter> dataset_writer,
                                 FileSystemDatasetWriteOptions write_options,
                                 std::shared_ptr<util::AsyncToggle> backpressure_toggle)
      : schema_(std::move(schema)),
        dataset_writer_(std::move(dataset_writer)),
        write_options_(std::move(write_options)),
        backpressure_toggle_(std::move(backpressure_toggle)) {}

  Status Consume(compute::ExecBatch batch) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> record_batch,
                          batch.ToRecordBatch(schema_));
    return WriteNextBatch(std::move(record_batch), batch.guarantee);
  }

  Future<> Finish() {
    RETURN_NOT_OK(task_group_.AddTask([this] { return dataset_writer_->Finish(); }));
    return task_group_.End();
  }

 private:
  Status WriteNextBatch(std::shared_ptr<RecordBatch> batch,
                        compute::Expression guarantee) {
    ARROW_ASSIGN_OR_RAISE(auto groups, write_options_.partitioning->Partition(batch));
    batch.reset();  // drop to hopefully conserve memory

    if (write_options_.max_partitions <= 0) {
      return Status::Invalid("max_partitions must be positive (was ",
                             write_options_.max_partitions, ")");
    }

    if (groups.batches.size() > static_cast<size_t>(write_options_.max_partitions)) {
      return Status::Invalid("Fragment would be written into ", groups.batches.size(),
                             " partitions. This exceeds the maximum of ",
                             write_options_.max_partitions);
    }

    for (std::size_t index = 0; index < groups.batches.size(); index++) {
      auto partition_expression = and_(groups.expressions[index], guarantee);
      auto next_batch = groups.batches[index];
      ARROW_ASSIGN_OR_RAISE(std::string destination,
                            write_options_.partitioning->Format(partition_expression));
      RETURN_NOT_OK(task_group_.AddTask([this, next_batch, destination] {
        Future<> has_room = dataset_writer_->WriteRecordBatch(next_batch, destination);
        if (!has_room.is_finished() && backpressure_toggle_) {
          backpressure_toggle_->Close();
          return has_room.Then([this] { backpressure_toggle_->Open(); });
        }
        return has_room;
      }));
    }
    return Status::OK();
  }

  std::shared_ptr<Schema> schema_;
  std::unique_ptr<internal::DatasetWriter> dataset_writer_;
  FileSystemDatasetWriteOptions write_options_;
  std::shared_ptr<util::AsyncToggle> backpressure_toggle_;
  util::SerializedAsyncTaskGroup task_group_;
};

}  // namespace

Status FileSystemDataset::Write(const FileSystemDatasetWriteOptions& write_options,
                                std::shared_ptr<Scanner> scanner) {
  const io::IOContext& io_context = scanner->options()->io_context;
  std::shared_ptr<compute::ExecContext> exec_context =
      std::make_shared<compute::ExecContext>(io_context.pool(),
                                             ::arrow::internal::GetCpuThreadPool());

  ARROW_ASSIGN_OR_RAISE(auto plan, compute::ExecPlan::Make(exec_context.get()));

  auto exprs = scanner->options()->projection.call()->arguments;
  auto names = checked_cast<const compute::MakeStructOptions*>(
                   scanner->options()->projection.call()->options.get())
                   ->field_names;
  std::shared_ptr<Dataset> dataset = scanner->dataset();
  std::shared_ptr<util::AsyncToggle> backpressure_toggle =
      std::make_shared<util::AsyncToggle>();

  RETURN_NOT_OK(
      compute::Declaration::Sequence(
          {
              {"scan", ScanNodeOptions{dataset, scanner->options(), backpressure_toggle}},
              {"filter", compute::FilterNodeOptions{scanner->options()->filter}},
              {"project",
               compute::ProjectNodeOptions{std::move(exprs), std::move(names)}},
              {"write",
               WriteNodeOptions{write_options, scanner->options()->projected_schema,
                                backpressure_toggle}},
          })
          .AddToPlan(plan.get()));

  RETURN_NOT_OK(plan->StartProducing());
  return plan->finished().status();
}

Result<compute::ExecNode*> MakeWriteNode(compute::ExecPlan* plan,
                                         std::vector<compute::ExecNode*> inputs,
                                         const compute::ExecNodeOptions& options) {
  if (inputs.size() != 1) {
    return Status::Invalid("Write SinkNode requires exactly 1 input, got ",
                           inputs.size());
  }

  const WriteNodeOptions write_node_options =
      checked_cast<const WriteNodeOptions&>(options);
  const FileSystemDatasetWriteOptions& write_options = write_node_options.write_options;
  const std::shared_ptr<Schema>& schema = write_node_options.schema;
  const std::shared_ptr<util::AsyncToggle>& backpressure_toggle =
      write_node_options.backpressure_toggle;

  ARROW_ASSIGN_OR_RAISE(auto dataset_writer,
                        internal::DatasetWriter::Make(write_options));

  std::shared_ptr<DatasetWritingSinkNodeConsumer> consumer =
      std::make_shared<DatasetWritingSinkNodeConsumer>(
          schema, std::move(dataset_writer), write_options, backpressure_toggle);

  ARROW_ASSIGN_OR_RAISE(
      auto node,
      compute::MakeExecNode("consuming_sink", plan, std::move(inputs),
                            compute::ConsumingSinkNodeOptions{std::move(consumer)}));

  return node;
}

namespace internal {
void InitializeDatasetWriter(arrow::compute::ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("write", MakeWriteNode));
}
}  // namespace internal

}  // namespace dataset

}  // namespace arrow
