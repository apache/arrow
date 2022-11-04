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
#include <memory>
#include <unordered_map>
#include <variant>
#include <vector>

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/exec/forest_internal.h"
#include "arrow/compute/exec/map_node.h"
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
#include "arrow/util/map.h"
#include "arrow/util/string.h"
#include "arrow/util/task_group.h"
#include "arrow/util/tracing_internal.h"

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
    std::optional<Compression::type> compression) const {
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

bool FileSource::Equals(const FileSource& other) const {
  bool match_file_system =
      (filesystem_ == nullptr && other.filesystem_ == nullptr) ||
      (filesystem_ && other.filesystem_ && filesystem_->Equals(other.filesystem_));
  bool match_buffer = (buffer_ == nullptr && other.buffer_ == nullptr) ||
                      ((buffer_ != nullptr && other.buffer_ != nullptr) &&
                       (buffer_->address() == other.buffer_->address()));
  return match_file_system && match_buffer && file_info_.Equals(other.file_info_) &&
         compression_ == other.compression_;
}

Future<std::optional<int64_t>> FileFormat::CountRows(
    const std::shared_ptr<FileFragment>&, compute::Expression,
    const std::shared_ptr<ScanOptions>&) {
  return Future<std::optional<int64_t>>::MakeFinished(std::nullopt);
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

Future<std::optional<int64_t>> FileFragment::CountRows(
    compute::Expression predicate, const std::shared_ptr<ScanOptions>& options) {
  ARROW_ASSIGN_OR_RAISE(predicate, compute::SimplifyWithGuarantee(std::move(predicate),
                                                                  partition_expression_));
  if (!predicate.IsSatisfiable()) {
    return Future<std::optional<int64_t>>::MakeFinished(0);
  }
  auto self = checked_pointer_cast<FileFragment>(shared_from_this());
  return format()->CountRows(self, std::move(predicate), options);
}

bool FileFragment::Equals(const FileFragment& other) const {
  return source_.Equals(other.source_) && format_->Equals(*other.format_);
}

struct FileSystemDataset::FragmentSubtrees {
  // Forest for skipping fragments based on extracted subtree expressions
  compute::Forest forest;
  // fragment indices and subtree expressions in forest order
  std::vector<std::variant<int, compute::Expression>> fragments_and_subtrees;
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
                std::get_if<int>(&subtrees_->fragments_and_subtrees[ref.i])) {
          fragment_indices.push_back(*fragment_index);
          return false;
        }

        const auto& subtree_expr =
            std::get<compute::Expression>(subtrees_->fragments_and_subtrees[ref.i]);
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

Future<> FileWriter::Finish() {
  return FinishInternal().Then([this]() -> Future<> {
    ARROW_ASSIGN_OR_RAISE(bytes_written_, destination_->Tell());
    return destination_->CloseAsync();
  });
}

Result<int64_t> FileWriter::GetBytesWritten() const {
  if (bytes_written_.has_value()) {
    return bytes_written_.value();
  } else {
    return Status::Invalid("Cannot retrieve bytes written before calling Finish()");
  }
}

namespace {

Status WriteBatch(
    std::shared_ptr<RecordBatch> batch, compute::Expression guarantee,
    FileSystemDatasetWriteOptions write_options,
    std::function<Status(std::shared_ptr<RecordBatch>, const PartitionPathFormat&)>
        write) {
  ARROW_ASSIGN_OR_RAISE(auto groups, write_options.partitioning->Partition(batch));
  batch.reset();  // drop to hopefully conserve memory

  if (write_options.max_partitions <= 0) {
    return Status::Invalid("max_partitions must be positive (was ",
                           write_options.max_partitions, ")");
  }

  if (groups.batches.size() > static_cast<size_t>(write_options.max_partitions)) {
    return Status::Invalid("Fragment would be written into ", groups.batches.size(),
                           " partitions. This exceeds the maximum of ",
                           write_options.max_partitions);
  }

  for (std::size_t index = 0; index < groups.batches.size(); index++) {
    auto partition_expression = and_(groups.expressions[index], guarantee);
    auto next_batch = groups.batches[index];
    PartitionPathFormat destination;
    ARROW_ASSIGN_OR_RAISE(destination,
                          write_options.partitioning->Format(partition_expression));
    RETURN_NOT_OK(write(next_batch, destination));
  }
  return Status::OK();
}

class DatasetWritingSinkNodeConsumer : public compute::SinkNodeConsumer {
 public:
  DatasetWritingSinkNodeConsumer(std::shared_ptr<const KeyValueMetadata> custom_metadata,
                                 FileSystemDatasetWriteOptions write_options)
      : custom_metadata_(std::move(custom_metadata)),
        write_options_(std::move(write_options)) {}

  Status Init(const std::shared_ptr<Schema>& schema,
              compute::BackpressureControl* backpressure_control,
              compute::ExecPlan* plan) override {
    if (custom_metadata_) {
      schema_ = schema->WithMetadata(custom_metadata_);
    } else {
      schema_ = schema;
    }
    ARROW_ASSIGN_OR_RAISE(
        dataset_writer_,
        internal::DatasetWriter::Make(
            write_options_, plan->async_scheduler(),
            [backpressure_control] { backpressure_control->Pause(); },
            [backpressure_control] { backpressure_control->Resume(); }, [] {}));
    return Status::OK();
  }

  Status Consume(compute::ExecBatch batch) override {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> record_batch,
                          batch.ToRecordBatch(schema_));
    return WriteNextBatch(std::move(record_batch), batch.guarantee);
  }

  Future<> Finish() override {
    dataset_writer_->Finish();
    // Some write tasks may still be in the queue at this point but that is ok.
    return Future<>::MakeFinished();
  }

 private:
  Status WriteNextBatch(std::shared_ptr<RecordBatch> batch,
                        compute::Expression guarantee) {
    return WriteBatch(batch, guarantee, write_options_,
                      [this](std::shared_ptr<RecordBatch> next_batch,
                             const PartitionPathFormat& destination) {
                        dataset_writer_->WriteRecordBatch(std::move(next_batch),
                                                          destination.directory,
                                                          destination.filename);
                        return Status::OK();
                      });
  }

  std::shared_ptr<const KeyValueMetadata> custom_metadata_;
  std::unique_ptr<internal::DatasetWriter> dataset_writer_;
  FileSystemDatasetWriteOptions write_options_;
  Future<> finished_ = Future<>::Make();
  std::shared_ptr<Schema> schema_ = nullptr;
};

}  // namespace

Status FileSystemDataset::Write(const FileSystemDatasetWriteOptions& write_options,
                                std::shared_ptr<Scanner> scanner) {
  const io::IOContext& io_context = scanner->options()->io_context;
  auto cpu_executor =
      scanner->options()->use_threads ? ::arrow::internal::GetCpuThreadPool() : nullptr;
  std::shared_ptr<compute::ExecContext> exec_context =
      std::make_shared<compute::ExecContext>(io_context.pool(), cpu_executor);

  ARROW_ASSIGN_OR_RAISE(auto plan, compute::ExecPlan::Make(exec_context.get()));

  auto exprs = scanner->options()->projection.call()->arguments;
  auto names = checked_cast<const compute::MakeStructOptions*>(
                   scanner->options()->projection.call()->options.get())
                   ->field_names;
  std::shared_ptr<Dataset> dataset = scanner->dataset();

  // The projected_schema is currently used by pyarrow to preserve the custom metadata
  // when reading from a single input file.
  const auto& custom_metadata = scanner->options()->projected_schema->metadata();

  RETURN_NOT_OK(
      compute::Declaration::Sequence(
          {
              {"scan", ScanNodeOptions{dataset, scanner->options()}},
              {"filter", compute::FilterNodeOptions{scanner->options()->filter}},
              {"project",
               compute::ProjectNodeOptions{std::move(exprs), std::move(names)}},
              {"write", WriteNodeOptions{write_options, custom_metadata}},
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
  const std::shared_ptr<const KeyValueMetadata>& custom_metadata =
      write_node_options.custom_metadata;
  const FileSystemDatasetWriteOptions& write_options = write_node_options.write_options;

  if (!write_options.partitioning) {
    return Status::Invalid("Must provide partitioning");
  }

  std::shared_ptr<DatasetWritingSinkNodeConsumer> consumer =
      std::make_shared<DatasetWritingSinkNodeConsumer>(custom_metadata, write_options);

  ARROW_ASSIGN_OR_RAISE(
      auto node,
      compute::MakeExecNode("consuming_sink", plan, std::move(inputs),
                            compute::ConsumingSinkNodeOptions{std::move(consumer)}));

  return node;
}

namespace {

class TeeNode : public compute::MapNode {
 public:
  TeeNode(compute::ExecPlan* plan, std::vector<compute::ExecNode*> inputs,
          std::shared_ptr<Schema> output_schema,
          FileSystemDatasetWriteOptions write_options)
      : MapNode(plan, std::move(inputs), std::move(output_schema)),
        write_options_(std::move(write_options)) {}

  Status StartProducing() override {
    ARROW_ASSIGN_OR_RAISE(dataset_writer_, internal::DatasetWriter::Make(
                                               write_options_, plan_->async_scheduler(),
                                               [this] { Pause(); }, [this] { Resume(); },
                                               [this] { MapNode::Finish(); }));
    return MapNode::StartProducing();
  }

  static Result<compute::ExecNode*> Make(compute::ExecPlan* plan,
                                         std::vector<compute::ExecNode*> inputs,
                                         const compute::ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 1, "TeeNode"));

    const WriteNodeOptions write_node_options =
        checked_cast<const WriteNodeOptions&>(options);
    const FileSystemDatasetWriteOptions& write_options = write_node_options.write_options;
    const std::shared_ptr<Schema> schema = inputs[0]->output_schema();

    return plan->EmplaceNode<TeeNode>(plan, std::move(inputs), std::move(schema),
                                      std::move(write_options));
  }

  const char* kind_name() const override { return "TeeNode"; }

  void Finish(Status finish_st) override {
    if (!finish_st.ok()) {
      MapNode::Finish(std::move(finish_st));
      return;
    }
    dataset_writer_->Finish();
  }

  Result<compute::ExecBatch> DoTee(const compute::ExecBatch& batch) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> record_batch,
                          batch.ToRecordBatch(output_schema()));
    ARROW_RETURN_NOT_OK(WriteNextBatch(std::move(record_batch), batch.guarantee));
    return batch;
  }

  Status WriteNextBatch(std::shared_ptr<RecordBatch> batch,
                        compute::Expression guarantee) {
    return WriteBatch(batch, guarantee, write_options_,
                      [this](std::shared_ptr<RecordBatch> next_batch,
                             const PartitionPathFormat& destination) {
                        util::tracing::Span span;
                        dataset_writer_->WriteRecordBatch(
                            next_batch, destination.directory, destination.filename);
                        return Status::OK();
                      });
  }

  void InputReceived(compute::ExecNode* input, compute::ExecBatch batch) override {
    EVENT(span_, "InputReceived", {{"batch.length", batch.length}});
    DCHECK_EQ(input, inputs_[0]);
    auto func = [this](compute::ExecBatch batch) {
      util::tracing::Span span;
      START_SPAN_WITH_PARENT(span, span_, "InputReceived",
                             {{"tee", ToStringExtra()},
                              {"node.label", label()},
                              {"batch.length", batch.length}});
      auto result = DoTee(std::move(batch));
      MARK_SPAN(span, result.status());
      END_SPAN(span);
      return result;
    };
    this->SubmitTask(std::move(func), std::move(batch));
  }

  void Pause() { inputs_[0]->PauseProducing(this, ++backpressure_counter_); }

  void Resume() { inputs_[0]->ResumeProducing(this, ++backpressure_counter_); }

 protected:
  std::string ToStringExtra(int indent = 0) const override {
    return "base_dir=" + write_options_.base_dir;
  }

 private:
  std::unique_ptr<internal::DatasetWriter> dataset_writer_;
  FileSystemDatasetWriteOptions write_options_;
  int32_t backpressure_counter_ = 0;
};

}  // namespace

namespace internal {
void InitializeDatasetWriter(arrow::compute::ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("write", MakeWriteNode));
  DCHECK_OK(registry->AddFactory("tee", TeeNode::Make));
}
}  // namespace internal

}  // namespace dataset

}  // namespace arrow
