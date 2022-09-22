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

#include <memory>
#include <utility>

#include "arrow/dataset/dataset.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/scanner.h"
#include "arrow/table.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

using internal::checked_pointer_cast;

namespace dataset {

Fragment::Fragment(compute::Expression partition_expression,
                   std::shared_ptr<Schema> physical_schema)
    : partition_expression_(std::move(partition_expression)),
      physical_schema_(std::move(physical_schema)) {}

Result<std::shared_ptr<Schema>> Fragment::ReadPhysicalSchema() {
  {
    auto lock = physical_schema_mutex_.Lock();
    if (physical_schema_ != nullptr) return physical_schema_;
  }

  // allow ReadPhysicalSchemaImpl to lock mutex_, if necessary
  ARROW_ASSIGN_OR_RAISE(auto physical_schema, ReadPhysicalSchemaImpl());

  auto lock = physical_schema_mutex_.Lock();
  if (physical_schema_ == nullptr) {
    physical_schema_ = std::move(physical_schema);
  }
  return physical_schema_;
}

Future<std::optional<int64_t>> Fragment::CountRows(compute::Expression,
                                                   const std::shared_ptr<ScanOptions>&) {
  return Future<std::optional<int64_t>>::MakeFinished(std::nullopt);
}

Result<std::shared_ptr<Schema>> InMemoryFragment::ReadPhysicalSchemaImpl() {
  return physical_schema_;
}

InMemoryFragment::InMemoryFragment(std::shared_ptr<Schema> schema,
                                   RecordBatchVector record_batches,
                                   compute::Expression partition_expression)
    : Fragment(std::move(partition_expression), std::move(schema)),
      record_batches_(std::move(record_batches)) {
  DCHECK_NE(physical_schema_, nullptr);
}

InMemoryFragment::InMemoryFragment(RecordBatchVector record_batches,
                                   compute::Expression partition_expression)
    : Fragment(std::move(partition_expression), /*schema=*/nullptr),
      record_batches_(std::move(record_batches)) {
  // Order of argument evaluation is undefined, so compute physical_schema here
  physical_schema_ = record_batches_.empty() ? schema({}) : record_batches_[0]->schema();
}

Result<RecordBatchGenerator> InMemoryFragment::ScanBatchesAsync(
    const std::shared_ptr<ScanOptions>& options) {
  struct State {
    State(std::shared_ptr<InMemoryFragment> fragment, int64_t batch_size)
        : fragment(std::move(fragment)),
          batch_index(0),
          offset(0),
          batch_size(batch_size) {}

    std::shared_ptr<RecordBatch> Next() {
      const auto& next_parent = fragment->record_batches_[batch_index];
      if (offset < next_parent->num_rows()) {
        auto next = next_parent->Slice(offset, batch_size);
        offset += batch_size;
        return next;
      }
      batch_index++;
      offset = 0;
      return nullptr;
    }

    bool Finished() { return batch_index >= fragment->record_batches_.size(); }

    std::shared_ptr<InMemoryFragment> fragment;
    std::size_t batch_index;
    int64_t offset;
    int64_t batch_size;
  };

  struct Generator {
    Generator(std::shared_ptr<InMemoryFragment> fragment, int64_t batch_size)
        : state(std::make_shared<State>(std::move(fragment), batch_size)) {}

    Future<std::shared_ptr<RecordBatch>> operator()() {
      while (!state->Finished()) {
        auto next = state->Next();
        if (next) {
          return Future<std::shared_ptr<RecordBatch>>::MakeFinished(std::move(next));
        }
      }
      return AsyncGeneratorEnd<std::shared_ptr<RecordBatch>>();
    }

    std::shared_ptr<State> state;
  };
  return Generator(checked_pointer_cast<InMemoryFragment>(shared_from_this()),
                   options->batch_size);
}

Future<std::optional<int64_t>> InMemoryFragment::CountRows(
    compute::Expression predicate, const std::shared_ptr<ScanOptions>& options) {
  if (ExpressionHasFieldRefs(predicate)) {
    return Future<std::optional<int64_t>>::MakeFinished(std::nullopt);
  }
  int64_t total = 0;
  for (const auto& batch : record_batches_) {
    total += batch->num_rows();
  }
  return Future<std::optional<int64_t>>::MakeFinished(total);
}

Dataset::Dataset(std::shared_ptr<Schema> schema, compute::Expression partition_expression)
    : schema_(std::move(schema)),
      partition_expression_(std::move(partition_expression)) {}

Result<std::shared_ptr<ScannerBuilder>> Dataset::NewScan() {
  return std::make_shared<ScannerBuilder>(this->shared_from_this());
}

Result<FragmentIterator> Dataset::GetFragments() {
  return GetFragments(compute::literal(true));
}

Result<FragmentIterator> Dataset::GetFragments(compute::Expression predicate) {
  ARROW_ASSIGN_OR_RAISE(
      predicate, SimplifyWithGuarantee(std::move(predicate), partition_expression_));
  return predicate.IsSatisfiable() ? GetFragmentsImpl(std::move(predicate))
                                   : MakeEmptyIterator<std::shared_ptr<Fragment>>();
}

Result<FragmentGenerator> Dataset::GetFragmentsAsync() {
  return GetFragmentsAsync(compute::literal(true));
}

Result<FragmentGenerator> Dataset::GetFragmentsAsync(compute::Expression predicate) {
  ARROW_ASSIGN_OR_RAISE(
      predicate, SimplifyWithGuarantee(std::move(predicate), partition_expression_));
  return predicate.IsSatisfiable()
             ? GetFragmentsAsyncImpl(std::move(predicate),
                                     arrow::internal::GetCpuThreadPool())
             : MakeEmptyGenerator<std::shared_ptr<Fragment>>();
}

// Default impl delegating the work to `GetFragmentsImpl` and wrapping it into
// BackgroundGenerator/TransferredGenerator, which offloads potentially
// IO-intensive work to the default IO thread pool and then transfers the control
// back to the specified executor.
Result<FragmentGenerator> Dataset::GetFragmentsAsyncImpl(
    compute::Expression predicate, arrow::internal::Executor* executor) {
  ARROW_ASSIGN_OR_RAISE(auto iter, GetFragmentsImpl(std::move(predicate)));
  ARROW_ASSIGN_OR_RAISE(
      auto background_gen,
      MakeBackgroundGenerator(std::move(iter), io::default_io_context().executor()));
  auto transferred_gen = MakeTransferredGenerator(std::move(background_gen), executor);
  return transferred_gen;
}

struct VectorRecordBatchGenerator : InMemoryDataset::RecordBatchGenerator {
  explicit VectorRecordBatchGenerator(RecordBatchVector batches)
      : batches_(std::move(batches)) {}

  RecordBatchIterator Get() const final { return MakeVectorIterator(batches_); }

  RecordBatchVector batches_;
};

InMemoryDataset::InMemoryDataset(std::shared_ptr<Schema> schema,
                                 RecordBatchVector batches)
    : Dataset(std::move(schema)),
      get_batches_(new VectorRecordBatchGenerator(std::move(batches))) {}

struct TableRecordBatchGenerator : InMemoryDataset::RecordBatchGenerator {
  explicit TableRecordBatchGenerator(std::shared_ptr<Table> table)
      : table_(std::move(table)) {}

  RecordBatchIterator Get() const final {
    auto reader = std::make_shared<TableBatchReader>(*table_);
    auto table = table_;
    return MakeFunctionIterator([reader, table] { return reader->Next(); });
  }

  std::shared_ptr<Table> table_;
};

InMemoryDataset::InMemoryDataset(std::shared_ptr<Table> table)
    : Dataset(table->schema()),
      get_batches_(new TableRecordBatchGenerator(std::move(table))) {}

Result<std::shared_ptr<Dataset>> InMemoryDataset::ReplaceSchema(
    std::shared_ptr<Schema> schema) const {
  RETURN_NOT_OK(CheckProjectable(*schema_, *schema));
  return std::make_shared<InMemoryDataset>(std::move(schema), get_batches_);
}

Result<FragmentIterator> InMemoryDataset::GetFragmentsImpl(compute::Expression) {
  auto schema = this->schema();

  auto create_fragment =
      [schema](std::shared_ptr<RecordBatch> batch) -> Result<std::shared_ptr<Fragment>> {
    RETURN_NOT_OK(CheckProjectable(*schema, *batch->schema()));
    return std::make_shared<InMemoryFragment>(RecordBatchVector{std::move(batch)});
  };

  auto batches_it = get_batches_->Get();
  return MakeMaybeMapIterator(std::move(create_fragment), std::move(batches_it));
}

Result<std::shared_ptr<UnionDataset>> UnionDataset::Make(std::shared_ptr<Schema> schema,
                                                         DatasetVector children) {
  for (const auto& child : children) {
    if (!child->schema()->Equals(*schema)) {
      return Status::TypeError("child Dataset had schema ", *child->schema(),
                               " but the union schema was ", *schema);
    }
  }

  return std::shared_ptr<UnionDataset>(
      new UnionDataset(std::move(schema), std::move(children)));
}

Result<std::shared_ptr<Dataset>> UnionDataset::ReplaceSchema(
    std::shared_ptr<Schema> schema) const {
  auto children = children_;
  for (auto& child : children) {
    ARROW_ASSIGN_OR_RAISE(child, child->ReplaceSchema(schema));
  }

  return std::shared_ptr<Dataset>(
      new UnionDataset(std::move(schema), std::move(children)));
}

Result<FragmentIterator> UnionDataset::GetFragmentsImpl(compute::Expression predicate) {
  return GetFragmentsFromDatasets(children_, predicate);
}

}  // namespace dataset
}  // namespace arrow
