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

#include "arrow/dataset/dataset.h"

#include <memory>
#include <utility>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/scanner.h"
#include "arrow/table.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/future.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/vector.h"

namespace arrow {
namespace dataset {

Fragment::Fragment(Expression partition_expression,
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

Result<std::shared_ptr<Schema>> InMemoryFragment::ReadPhysicalSchemaImpl() {
  return physical_schema_;
}

namespace {
struct VectorIterable {
  Result<RecordBatchGenerator> operator()() { return MakeVectorGenerator(batches); }
  RecordBatchVector batches;
};
}  // namespace

InMemoryFragment::InMemoryFragment(std::shared_ptr<Schema> physical_schema,
                                   RecordBatchIterable get_batches,
                                   Expression partition_expression)
    : Fragment(std::move(partition_expression), std::move(physical_schema)),
      get_batches_(std::move(get_batches)) {
  DCHECK_NE(physical_schema_, nullptr);
}

InMemoryFragment::InMemoryFragment(std::shared_ptr<Schema> physical_schema,
                                   RecordBatchVector batches,
                                   Expression partition_expression)
    : Fragment(std::move(partition_expression), std::move(physical_schema)),
      get_batches_(VectorIterable{std::move(batches)}) {
  DCHECK_NE(physical_schema_, nullptr);
}

Future<ScanTaskVector> InMemoryFragment::Scan(std::shared_ptr<ScanOptions> options) {
  auto self = shared_from_this();
  ScanTaskVector scan_tasks{std::make_shared<InMemoryScanTask>(
      get_batches_, std::move(options), std::move(self))};
  return Future<ScanTaskVector>::MakeFinished(scan_tasks);
}

Dataset::Dataset(std::shared_ptr<Schema> schema, Expression partition_expression)
    : schema_(std::move(schema)),
      partition_expression_(std::move(partition_expression)) {}

Result<std::shared_ptr<ScannerBuilder>> Dataset::NewScan(
    std::shared_ptr<ScanOptions> options) {
  return std::make_shared<ScannerBuilder>(this->shared_from_this(), options);
}

Result<std::shared_ptr<ScannerBuilder>> Dataset::NewScan() {
  return NewScan(std::make_shared<ScanOptions>());
}

Future<FragmentVector> Dataset::GetFragmentsAsync() const {
  ARROW_ASSIGN_OR_RAISE(auto predicate, literal(true).Bind(*schema_));
  return GetFragmentsAsync(std::move(predicate));
}

Future<FragmentVector> Dataset::GetFragmentsAsync(Expression predicate) const {
  ARROW_ASSIGN_OR_RAISE(
      predicate, SimplifyWithGuarantee(std::move(predicate), partition_expression_));
  return predicate.IsSatisfiable() ? GetFragmentsImpl(std::move(predicate))
                                   : FragmentVector{};
}

namespace {

struct TableIterable {
  Result<RecordBatchGenerator> operator()() {
    auto reader = std::make_shared<TableBatchReader>(*table);
    return [reader] { return reader->Next(); };
  }
  std::shared_ptr<Table> table;
};

struct ReaderIterableState {
  explicit ReaderIterableState(std::shared_ptr<RecordBatchReader> reader)
      : reader(std::move(reader)), consumed(0) {}

  std::shared_ptr<RecordBatchReader> reader;
  std::atomic<uint8_t> consumed;
};
struct ReaderIterable {
  explicit ReaderIterable(std::shared_ptr<RecordBatchReader> reader)
      : state(std::make_shared<ReaderIterableState>(std::move(reader))) {}

  Result<RecordBatchGenerator> operator()() {
    if (state->consumed.fetch_or(1)) {
      return Status::Invalid(
          "A dataset created from a RecordBatchReader can only be scanned once");
    }
    auto reader_capture = state->reader;
    return [reader_capture] { return reader_capture->Next(); };
  }

  std::shared_ptr<ReaderIterableState> state;
};

}  // namespace

std::shared_ptr<InMemoryDataset> InMemoryDataset::FromTable(
    std::shared_ptr<Table> table) {
  auto schema = table->schema();
  return std::make_shared<InMemoryDataset>(std::move(schema),
                                           TableIterable{std::move(table)});
}

std::shared_ptr<InMemoryDataset> InMemoryDataset::FromReader(
    std::shared_ptr<RecordBatchReader> reader) {
  auto schema = reader->schema();
  return std::make_shared<InMemoryDataset>(std::move(schema),
                                           ReaderIterable{std::move(reader)});
}

std::shared_ptr<InMemoryDataset> InMemoryDataset::FromBatches(
    std::shared_ptr<Schema> schema, RecordBatchVector batches) {
  return std::make_shared<InMemoryDataset>(std::move(schema),
                                           VectorIterable{std::move(batches)});
}

Result<std::shared_ptr<Dataset>> InMemoryDataset::ReplaceSchema(
    std::shared_ptr<Schema> schema) const {
  RETURN_NOT_OK(CheckProjectable(*schema_, *schema));
  return std::make_shared<InMemoryDataset>(std::move(schema), std::move(get_batches_));
}

Future<FragmentVector> InMemoryDataset::GetFragmentsImpl(Expression) const {
  auto schema = this->schema();

  FragmentVector fragments{std::make_shared<InMemoryFragment>(schema, get_batches_)};
  return Future<FragmentVector>::MakeFinished(std::move(fragments));
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

Future<FragmentVector> UnionDataset::GetFragmentsImpl(Expression predicate) const {
  return GetFragmentsFromDatasets(children_, predicate);
}

}  // namespace dataset
}  // namespace arrow
