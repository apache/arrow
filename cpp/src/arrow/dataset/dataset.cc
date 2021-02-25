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

InMemoryFragment::InMemoryFragment(std::shared_ptr<Schema> schema,
                                   RecordBatchVector record_batches,
                                   Expression partition_expression)
    : Fragment(std::move(partition_expression), std::move(schema)),
      record_batches_(std::move(record_batches)) {
  DCHECK_NE(physical_schema_, nullptr);
}

InMemoryFragment::InMemoryFragment(RecordBatchVector record_batches,
                                   Expression partition_expression)
    : InMemoryFragment(record_batches.empty() ? schema({}) : record_batches[0]->schema(),
                       std::move(record_batches), std::move(partition_expression)) {}

Future<ScanTaskVector> InMemoryFragment::Scan(std::shared_ptr<ScanOptions> options) {
  // Make an explicit copy of record_batches_ to ensure Scan can be called
  // multiple times.
  auto batches_it = MakeVectorIterator(record_batches_);

  auto batch_size = options->batch_size;
  // RecordBatch -> ScanTask
  auto self = shared_from_this();
  auto fn = [=](std::shared_ptr<RecordBatch> batch) -> std::shared_ptr<ScanTask> {
    RecordBatchVector batches;

    auto n_batches = BitUtil::CeilDiv(batch->num_rows(), batch_size);
    for (int i = 0; i < n_batches; i++) {
      batches.push_back(batch->Slice(batch_size * i, batch_size));
    }

    return ::arrow::internal::make_unique<InMemoryScanTask>(std::move(batches),
                                                            std::move(options), self);
  };

  return Future<ScanTaskVector>::MakeFinished(
      MakeMapIterator(fn, std::move(batches_it)).ToVector());
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

Future<FragmentVector> Dataset::GetFragmentsAsync() {
  ARROW_ASSIGN_OR_RAISE(auto predicate, literal(true).Bind(*schema_));
  return GetFragmentsAsync(std::move(predicate));
}

Future<FragmentVector> Dataset::GetFragmentsAsync(Expression predicate) {
  ARROW_ASSIGN_OR_RAISE(
      predicate, SimplifyWithGuarantee(std::move(predicate), partition_expression_));
  return predicate.IsSatisfiable() ? GetFragmentsImpl(std::move(predicate))
                                   : FragmentVector{};
}

struct VectorRecordBatchVectorFactory : InMemoryDataset::RecordBatchVectorFactory {
  explicit VectorRecordBatchVectorFactory(RecordBatchVector batches)
      : batches_(std::move(batches)) {}

  Result<RecordBatchVector> Get() const final { return batches_; }

  RecordBatchVector batches_;
};

InMemoryDataset::InMemoryDataset(std::shared_ptr<Schema> schema,
                                 RecordBatchVector batches)
    : Dataset(std::move(schema)),
      get_batches_(new VectorRecordBatchVectorFactory(std::move(batches))) {}

struct TableRecordBatchVectorFactory : InMemoryDataset::RecordBatchVectorFactory {
  explicit TableRecordBatchVectorFactory(std::shared_ptr<Table> table)
      : table_(std::move(table)) {}

  Result<RecordBatchVector> Get() const final {
    auto reader = std::make_shared<TableBatchReader>(*table_);
    auto table = table_;
    auto iter = MakeFunctionIterator([reader, table] { return reader->Next(); });
    return iter.ToVector();
  }

  std::shared_ptr<Table> table_;
};

InMemoryDataset::InMemoryDataset(std::shared_ptr<Table> table)
    : Dataset(table->schema()),
      get_batches_(new TableRecordBatchVectorFactory(std::move(table))) {}

Result<std::shared_ptr<Dataset>> InMemoryDataset::ReplaceSchema(
    std::shared_ptr<Schema> schema) const {
  RETURN_NOT_OK(CheckProjectable(*schema_, *schema));
  return std::make_shared<InMemoryDataset>(std::move(schema), get_batches_);
}

Future<FragmentVector> InMemoryDataset::GetFragmentsImpl(Expression) {
  auto schema = this->schema();

  // FIXME Need auto here
  std::function<Result<std::shared_ptr<Fragment>>(const std::shared_ptr<RecordBatch>&)>
      create_fragment = [schema](const std::shared_ptr<RecordBatch>& batch)
      -> Result<std::shared_ptr<Fragment>> {
    if (!batch->schema()->Equals(schema)) {
      return Status::TypeError("yielded batch had schema ", *batch->schema(),
                               " which did not match InMemorySource's: ", *schema);
    }

    RecordBatchVector batches{batch};
    return std::make_shared<InMemoryFragment>(std::move(batches));
  };

  ARROW_ASSIGN_OR_RAISE(auto batches, get_batches_->Get());

  return Future<FragmentVector>::MakeFinished(
      internal::MaybeMapVector(std::move(create_fragment), batches));
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

Future<FragmentVector> UnionDataset::GetFragmentsImpl(Expression predicate) {
  return GetFragmentsFromDatasets(children_, predicate);
}

}  // namespace dataset
}  // namespace arrow
