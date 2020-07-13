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
#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner.h"
#include "arrow/table.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"

namespace arrow {
namespace dataset {

Fragment::Fragment(std::shared_ptr<Expression> partition_expression,
                   std::shared_ptr<Schema> physical_schema)
    : partition_expression_(std::move(partition_expression)),
      physical_schema_(std::move(physical_schema)) {
  DCHECK_NE(partition_expression_, nullptr);
}

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
                                   std::shared_ptr<Expression> partition_expression)
    : Fragment(std::move(partition_expression), std::move(schema)),
      record_batches_(std::move(record_batches)) {
  DCHECK_NE(physical_schema_, nullptr);
}

InMemoryFragment::InMemoryFragment(RecordBatchVector record_batches,
                                   std::shared_ptr<Expression> partition_expression)
    : InMemoryFragment(record_batches.empty() ? schema({}) : record_batches[0]->schema(),
                       std::move(record_batches), std::move(partition_expression)) {}

Result<ScanTaskIterator> InMemoryFragment::Scan(std::shared_ptr<ScanOptions> options,
                                                std::shared_ptr<ScanContext> context) {
  // Make an explicit copy of record_batches_ to ensure Scan can be called
  // multiple times.
  auto batches_it = MakeVectorIterator(record_batches_);

  auto batch_size = options->batch_size;
  // RecordBatch -> ScanTask
  auto fn = [=](std::shared_ptr<RecordBatch> batch) -> std::shared_ptr<ScanTask> {
    RecordBatchVector batches;

    auto n_batches = BitUtil::CeilDiv(batch->num_rows(), batch_size);
    for (int i = 0; i < n_batches; i++) {
      batches.push_back(batch->Slice(batch_size * i, batch_size));
    }

    return ::arrow::internal::make_unique<InMemoryScanTask>(
        std::move(batches), std::move(options), std::move(context));
  };

  return MakeMapIterator(fn, std::move(batches_it));
}

Dataset::Dataset(std::shared_ptr<Schema> schema,
                 std::shared_ptr<Expression> partition_expression)
    : schema_(std::move(schema)), partition_expression_(std::move(partition_expression)) {
  DCHECK_NE(partition_expression_, nullptr);
}

Result<std::shared_ptr<ScannerBuilder>> Dataset::NewScan(
    std::shared_ptr<ScanContext> context) {
  return std::make_shared<ScannerBuilder>(this->shared_from_this(), context);
}

Result<std::shared_ptr<ScannerBuilder>> Dataset::NewScan() {
  return NewScan(std::make_shared<ScanContext>());
}

FragmentIterator Dataset::GetFragments(std::shared_ptr<Expression> predicate) {
  predicate = predicate->Assume(*partition_expression_);
  return predicate->IsSatisfiable() ? GetFragmentsImpl(std::move(predicate))
                                    : MakeEmptyIterator<std::shared_ptr<Fragment>>();
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

FragmentIterator InMemoryDataset::GetFragmentsImpl(std::shared_ptr<Expression>) {
  auto schema = this->schema();

  auto create_fragment =
      [schema](std::shared_ptr<RecordBatch> batch) -> Result<std::shared_ptr<Fragment>> {
    if (!batch->schema()->Equals(schema)) {
      return Status::TypeError("yielded batch had schema ", *batch->schema(),
                               " which did not match InMemorySource's: ", *schema);
    }

    RecordBatchVector batches{batch};
    return std::make_shared<InMemoryFragment>(std::move(batches));
  };

  return MakeMaybeMapIterator(std::move(create_fragment), get_batches_->Get());
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

FragmentIterator UnionDataset::GetFragmentsImpl(std::shared_ptr<Expression> predicate) {
  return GetFragmentsFromDatasets(children_, predicate);
}

}  // namespace dataset
}  // namespace arrow
