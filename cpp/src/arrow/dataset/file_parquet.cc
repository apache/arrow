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

#include "arrow/dataset/file_parquet.h"

#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner.h"
#include "arrow/table.h"
#include "arrow/util/iterator.h"
#include "arrow/util/range.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/file_reader.h"
#include "parquet/statistics.h"

namespace arrow {
namespace dataset {

using parquet::arrow::SchemaField;
using parquet::arrow::SchemaManifest;
using parquet::arrow::StatisticsAsScalars;

/// \brief A ScanTask backed by a parquet file and a RowGroup within a parquet file.
class ParquetScanTask : public ScanTask {
 public:
  ParquetScanTask(int row_group, std::vector<int> column_projection,
                  std::shared_ptr<parquet::arrow::FileReader> reader,
                  std::shared_ptr<ScanOptions> options,
                  std::shared_ptr<ScanContext> context)
      : ScanTask(std::move(options), std::move(context)),
        row_group_(row_group),
        column_projection_(std::move(column_projection)),
        reader_(std::move(reader)) {}

  Result<RecordBatchIterator> Execute() override {
    // The construction of parquet's RecordBatchReader is deferred here to
    // control the memory usage of consumers who materialize all ScanTasks
    // before dispatching them, e.g. for scheduling purposes.
    //
    // Thus the memory incurred by the RecordBatchReader is allocated when
    // Scan is called.
    std::unique_ptr<RecordBatchReader> record_batch_reader;
    RETURN_NOT_OK(reader_->GetRecordBatchReader({row_group_}, column_projection_,
                                                &record_batch_reader));

    std::shared_ptr<RecordBatchReader> r = std::move(record_batch_reader);
    return MakeFunctionIterator([r] { return r->Next(); });
  }

 private:
  int row_group_;
  std::vector<int> column_projection_;
  // The ScanTask _must_ hold a reference to reader_ because there's no
  // guarantee the producing ParquetScanTaskIterator is still alive. This is a
  // contract required by record_batch_reader_
  std::shared_ptr<parquet::arrow::FileReader> reader_;
};

// Skip RowGroups with a filter and metadata
class RowGroupSkipper {
 public:
  static constexpr int kIterationDone = -1;

  RowGroupSkipper(std::shared_ptr<parquet::FileMetaData> metadata,
                  std::shared_ptr<Expression> filter)
      : metadata_(std::move(metadata)), filter_(std::move(filter)), row_group_idx_(0) {
    num_row_groups_ = metadata_->num_row_groups();
  }

  int Next() {
    while (row_group_idx_ < num_row_groups_) {
      const auto row_group_idx = row_group_idx_++;
      const auto row_group = metadata_->RowGroup(row_group_idx);

      const auto num_rows = row_group->num_rows();
      if (CanSkip(*row_group)) {
        rows_skipped_ += num_rows;
        continue;
      }

      return row_group_idx;
    }

    return kIterationDone;
  }

 private:
  bool CanSkip(const parquet::RowGroupMetaData& metadata) const {
    auto maybe_stats_expr = RowGroupStatisticsAsExpression(metadata);
    // Errors with statistics are ignored and post-filtering will apply.
    if (!maybe_stats_expr.ok()) {
      return false;
    }

    auto stats_expr = maybe_stats_expr.ValueOrDie();
    auto expr = filter_->Assume(stats_expr);
    return (expr->IsNull() || expr->Equals(false));
  }

  std::shared_ptr<parquet::FileMetaData> metadata_;
  std::shared_ptr<Expression> filter_;
  int row_group_idx_;
  int num_row_groups_;
  int64_t rows_skipped_;
};

template <typename M>
static Result<SchemaManifest> GetSchemaManifest(const M& metadata) {
  SchemaManifest manifest;
  RETURN_NOT_OK(SchemaManifest::Make(
      metadata.schema(), nullptr, parquet::default_arrow_reader_properties(), &manifest));
  return manifest;
}

class ParquetScanTaskIterator {
 public:
  static Result<ScanTaskIterator> Make(
      std::shared_ptr<ScanOptions> options, std::shared_ptr<ScanContext> context,
      std::unique_ptr<parquet::ParquetFileReader> reader) {
    auto metadata = reader->metadata();

    auto column_projection = InferColumnProjection(*metadata, options);

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    RETURN_NOT_OK(parquet::arrow::FileReader::Make(context->pool, std::move(reader),
                                                   &arrow_reader));

    return ScanTaskIterator(ParquetScanTaskIterator(
        std::move(options), std::move(context), std::move(column_projection),
        std::move(metadata), std::move(arrow_reader)));
  }

  Result<std::shared_ptr<ScanTask>> Next() {
    auto row_group = skipper_.Next();

    // Iteration is done.
    if (row_group == RowGroupSkipper::kIterationDone) {
      return nullptr;
    }

    return std::shared_ptr<ScanTask>(
        new ParquetScanTask(row_group, column_projection_, reader_, options_, context_));
  }

 private:
  // Compute the column projection out of an optional arrow::Schema
  static std::vector<int> InferColumnProjection(
      const parquet::FileMetaData& metadata,
      const std::shared_ptr<ScanOptions>& options) {
    auto maybe_manifest = GetSchemaManifest(metadata);
    if (!maybe_manifest.ok()) {
      return internal::Iota(metadata.num_columns());
    }
    auto manifest = std::move(maybe_manifest).ValueOrDie();

    // Checks if the field is needed in either the projection or the filter.
    auto fields_name = options->MaterializedFields();
    std::unordered_set<std::string> materialized_fields{fields_name.cbegin(),
                                                        fields_name.cend()};
    auto should_materialize_column = [&materialized_fields](const std::string& f) {
      return materialized_fields.find(f) != materialized_fields.end();
    };

    std::vector<int> columns_selection;
    // Note that the loop is using the file's schema to iterate instead of the
    // materialized fields of the ScanOptions. This ensures that missing
    // fields in the file (but present in the ScanOptions) will be ignored. The
    // scanner's projector will take care of padding the column with the proper
    // values.
    for (const auto& schema_field : manifest.schema_fields) {
      if (should_materialize_column(schema_field.field->name())) {
        AddColumnIndices(schema_field, &columns_selection);
      }
    }

    return columns_selection;
  }

  static void AddColumnIndices(const SchemaField& schema_field,
                               std::vector<int>* column_projection) {
    if (schema_field.is_leaf()) {
      column_projection->push_back(schema_field.column_index);
    } else {
      // The following ensure that complex types, e.g. struct,  are materialized.
      for (const auto& child : schema_field.children) {
        AddColumnIndices(child, column_projection);
      }
    }
  }

  ParquetScanTaskIterator(std::shared_ptr<ScanOptions> options,
                          std::shared_ptr<ScanContext> context,
                          std::vector<int> column_projection,
                          std::shared_ptr<parquet::FileMetaData> metadata,
                          std::unique_ptr<parquet::arrow::FileReader> reader)
      : options_(std::move(options)),
        context_(std::move(context)),
        column_projection_(std::move(column_projection)),
        skipper_(std::move(metadata), options_->filter),
        reader_(std::move(reader)) {}

  std::shared_ptr<ScanOptions> options_;
  std::shared_ptr<ScanContext> context_;
  std::vector<int> column_projection_;
  RowGroupSkipper skipper_;
  std::shared_ptr<parquet::arrow::FileReader> reader_;
};

Result<bool> ParquetFileFormat::IsSupported(const FileSource& source) const {
  try {
    ARROW_ASSIGN_OR_RAISE(auto input, source.Open());
    auto reader = parquet::ParquetFileReader::Open(input);
  } catch (const ::parquet::ParquetInvalidOrCorruptedFileException& e) {
    ARROW_UNUSED(e);
    return false;
  } catch (const ::parquet::ParquetException& e) {
    return Status::IOError("Could not open parquet input source '", source.path(),
                           "': ", e.what());
  }

  return true;
}

Result<std::shared_ptr<Schema>> ParquetFileFormat::Inspect(
    const FileSource& source) const {
  auto pool = default_memory_pool();
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(source, pool));

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  RETURN_NOT_OK(parquet::arrow::FileReader::Make(pool, std::move(reader), &arrow_reader));

  std::shared_ptr<Schema> schema;
  RETURN_NOT_OK(arrow_reader->GetSchema(&schema));
  return schema;
}

Result<ScanTaskIterator> ParquetFileFormat::ScanFile(
    const FileSource& source, std::shared_ptr<ScanOptions> options,
    std::shared_ptr<ScanContext> context) const {
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(source, context->pool));
  return ParquetScanTaskIterator::Make(options, context, std::move(reader));
}

Result<std::shared_ptr<Fragment>> ParquetFileFormat::MakeFragment(
    const FileSource& source, std::shared_ptr<ScanOptions> options) {
  return std::make_shared<ParquetFragment>(source, options);
}

Result<std::unique_ptr<parquet::ParquetFileReader>> ParquetFileFormat::OpenReader(
    const FileSource& source, MemoryPool* pool) const {
  ARROW_ASSIGN_OR_RAISE(auto input, source.Open());
  try {
    return parquet::ParquetFileReader::Open(input);
  } catch (const ::parquet::ParquetException& e) {
    return Status::IOError("Could not open parquet input source '", source.path(),
                           "': ", e.what());
  }

  return Status::UnknownError("unknown exception caught");
}

static std::shared_ptr<Expression> ColumnChunkStatisticsAsExpression(
    const SchemaField& schema_field, const parquet::RowGroupMetaData& metadata) {
  // For the remaining of this function, failure to extract/parse statistics
  // are ignored by returning the `true` scalar. The goal is two fold. First
  // avoid that an optimization break the computation. Second, allow the
  // following columns to maybe succeed in extracting column statistics.

  // For now, only leaf (primitive) types are supported.
  if (!schema_field.is_leaf()) {
    return scalar(true);
  }

  auto column_metadata = metadata.ColumnChunk(schema_field.column_index);
  auto field = schema_field.field;
  auto field_expr = field_ref(field->name());

  // In case of missing statistics, return nothing.
  if (!column_metadata->is_stats_set()) {
    return scalar(true);
  }

  auto statistics = column_metadata->statistics();
  if (statistics == nullptr) {
    return scalar(true);
  }

  // Optimize for corner case where all values are nulls
  if (statistics->num_values() == statistics->null_count()) {
    return equal(field_expr, scalar(MakeNullScalar(field->type())));
  }

  std::shared_ptr<Scalar> min, max;
  if (!StatisticsAsScalars(*statistics, &min, &max).ok()) {
    return scalar(true);
  }

  return and_(greater_equal(field_expr, scalar(min)),
              less_equal(field_expr, scalar(max)));
}

Result<std::shared_ptr<Expression>> RowGroupStatisticsAsExpression(
    const parquet::RowGroupMetaData& metadata) {
  ARROW_ASSIGN_OR_RAISE(auto manifest, GetSchemaManifest(metadata));

  ExpressionVector expressions;
  for (const auto& schema_field : manifest.schema_fields) {
    expressions.emplace_back(ColumnChunkStatisticsAsExpression(schema_field, metadata));
  }

  return expressions.empty() ? scalar(true) : and_(expressions);
}

}  // namespace dataset
}  // namespace arrow
