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
#include <utility>
#include <vector>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner.h"
#include "arrow/table.h"
#include "arrow/util/iterator.h"
#include "arrow/util/range.h"
#include "arrow/util/stl.h"
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
      : row_group_(row_group),
        column_projection_(std::move(column_projection)),
        reader_(reader),
        options_(std::move(options)),
        context_(std::move(context)) {}

  RecordBatchIterator Scan() {
    // The construction of parquet's RecordBatchReader is deferred here to
    // control the memory usage of consumers who materialize all ScanTasks
    // before dispatching them, e.g. for scheduling purposes.
    //
    // Thus the memory incurred by the RecordBatchReader is allocated when
    // Scan is called.
    std::unique_ptr<RecordBatchReader> record_batch_reader;
    auto status = reader_->GetRecordBatchReader({row_group_}, column_projection_,
                                                &record_batch_reader);
    // Propagate the previous error as an error iterator.
    if (!status.ok()) {
      return MakeErrorIterator<std::shared_ptr<RecordBatch>>(std::move(status));
    }

    return MakePointerIterator(std::move(record_batch_reader));
  }

 private:
  int row_group_;
  std::vector<int> column_projection_;
  // The ScanTask _must_ hold a reference to reader_ because there's no
  // guarantee the producing ParquetScanTaskIterator is still alive. This is a
  // contract required by record_batch_reader_
  std::shared_ptr<parquet::arrow::FileReader> reader_;

  std::shared_ptr<ScanOptions> options_;
  std::shared_ptr<ScanContext> context_;
};

// Skip RowGroups with a filter and metadata
class RowGroupSkipper {
 public:
  static constexpr int kIterationDone = -1;

  RowGroupSkipper(std::shared_ptr<parquet::FileMetaData> metadata,
                  std::shared_ptr<Expression> filter)
      : metadata_(std::move(metadata)), filter_(filter), row_group_idx_(0) {
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

class ParquetScanTaskIterator {
 public:
  static Status Make(std::shared_ptr<ScanOptions> options,
                     std::shared_ptr<ScanContext> context,
                     std::unique_ptr<parquet::ParquetFileReader> reader,
                     ScanTaskIterator* out) {
    auto metadata = reader->metadata();

    auto column_projection = InferColumnProjection(*metadata, options);

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    RETURN_NOT_OK(parquet::arrow::FileReader::Make(context->pool, std::move(reader),
                                                   &arrow_reader));

    *out = ScanTaskIterator(ParquetScanTaskIterator(
        std::move(options), std::move(context), std::move(column_projection),
        std::move(metadata), std::move(arrow_reader)));
    return Status::OK();
  }

  Status Next(std::unique_ptr<ScanTask>* task) {
    auto row_group = skipper_.Next();

    // Iteration is done.
    if (row_group == RowGroupSkipper::kIterationDone) {
      task->reset(nullptr);
      return Status::OK();
    }

    task->reset(
        new ParquetScanTask(row_group, column_projection_, reader_, options_, context_));

    return Status::OK();
  }

 private:
  // Compute the column projection out of an optional arrow::Schema
  static std::vector<int> InferColumnProjection(
      const parquet::FileMetaData& metadata,
      const std::shared_ptr<ScanOptions>& options) {
    if (options->projector == nullptr) {
      // fall back to no push down projection
      return internal::Iota(metadata.num_columns());
    }

    SchemaManifest manifest;
    if (!SchemaManifest::Make(metadata.schema(), nullptr,
                              parquet::default_arrow_reader_properties(), &manifest)
             .ok()) {
      return internal::Iota(metadata.num_columns());
    }

    // get column indices
    auto filter_fields = FieldsInExpression(options->filter);

    std::vector<int> column_projection;

    for (const auto& schema_field : manifest.schema_fields) {
      auto field_name = schema_field.field->name();

      if (options->projector->schema()->GetFieldIndex(field_name) != -1) {
        // add explicitly projected field
        AddColumnIndices(schema_field, &column_projection);
        continue;
      }

      if (std::find(filter_fields.begin(), filter_fields.end(), field_name) !=
          filter_fields.end()) {
        // add field referenced by filter
        AddColumnIndices(schema_field, &column_projection);
      }
    }

    return column_projection;
  }

  static void AddColumnIndices(const SchemaField& schema_field,
                               std::vector<int>* column_projection) {
    if (schema_field.column_index != -1) {
      column_projection->push_back(schema_field.column_index);
      return;
    }

    for (const auto& child : schema_field.children) {
      AddColumnIndices(child, column_projection);
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

Status ParquetFileFormat::IsSupported(const FileSource& source, bool* supported) const {
  try {
    std::shared_ptr<io::RandomAccessFile> input;
    RETURN_NOT_OK(source.Open(&input));
    auto reader = parquet::ParquetFileReader::Open(input);
  } catch (const ::parquet::ParquetInvalidOrCorruptedFileException& e) {
    ARROW_UNUSED(e);
    *supported = false;
    return Status::OK();
  } catch (const ::parquet::ParquetException& e) {
    return Status::IOError("Could not open parquet input source '", source.path(),
                           "': ", e.what());
  }

  *supported = true;
  return Status::OK();
}

Status ParquetFileFormat::Inspect(const FileSource& source,
                                  std::shared_ptr<Schema>* out) const {
  auto pool = default_memory_pool();

  std::unique_ptr<parquet::ParquetFileReader> reader;
  RETURN_NOT_OK(OpenReader(source, pool, &reader));

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  RETURN_NOT_OK(parquet::arrow::FileReader::Make(pool, std::move(reader), &arrow_reader));

  return arrow_reader->GetSchema(out);
}

Status ParquetFileFormat::ScanFile(const FileSource& source,
                                   std::shared_ptr<ScanOptions> scan_options,
                                   std::shared_ptr<ScanContext> scan_context,
                                   ScanTaskIterator* out) const {
  std::unique_ptr<parquet::ParquetFileReader> reader;
  RETURN_NOT_OK(OpenReader(source, scan_context->pool, &reader));

  return ParquetScanTaskIterator::Make(scan_options, scan_context, std::move(reader),
                                       out);
}

Status ParquetFileFormat::MakeFragment(const FileSource& source,
                                       std::shared_ptr<ScanOptions> opts,
                                       std::unique_ptr<DataFragment>* out) {
  // TODO(bkietz) check location.path() against IsKnownExtension etc
  *out = internal::make_unique<ParquetFragment>(source, opts);
  return Status::OK();
}

Status ParquetFileFormat::OpenReader(
    const FileSource& source, MemoryPool* pool,
    std::unique_ptr<parquet::ParquetFileReader>* out) const {
  std::shared_ptr<io::RandomAccessFile> input;
  RETURN_NOT_OK(source.Open(&input));

  try {
    *out = parquet::ParquetFileReader::Open(input);
  } catch (const ::parquet::ParquetException& e) {
    return Status::IOError("Could not open parquet input source '", source.path(),
                           "': ", e.what());
  }
  return Status::OK();
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
    std::shared_ptr<Scalar> null_scalar;
    if (!MakeNullScalar(field->type(), &null_scalar).ok()) {
      // MakeNullScalar can fail for some nested/repeated types.
      return scalar(true);
    }

    return equal(field_expr, scalar(null_scalar));
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
  SchemaManifest manifest;
  RETURN_NOT_OK(SchemaManifest::Make(
      metadata.schema(), nullptr, parquet::default_arrow_reader_properties(), &manifest));

  std::vector<std::shared_ptr<Expression>> expressions;
  for (const auto& schema_field : manifest.schema_fields) {
    expressions.emplace_back(ColumnChunkStatisticsAsExpression(schema_field, metadata));
  }

  return expressions.empty() ? scalar(true) : and_(expressions);
}

}  // namespace dataset
}  // namespace arrow
