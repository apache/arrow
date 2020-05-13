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
#include "parquet/properties.h"
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
    return IteratorFromReader(std::move(record_batch_reader));
  }

 private:
  int row_group_;
  std::vector<int> column_projection_;
  // The ScanTask _must_ hold a reference to reader_ because there's no
  // guarantee the producing ParquetScanTaskIterator is still alive. This is a
  // contract required by record_batch_reader_
  std::shared_ptr<parquet::arrow::FileReader> reader_;
};

static Result<std::unique_ptr<parquet::ParquetFileReader>> OpenReader(
    const FileSource& source, parquet::ReaderProperties properties) {
  ARROW_ASSIGN_OR_RAISE(auto input, source.Open());
  try {
    return parquet::ParquetFileReader::Open(std::move(input), std::move(properties));
  } catch (const ::parquet::ParquetException& e) {
    return Status::IOError("Could not open parquet input source '", source.path(),
                           "': ", e.what());
  }

  return Status::UnknownError("unknown exception caught");
}

static parquet::ReaderProperties MakeReaderProperties(
    const ParquetFileFormat& format, MemoryPool* pool = default_memory_pool()) {
  parquet::ReaderProperties properties(pool);
  if (format.reader_options.use_buffered_stream) {
    properties.enable_buffered_stream();
  } else {
    properties.disable_buffered_stream();
  }
  properties.set_buffer_size(format.reader_options.buffer_size);
  properties.file_decryption_properties(format.reader_options.file_decryption_properties);
  return properties;
}

static parquet::ArrowReaderProperties MakeArrowReaderProperties(
    const ParquetFileFormat& format, int64_t batch_size,
    const parquet::ParquetFileReader& reader) {
  parquet::ArrowReaderProperties properties(/* use_threads = */ false);
  for (const std::string& name : format.reader_options.dict_columns) {
    auto column_index = reader.metadata()->schema()->ColumnIndex(name);
    properties.set_read_dictionary(column_index, true);
  }
  properties.set_batch_size(batch_size);
  return properties;
}

template <typename M>
static Result<SchemaManifest> GetSchemaManifest(
    const M& metadata, const parquet::ArrowReaderProperties& properties) {
  SchemaManifest manifest;
  const std::shared_ptr<const ::arrow::KeyValueMetadata>& key_value_metadata = nullptr;
  RETURN_NOT_OK(
      SchemaManifest::Make(metadata.schema(), key_value_metadata, properties, &manifest));
  return manifest;
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

static Result<std::shared_ptr<Expression>> RowGroupStatisticsAsExpression(
    const parquet::RowGroupMetaData& metadata,
    const parquet::ArrowReaderProperties& properties) {
  ARROW_ASSIGN_OR_RAISE(auto manifest, GetSchemaManifest(metadata, properties));

  ExpressionVector expressions;
  for (const auto& schema_field : manifest.schema_fields) {
    expressions.emplace_back(ColumnChunkStatisticsAsExpression(schema_field, metadata));
  }

  return expressions.empty() ? scalar(true) : and_(expressions);
}

// Skip RowGroups with a filter and metadata
class RowGroupSkipper {
 public:
  static constexpr int kIterationDone = -1;

  RowGroupSkipper(std::shared_ptr<parquet::FileMetaData> metadata,
                  parquet::ArrowReaderProperties arrow_properties,
                  std::shared_ptr<Expression> filter, std::vector<int> row_groups)
      : metadata_(std::move(metadata)),
        arrow_properties_(std::move(arrow_properties)),
        filter_(std::move(filter)),
        row_group_idx_(0),
        row_groups_(std::move(row_groups)),
        num_row_groups_(row_groups_.empty() ? metadata_->num_row_groups()
                                            : static_cast<int>(row_groups_.size())) {}

  int Next() {
    while (row_group_idx_ < num_row_groups_) {
      const int row_group =
          row_groups_.empty() ? row_group_idx_++ : row_groups_[row_group_idx_++];

      const auto row_group_metadata = metadata_->RowGroup(row_group);

      const int64_t num_rows = row_group_metadata->num_rows();
      if (CanSkip(*row_group_metadata)) {
        rows_skipped_ += num_rows;
        continue;
      }

      return row_group;
    }

    return kIterationDone;
  }

 private:
  bool CanSkip(const parquet::RowGroupMetaData& metadata) const {
    auto maybe_stats_expr = RowGroupStatisticsAsExpression(metadata, arrow_properties_);
    // Errors with statistics are ignored and post-filtering will apply.
    if (!maybe_stats_expr.ok()) {
      return false;
    }

    auto stats_expr = maybe_stats_expr.ValueOrDie();
    return !filter_->Assume(stats_expr)->IsSatisfiable();
  }

  std::shared_ptr<parquet::FileMetaData> metadata_;
  parquet::ArrowReaderProperties arrow_properties_;
  std::shared_ptr<Expression> filter_;
  int row_group_idx_;
  std::vector<int> row_groups_;
  int num_row_groups_;
  int64_t rows_skipped_;
};

class ParquetScanTaskIterator {
 public:
  static Result<ScanTaskIterator> Make(std::shared_ptr<ScanOptions> options,
                                       std::shared_ptr<ScanContext> context,
                                       std::unique_ptr<parquet::ParquetFileReader> reader,
                                       parquet::ArrowReaderProperties arrow_properties,
                                       const std::vector<int>& row_groups) {
    auto metadata = reader->metadata();

    auto column_projection = InferColumnProjection(*metadata, arrow_properties, options);

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    RETURN_NOT_OK(parquet::arrow::FileReader::Make(context->pool, std::move(reader),
                                                   arrow_properties, &arrow_reader));

    RowGroupSkipper skipper(std::move(metadata), std::move(arrow_properties),
                            options->filter, row_groups);

    return ScanTaskIterator(ParquetScanTaskIterator(
        std::move(options), std::move(context), std::move(column_projection),
        std::move(skipper), std::move(arrow_reader)));
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
      const parquet::ArrowReaderProperties& arrow_properties,
      const std::shared_ptr<ScanOptions>& options) {
    auto maybe_manifest = GetSchemaManifest(metadata, arrow_properties);
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
                          std::vector<int> column_projection, RowGroupSkipper skipper,
                          std::unique_ptr<parquet::arrow::FileReader> reader)
      : options_(std::move(options)),
        context_(std::move(context)),
        column_projection_(std::move(column_projection)),
        skipper_(std::move(skipper)),
        reader_(std::move(reader)) {}

  std::shared_ptr<ScanOptions> options_;
  std::shared_ptr<ScanContext> context_;
  std::vector<int> column_projection_;
  RowGroupSkipper skipper_;
  std::shared_ptr<parquet::arrow::FileReader> reader_;
};

ParquetFileFormat::ParquetFileFormat(const parquet::ReaderProperties& reader_properties) {
  reader_options.use_buffered_stream = reader_properties.is_buffered_stream_enabled();
  reader_options.buffer_size = reader_properties.buffer_size();
  reader_options.file_decryption_properties =
      reader_properties.file_decryption_properties();
}

Result<bool> ParquetFileFormat::IsSupported(const FileSource& source) const {
  try {
    ARROW_ASSIGN_OR_RAISE(auto input, source.Open());
    auto properties = MakeReaderProperties(*this);
    auto reader =
        parquet::ParquetFileReader::Open(std::move(input), std::move(properties));
    auto metadata = reader->metadata();
    return metadata != nullptr && metadata->can_decompress();
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
  auto properties = MakeReaderProperties(*this);
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(source, std::move(properties)));

  auto arrow_properties =
      MakeArrowReaderProperties(*this, parquet::kArrowDefaultBatchSize, *reader);
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  RETURN_NOT_OK(parquet::arrow::FileReader::Make(default_memory_pool(), std::move(reader),
                                                 std::move(arrow_properties),
                                                 &arrow_reader));

  std::shared_ptr<Schema> schema;
  RETURN_NOT_OK(arrow_reader->GetSchema(&schema));
  return schema;
}

Result<ScanTaskIterator> ParquetFileFormat::ScanFile(
    const FileSource& source, std::shared_ptr<ScanOptions> options,
    std::shared_ptr<ScanContext> context) const {
  return ScanFile(source, std::move(options), std::move(context), {});
}

Result<ScanTaskIterator> ParquetFileFormat::ScanFile(
    const FileSource& source, std::shared_ptr<ScanOptions> options,
    std::shared_ptr<ScanContext> context, const std::vector<int>& row_groups) const {
  auto properties = MakeReaderProperties(*this, context->pool);
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(source, std::move(properties)));

  for (int i : row_groups) {
    if (i >= reader->metadata()->num_row_groups()) {
      return Status::IndexError("trying to scan row group ", i, " but ", source.path(),
                                " only has ", reader->metadata()->num_row_groups(),
                                " row groups");
    }
  }

  auto arrow_properties = MakeArrowReaderProperties(*this, options->batch_size, *reader);
  return ParquetScanTaskIterator::Make(std::move(options), std::move(context),
                                       std::move(reader), std::move(arrow_properties),
                                       row_groups);
}

Result<std::shared_ptr<FileFragment>> ParquetFileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Expression> partition_expression,
    std::vector<int> row_groups) {
  return std::shared_ptr<FileFragment>(
      new ParquetFileFragment(std::move(source), shared_from_this(),
                              std::move(partition_expression), std::move(row_groups)));
}

Result<std::shared_ptr<FileFragment>> ParquetFileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Expression> partition_expression) {
  return std::shared_ptr<FileFragment>(new ParquetFileFragment(
      std::move(source), shared_from_this(), std::move(partition_expression), {}));
}

Result<FragmentIterator> ParquetFileFormat::GetRowGroupFragments(
    const ParquetFileFragment& fragment, std::shared_ptr<Expression> filter) {
  auto properties = MakeReaderProperties(*this);
  ARROW_ASSIGN_OR_RAISE(auto reader,
                        OpenReader(fragment.source(), std::move(properties)));

  auto arrow_properties =
      MakeArrowReaderProperties(*this, parquet::kArrowDefaultBatchSize, *reader);
  auto metadata = reader->metadata();

  auto row_groups = fragment.row_groups();
  if (row_groups.empty()) {
    row_groups = internal::Iota(metadata->num_row_groups());
  }
  FragmentVector fragments(row_groups.size());

  RowGroupSkipper skipper(std::move(metadata), std::move(arrow_properties),
                          std::move(filter), std::move(row_groups));

  for (int i = 0, row_group = skipper.Next();
       row_group != RowGroupSkipper::kIterationDone; row_group = skipper.Next()) {
    ARROW_ASSIGN_OR_RAISE(
        fragments[i++],
        MakeFragment(fragment.source(), fragment.partition_expression(), {row_group}));
  }

  return MakeVectorIterator(std::move(fragments));
}

Result<ScanTaskIterator> ParquetFileFragment::Scan(std::shared_ptr<ScanOptions> options,
                                                   std::shared_ptr<ScanContext> context) {
  return parquet_format().ScanFile(source_, std::move(options), std::move(context),
                                   row_groups_);
}

const ParquetFileFormat& ParquetFileFragment::parquet_format() const {
  return internal::checked_cast<const ParquetFileFormat&>(*format_);
}

}  // namespace dataset
}  // namespace arrow
