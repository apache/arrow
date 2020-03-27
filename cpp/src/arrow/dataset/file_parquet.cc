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
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner.h"
#include "arrow/filesystem/path_util.h"
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
    const ParquetFileFormat& format, const parquet::FileMetaData& metadata) {
  parquet::ArrowReaderProperties properties(/* use_threads = */ false);
  for (const std::string& name : format.reader_options.dict_columns) {
    auto column_index = metadata.schema()->ColumnIndex(name);
    properties.set_read_dictionary(column_index, true);
  }
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
  auto statistics = column_metadata->statistics();
  if (statistics == nullptr) {
    return scalar(true);
  }

  const auto& field = schema_field.field;
  auto field_expr = field_ref(field->name());

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

static std::shared_ptr<Expression> RowGroupStatisticsAsExpression(
    const parquet::RowGroupMetaData& metadata, const SchemaManifest& manifest,
    const parquet::ArrowReaderProperties& properties) {
  const auto& fields = manifest.schema_fields;
  ExpressionVector expressions{fields.size()};
  for (const auto& field : fields) {
    expressions.emplace_back(ColumnChunkStatisticsAsExpression(field, metadata));
  }

  return expressions.empty() ? scalar(true) : and_(expressions);
}

class ParquetScanTaskIterator {
 public:
  static Result<ScanTaskIterator> Make(std::shared_ptr<ScanOptions> options,
                                       std::shared_ptr<ScanContext> context,
                                       std::unique_ptr<parquet::ParquetFileReader> reader,
                                       parquet::ArrowReaderProperties arrow_properties,
                                       std::vector<int> row_groups) {
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    RETURN_NOT_OK(parquet::arrow::FileReader::Make(context->pool, std::move(reader),
                                                   arrow_properties, &arrow_reader));

    auto metadata = arrow_reader->parquet_reader()->metadata();
    auto column_projection = InferColumnProjection(*metadata, arrow_properties, options);

    return static_cast<ScanTaskIterator>(ParquetScanTaskIterator(
        std::move(options), std::move(context), std::move(column_projection),
        std::move(row_groups), std::move(arrow_reader)));
  }

  Result<std::shared_ptr<ScanTask>> Next() {
    if (idx_ >= row_groups_.size()) {
      return nullptr;
    }

    auto row_group = row_groups_[idx_++];
    return std::shared_ptr<ScanTask>(
        new ParquetScanTask(row_group, column_projection_, reader_, options_, context_));
  }

 private:
  // Compute the column projection out of an optional arrow::Schema
  static std::vector<int> InferColumnProjection(
      const parquet::FileMetaData& metadata,
      const parquet::ArrowReaderProperties& properties,
      const std::shared_ptr<ScanOptions>& options) {
    auto maybe_manifest = GetSchemaManifest(metadata, properties);
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
                          std::vector<int> column_projection, std::vector<int> row_groups,
                          std::unique_ptr<parquet::arrow::FileReader> reader)
      : options_(std::move(options)),
        context_(std::move(context)),
        column_projection_(std::move(column_projection)),
        row_groups_(std::move(row_groups)),
        reader_(std::move(reader)) {}

  std::shared_ptr<ScanOptions> options_;
  std::shared_ptr<ScanContext> context_;
  std::vector<int> column_projection_;
  std::vector<int> row_groups_;
  std::shared_ptr<parquet::arrow::FileReader> reader_;
  // row group index.
  size_t idx_ = 0;
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
  ARROW_ASSIGN_OR_RAISE(auto reader, GetReader(source));
  std::shared_ptr<Schema> schema;
  RETURN_NOT_OK(reader->GetSchema(&schema));
  return schema;
}

Result<std::unique_ptr<parquet::arrow::FileReader>> ParquetFileFormat::GetReader(
    const FileSource& source, MemoryPool* pool) const {
  auto properties = MakeReaderProperties(*this, pool);
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(source, std::move(properties)));

  auto metadata = reader->metadata();
  auto arrow_properties = MakeArrowReaderProperties(*this, *metadata);
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  RETURN_NOT_OK(parquet::arrow::FileReader::Make(default_memory_pool(), std::move(reader),
                                                 std::move(arrow_properties),
                                                 &arrow_reader));
  return arrow_reader;
}

Result<ScanTaskIterator> ParquetFileFormat::ScanFile(
    const FileSource& source, std::shared_ptr<ScanOptions> options,
    std::shared_ptr<ScanContext> context) const {
  auto properties = MakeReaderProperties(*this, context->pool);
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(source, std::move(properties)));
  int num_row_groups = reader->metadata()->num_row_groups();
  return ScanFile(std::move(options), std::move(context), std::move(reader),
                  internal::Iota(num_row_groups));
}

Result<ScanTaskIterator> ParquetFileFormat::ScanFile(
    std::shared_ptr<ScanOptions> options, std::shared_ptr<ScanContext> context,
    std::unique_ptr<parquet::ParquetFileReader> reader,
    std::vector<int> row_groups) const {
  auto metadata = reader->metadata();
  auto arrow_properties = MakeArrowReaderProperties(*this, *metadata);
  arrow_properties.set_batch_size(options->batch_size);
  return ParquetScanTaskIterator::Make(std::move(options), std::move(context),
                                       std::move(reader), std::move(arrow_properties),
                                       row_groups);
}

Result<std::shared_ptr<FileFragment>> ParquetFileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Expression> partition_expression,
    std::vector<int> row_groups, ExpressionVector statistics) {
  if (!statistics.empty() && statistics.size() != row_groups.size()) {
    return Status::Invalid("ParquetFileFragment statistics's size ", statistics.size(),
                           " must match the number of row groups ", row_groups.size());
  }

  return std::shared_ptr<FileFragment>(new ParquetFileFragment(
      std::move(source), shared_from_this(), std::move(partition_expression),
      std::move(row_groups), std::move(statistics)));
}

Result<std::shared_ptr<FileFragment>> ParquetFileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Expression> partition_expression,
    std::vector<int> row_groups) {
  return std::shared_ptr<FileFragment>(new ParquetFileFragment(
      std::move(source), shared_from_this(), std::move(partition_expression),
      std::move(row_groups), {}));
}

Result<std::shared_ptr<FileFragment>> ParquetFileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Expression> partition_expression) {
  return std::shared_ptr<FileFragment>(new ParquetFileFragment(
      std::move(source), shared_from_this(), std::move(partition_expression), {}, {}));
}

Result<std::vector<int>> ParquetFileFragment::RowGroupsWithFilter(
    const parquet::FileMetaData& metadata,
    const parquet::ArrowReaderProperties& properties, const Expression& predicate) const {
  try {
    ARROW_ASSIGN_OR_RAISE(auto manifest, GetSchemaManifest(metadata, properties));

    auto num_row_groups = metadata.num_row_groups();
    std::vector<int> row_groups =
        row_groups_.empty() ? internal::Iota(num_row_groups) : row_groups_;

    std::vector<int> filtered_groups;
    for (size_t i = 0; i < row_groups.size(); i++) {
      int row_group_id = row_groups[i];
      if (row_group_id >= num_row_groups) {
        // Deferred here instead of construction to avoid paying IO, i.e.
        // num_row_groups is known by reading the metadata.
        return Status::IndexError("trying to scan row group ", row_group_id, " but ",
                                  source_.path(), " only has ", num_row_groups,
                                  " row groups");
      }

      const auto& stats = statistics_.empty() ? RowGroupStatisticsAsExpression(
                                                    *metadata.RowGroup(row_group_id),
                                                    manifest, properties)
                                              : statistics_[i];
      if (predicate.IsSatisfiableWith(stats)) {
        filtered_groups.push_back(row_group_id);
      }
    }

    return filtered_groups;
  } catch (const ::parquet::ParquetException& e) {
    return Status::Invalid("Error while iterating over RowGroups :", e.what());
  }
}

Result<ScanTaskIterator> ParquetFileFragment::Scan(std::shared_ptr<ScanOptions> options,
                                                   std::shared_ptr<ScanContext> context) {
  auto properties = MakeReaderProperties(parquet_format_, context->pool);
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(source_, std::move(properties)));

  auto metadata = reader->metadata();
  auto arrow_properties = MakeArrowReaderProperties(parquet_format_, *metadata);
  ARROW_ASSIGN_OR_RAISE(
      auto row_groups,
      RowGroupsWithFilter(*metadata, std::move(arrow_properties), *options->filter));

  return parquet_format_.ScanFile(std::move(options), std::move(context),
                                  std::move(reader), row_groups);
}

Result<FragmentVector> ParquetFileFragment::SplitByRowGroup(
    std::shared_ptr<Expression> predicate) {
  auto properties = MakeReaderProperties(parquet_format_);
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(source_, std::move(properties)));

  auto metadata = reader->metadata();
  auto arrow_properties = MakeArrowReaderProperties(parquet_format_, *metadata);
  ARROW_ASSIGN_OR_RAISE(auto row_groups,
                        RowGroupsWithFilter(*metadata, arrow_properties, *predicate));

  FragmentVector fragments{row_groups.size()};
  for (size_t i = 0; i < row_groups.size(); i++) {
    ARROW_ASSIGN_OR_RAISE(
        fragments[i],
        parquet_format_.MakeFragment(source_, partition_expression_, {row_groups[i]}));
  }

  return fragments;
}

///
/// ParquetDatasetFactory
///

ParquetDatasetFactory::ParquetDatasetFactory(
    std::shared_ptr<fs::FileSystem> filesystem, std::shared_ptr<ParquetFileFormat> format,
    std::shared_ptr<parquet::FileMetaData> metadata, std::string base_path)
    : filesystem_(std::move(filesystem)),
      format_(std::move(format)),
      metadata_(std::move(metadata)),
      base_path_(std::move(base_path)) {}

Result<std::shared_ptr<DatasetFactory>> ParquetDatasetFactory::Make(
    const std::string& metadata_path, std::shared_ptr<fs::FileSystem> filesystem,
    std::shared_ptr<ParquetFileFormat> format) {
  // Paths in ColumnChunk are relative to the `_metadata` file. Thus, the base
  // directory of all parquet files is `dirname(metadata_path)`.
  auto dirname = arrow::fs::internal::GetAbstractPathParent(metadata_path).first;
  return Make({metadata_path, filesystem}, dirname, filesystem, format);
}

Result<std::shared_ptr<DatasetFactory>> ParquetDatasetFactory::Make(
    const FileSource& metadata_source, const std::string& base_path,
    std::shared_ptr<fs::FileSystem> filesystem,
    std::shared_ptr<ParquetFileFormat> format) {
  DCHECK_NE(filesystem, nullptr);
  DCHECK_NE(format, nullptr);

  ARROW_ASSIGN_OR_RAISE(auto reader, format->GetReader(metadata_source));
  auto metadata = reader->parquet_reader()->metadata();

  return std::shared_ptr<DatasetFactory>(new ParquetDatasetFactory(
      std::move(filesystem), std::move(format), std::move(metadata), base_path));
}

Result<std::vector<std::shared_ptr<Schema>>> ParquetDatasetFactory::InspectSchemas(
    InspectOptions options) {
  std::shared_ptr<Schema> schema;
  RETURN_NOT_OK(parquet::arrow::FromParquetSchema(metadata_->schema(), &schema));
  return std::vector<std::shared_ptr<Schema>>{schema};
}

static Result<std::string> FileFromRowGroup(const std::string base_path,
                                            const parquet::RowGroupMetaData& row_group) {
  try {
    auto n_columns = row_group.num_columns();
    if (n_columns == 0) {
      return Status::Invalid("RowGroup must have a least one columns to extract path");
    }

    auto first_column = row_group.ColumnChunk(0);
    auto path = first_column->file_path();
    if (path == "") {
      return Status::Invalid("Got empty file path");
    }

    for (int i = 1; i < n_columns; i++) {
      auto column = row_group.ColumnChunk(i);
      auto column_path = column->file_path();
      if (column_path != path) {
        return Status::Invalid("Path '", column_path, "' not equal to path '", path,
                               ", for ColumnChunk at index ", i);
      }
    }

    return fs::internal::ConcatAbstractPath(base_path, path);
  } catch (const ::parquet::ParquetException& e) {
    return Status::Invalid("Could not infer file path from RowGroup :", e.what());
  }
}

using PathsAndStatistics = std::pair<std::vector<std::string>, ExpressionVector>;

Result<std::vector<std::shared_ptr<FileFragment>>>
ParquetDatasetFactory::CollectParquetFragments(
    const parquet::FileMetaData& metadata,
    const parquet::ArrowReaderProperties& properties) {
  try {
    auto n_columns = metadata.num_columns();
    if (n_columns == 0) {
      return Status::Invalid("ParquetDatasetFactory at least one column");
    }

    std::unordered_map<std::string, ExpressionVector> paths_and_row_group_size;

    ARROW_ASSIGN_OR_RAISE(auto manifest, GetSchemaManifest(metadata, properties));

    for (int i = 0; i < metadata.num_row_groups(); i++) {
      auto row_group = metadata.RowGroup(i);
      ARROW_ASSIGN_OR_RAISE(auto path, FileFromRowGroup(base_path_, *row_group));
      auto stats = RowGroupStatisticsAsExpression(*row_group, manifest, properties);

      // Insert the path, or increase the count of row groups. It will be
      // assumed that the RowGroup of a file are ordered exactly like in
      // the metadata file.
      auto elem_and_inserted = paths_and_row_group_size.insert({path, {stats}});
      if (!elem_and_inserted.second) {
        auto& path_and_count = *elem_and_inserted.first;
        path_and_count.second.push_back(stats);
      }
    }

    std::vector<std::shared_ptr<FileFragment>> fragments;
    for (const auto& elem : paths_and_row_group_size) {
      const auto& path = elem.first;
      const auto& statistics = elem.second;

      int num_row_groups = statistics.size();
      ARROW_ASSIGN_OR_RAISE(
          auto fragment,
          format_->MakeFragment({path, filesystem_}, scalar(true),
                                internal::Iota(num_row_groups), statistics));
      fragments.push_back(std::move(fragment));
    }

    return fragments;
  } catch (const ::parquet::ParquetException& e) {
    return Status::Invalid("Could not infer file paths from FileMetaData:", e.what());
  }
}

Result<std::shared_ptr<Dataset>> ParquetDatasetFactory::Finish(FinishOptions options) {
  std::shared_ptr<Schema> schema = options.schema;
  bool schema_missing = schema == nullptr;
  if (schema_missing) {
    ARROW_ASSIGN_OR_RAISE(schema, Inspect(options.inspect_options));
  }

  auto properties = MakeArrowReaderProperties(*format_, *metadata_);
  ARROW_ASSIGN_OR_RAISE(auto fragments, CollectParquetFragments(*metadata_, properties));
  return FileSystemDataset::Make(std::move(schema), scalar(true), format_, fragments);
}

}  // namespace dataset
}  // namespace arrow
