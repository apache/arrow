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
  ParquetScanTask(RowGroupInfo row_group, std::vector<int> column_projection,
                  std::shared_ptr<parquet::arrow::FileReader> reader,
                  std::shared_ptr<ScanOptions> options,
                  std::shared_ptr<ScanContext> context)
      : ScanTask(std::move(options), std::move(context)),
        row_group_(std::move(row_group)),
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
    RETURN_NOT_OK(reader_->GetRecordBatchReader({row_group_.id()}, column_projection_,
                                                &record_batch_reader));
    return IteratorFromReader(std::move(record_batch_reader));
  }

 private:
  RowGroupInfo row_group_;
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
    const parquet::RowGroupMetaData& metadata, const SchemaManifest& manifest) {
  const auto& fields = manifest.schema_fields;
  ExpressionVector expressions;
  expressions.reserve(fields.size());
  for (const auto& field : fields) {
    expressions.emplace_back(ColumnChunkStatisticsAsExpression(field, metadata));
  }

  return expressions.empty() ? scalar(true) : and_(expressions);
}

class ParquetScanTaskIterator {
 public:
  static Result<ScanTaskIterator> Make(std::shared_ptr<ScanOptions> options,
                                       std::shared_ptr<ScanContext> context,
                                       FileSource source,
                                       std::unique_ptr<parquet::arrow::FileReader> reader,
                                       std::vector<RowGroupInfo> row_groups) {
    auto column_projection = InferColumnProjection(*reader, *options);
    return static_cast<ScanTaskIterator>(ParquetScanTaskIterator(
        std::move(options), std::move(context), std::move(source), std::move(reader),
        std::move(column_projection), std::move(row_groups)));
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
  static std::vector<int> InferColumnProjection(const parquet::arrow::FileReader& reader,
                                                const ScanOptions& options) {
    auto manifest = reader.manifest();
    // Checks if the field is needed in either the projection or the filter.
    auto field_names = options.MaterializedFields();
    std::unordered_set<std::string> materialized_fields{field_names.cbegin(),
                                                        field_names.cend()};
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
                          std::shared_ptr<ScanContext> context, FileSource source,
                          std::unique_ptr<parquet::arrow::FileReader> reader,
                          std::vector<int> column_projection,
                          std::vector<RowGroupInfo> row_groups)
      : options_(std::move(options)),
        context_(std::move(context)),
        source_(std::move(source)),
        reader_(std::move(reader)),
        column_projection_(std::move(column_projection)),
        row_groups_(std::move(row_groups)) {}

  std::shared_ptr<ScanOptions> options_;
  std::shared_ptr<ScanContext> context_;

  FileSource source_;
  std::shared_ptr<parquet::arrow::FileReader> reader_;

  std::vector<int> column_projection_;
  std::vector<RowGroupInfo> row_groups_;

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
    const FileSource& source, ScanOptions* options, ScanContext* context) const {
  MemoryPool* pool = context ? context->pool : default_memory_pool();
  auto properties = MakeReaderProperties(*this, pool);
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(source, std::move(properties)));

  auto metadata = reader->metadata();
  auto arrow_properties = MakeArrowReaderProperties(*this, *metadata);

  if (options) {
    arrow_properties.set_batch_size(options->batch_size);
  }

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  RETURN_NOT_OK(parquet::arrow::FileReader::Make(
      pool, std::move(reader), std::move(arrow_properties), &arrow_reader));
  return std::move(arrow_reader);
}

Result<ScanTaskIterator> ParquetFileFormat::ScanFile(
    const FileSource& source, std::shared_ptr<ScanOptions> options,
    std::shared_ptr<ScanContext> context) const {
  return ScanFile(source, std::move(options), std::move(context), {});
}

static inline bool RowGroupInfosAreComplete(const std::vector<RowGroupInfo>& infos) {
  return !infos.empty() &&
         std::all_of(infos.cbegin(), infos.cend(),
                     [](const RowGroupInfo& i) { return i.HasStatistics(); });
}

static inline std::vector<RowGroupInfo> FilterRowGroups(
    std::vector<RowGroupInfo> row_groups, const Expression& predicate) {
  auto filter = [&predicate](const RowGroupInfo& info) {
    return !info.Satisfy(predicate);
  };
  auto end = std::remove_if(row_groups.begin(), row_groups.end(), filter);
  row_groups.erase(end, row_groups.end());
  return row_groups;
}

static inline Result<std::vector<RowGroupInfo>> AugmentRowGroups(
    std::vector<RowGroupInfo> row_groups, parquet::arrow::FileReader* reader) {
  auto metadata = reader->parquet_reader()->metadata();
  auto manifest = reader->manifest();
  auto num_row_groups = metadata->num_row_groups();

  if (row_groups.empty()) {
    row_groups = RowGroupInfo::FromCount(num_row_groups);
  }

  // Augment a RowGroup with statistics if missing.
  auto augment = [&](RowGroupInfo& info) {
    if (!info.HasStatistics() && info.id() < num_row_groups) {
      auto row_group = metadata->RowGroup(info.id());
      info.set_num_rows(row_group->num_rows());
      info.set_statistics(RowGroupStatisticsAsExpression(*row_group, manifest));
    }
  };
  std::for_each(row_groups.begin(), row_groups.end(), augment);

  return row_groups;
}

Result<ScanTaskIterator> ParquetFileFormat::ScanFile(
    const FileSource& source, std::shared_ptr<ScanOptions> options,
    std::shared_ptr<ScanContext> context, std::vector<RowGroupInfo> row_groups) const {
  bool row_groups_are_complete = RowGroupInfosAreComplete(row_groups);
  // The following block is required to avoid any IO if all RowGroups are
  // excluded due to prior statistics knowledge.
  if (row_groups_are_complete) {
    // Apply a pre-filtering if the user requested an explicit sub-set of
    // row-groups. In the case where a RowGroup doesn't have statistics
    // metdata, it will not be excluded.
    row_groups = FilterRowGroups(std::move(row_groups), *options->filter);
    if (row_groups.empty()) {
      return MakeEmptyIterator<std::shared_ptr<ScanTask>>();
    }
  }

  // Open the reader and pay the real IO cost.
  ARROW_ASSIGN_OR_RAISE(auto reader, GetReader(source, options.get(), context.get()));

  // Ensure RowGroups are indexing valid RowGroups before augmenting.
  auto num_row_groups = reader->num_row_groups();
  for (const auto& row_group : row_groups) {
    if (row_group.id() >= num_row_groups) {
      return Status::IndexError("Trying to scan row group ", row_group.id(), " but ",
                                source.path(), " only has ", num_row_groups,
                                " row groups");
    }
  }

  if (!row_groups_are_complete) {
    ARROW_ASSIGN_OR_RAISE(row_groups,
                          AugmentRowGroups(std::move(row_groups), reader.get()));
    row_groups = FilterRowGroups(std::move(row_groups), *options->filter);
  }

  if (row_groups.empty()) {
    return MakeEmptyIterator<std::shared_ptr<ScanTask>>();
  }

  return ParquetScanTaskIterator::Make(std::move(options), std::move(context), source,
                                       std::move(reader), std::move(row_groups));
}

Result<std::shared_ptr<FileFragment>> ParquetFileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Expression> partition_expression,
    std::vector<RowGroupInfo> row_groups) {
  return std::shared_ptr<FileFragment>(
      new ParquetFileFragment(std::move(source), shared_from_this(),
                              std::move(partition_expression), std::move(row_groups)));
}

Result<std::shared_ptr<FileFragment>> ParquetFileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Expression> partition_expression,
    std::vector<int> row_groups) {
  return std::shared_ptr<FileFragment>(new ParquetFileFragment(
      std::move(source), shared_from_this(), std::move(partition_expression),
      RowGroupInfo::FromIdentifiers(row_groups)));
}

Result<std::shared_ptr<FileFragment>> ParquetFileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Expression> partition_expression) {
  return std::shared_ptr<FileFragment>(new ParquetFileFragment(
      std::move(source), shared_from_this(), std::move(partition_expression), {}));
}

///
/// RowGroupInfo
///

std::vector<RowGroupInfo> RowGroupInfo::FromIdentifiers(const std::vector<int> ids) {
  std::vector<RowGroupInfo> results;
  results.reserve(ids.size());
  for (auto i : ids) {
    results.emplace_back(i);
  }
  return results;
}

std::vector<RowGroupInfo> RowGroupInfo::FromCount(int count) {
  std::vector<RowGroupInfo> result;
  result.reserve(count);
  for (int i = 0; i < count; i++) {
    result.emplace_back(i);
  }
  return result;
}

bool RowGroupInfo::Satisfy(const Expression& predicate) const {
  return !HasStatistics() || predicate.IsSatisfiableWith(statistics_);
}

///
/// ParquetFileFragment
///

ParquetFileFragment::ParquetFileFragment(FileSource source,
                                         std::shared_ptr<FileFormat> format,
                                         std::shared_ptr<Expression> partition_expression,
                                         std::vector<RowGroupInfo> row_groups)
    : FileFragment(std::move(source), std::move(format), std::move(partition_expression)),
      row_groups_(std::move(row_groups)),
      parquet_format_(internal::checked_cast<ParquetFileFormat&>(*format_)),
      has_complete_metadata_(RowGroupInfosAreComplete(row_groups_)) {}

Result<ScanTaskIterator> ParquetFileFragment::Scan(std::shared_ptr<ScanOptions> options,
                                                   std::shared_ptr<ScanContext> context) {
  return parquet_format_.ScanFile(source_, std::move(options), std::move(context),
                                  row_groups_);
}

Result<FragmentVector> ParquetFileFragment::SplitByRowGroup(
    const std::shared_ptr<Expression>& predicate) {
  std::vector<RowGroupInfo> row_groups;
  if (HasCompleteMetadata()) {
    row_groups = FilterRowGroups(row_groups_, *predicate);
  } else {
    ARROW_ASSIGN_OR_RAISE(auto reader, parquet_format_.GetReader(source_));
    ARROW_ASSIGN_OR_RAISE(row_groups, AugmentRowGroups(row_groups_, reader.get()));
    row_groups = FilterRowGroups(std::move(row_groups), *predicate);
  }

  FragmentVector fragments;
  fragments.reserve(row_groups.size());
  for (auto&& row_group : row_groups) {
    ARROW_ASSIGN_OR_RAISE(auto fragment,
                          parquet_format_.MakeFragment(source_, partition_expression(),
                                                       {std::move(row_group)}));
    fragments.push_back(std::move(fragment));
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

static Result<std::string> FileFromRowGroup(const std::string& base_path,
                                            const parquet::RowGroupMetaData& row_group) {
  try {
    auto n_columns = row_group.num_columns();
    if (n_columns == 0) {
      return Status::Invalid(
          "Extracting file path from RowGroup failed. RowGroup must have a least one "
          "columns to extract path");
    }

    auto first_column = row_group.ColumnChunk(0);
    auto path = first_column->file_path();
    if (path == "") {
      return Status::Invalid(
          "Extracting file path from RowGroup failed. The column chunks "
          "file path should be set, but got an empty file path.");
    }

    for (int i = 1; i < n_columns; i++) {
      auto column = row_group.ColumnChunk(i);
      auto column_path = column->file_path();
      if (column_path != path) {
        return Status::Invalid("Extracting file path from RowGroup failed. Path '",
                               column_path, "' not equal to path '", path,
                               ", for ColumnChunk at index ", i,
                               "; ColumnChunks in a RowGroup must have the same path.");
      }
    }

    return fs::internal::JoinAbstractPath(std::vector<std::string>{base_path, path});
  } catch (const ::parquet::ParquetException& e) {
    return Status::Invalid("Extracting file path from RowGroup failed. Parquet threw:",
                           e.what());
  }
}

Result<std::vector<std::shared_ptr<FileFragment>>>
ParquetDatasetFactory::CollectParquetFragments(
    const parquet::FileMetaData& metadata,
    const parquet::ArrowReaderProperties& properties) {
  try {
    auto n_columns = metadata.num_columns();
    if (n_columns == 0) {
      return Status::Invalid(
          "ParquetDatasetFactory must contain a schema with at least one column");
    }

    std::unordered_map<std::string, std::vector<RowGroupInfo>> path_to_row_group_infos;

    ARROW_ASSIGN_OR_RAISE(auto manifest, GetSchemaManifest(metadata, properties));

    for (int i = 0; i < metadata.num_row_groups(); i++) {
      auto row_group = metadata.RowGroup(i);
      ARROW_ASSIGN_OR_RAISE(auto path, FileFromRowGroup(base_path_, *row_group));
      // Normalizing path is required for Windows.
      ARROW_ASSIGN_OR_RAISE(path, filesystem_->NormalizePath(std::move(path)));
      auto stats = RowGroupStatisticsAsExpression(*row_group, manifest);
      auto num_rows = row_group->num_rows();

      // Insert the path, or increase the count of row groups. It will be
      // assumed that the RowGroup of a file are ordered exactly like in
      // the metadata file.
      auto elem_and_inserted =
          path_to_row_group_infos.insert({path, {{0, num_rows, stats}}});
      if (!elem_and_inserted.second) {
        auto& path_and_count = *elem_and_inserted.first;
        auto& row_groups = path_and_count.second;
        auto row_group_id = static_cast<int>(row_groups.size());
        path_and_count.second.emplace_back(row_group_id, num_rows, stats);
      }
    }

    std::vector<std::shared_ptr<FileFragment>> fragments;
    fragments.reserve(path_to_row_group_infos.size());
    for (auto&& elem : path_to_row_group_infos) {
      ARROW_ASSIGN_OR_RAISE(auto fragment,
                            format_->MakeFragment({std::move(elem.first), filesystem_},
                                                  scalar(true), std::move(elem.second)));
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
  return FileSystemDataset::Make(std::move(schema), scalar(true), format_,
                                 std::move(fragments));
}

}  // namespace dataset
}  // namespace arrow
