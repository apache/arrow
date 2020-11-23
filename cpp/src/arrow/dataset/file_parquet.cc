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
#include "arrow/util/checked_cast.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/range.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/arrow/writer.h"
#include "parquet/file_reader.h"
#include "parquet/properties.h"
#include "parquet/statistics.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

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
    // The memory and IO incurred by the RecordBatchReader is allocated only
    // when Execute is called.
    struct {
      Result<std::shared_ptr<RecordBatch>> operator()() const {
        return record_batch_reader->Next();
      }

      // The RecordBatchIterator must hold a reference to the FileReader;
      // since it must outlive the wrapped RecordBatchReader
      std::shared_ptr<parquet::arrow::FileReader> file_reader;
      std::unique_ptr<RecordBatchReader> record_batch_reader;
    } NextBatch;

    NextBatch.file_reader = reader_;
    RETURN_NOT_OK(reader_->GetRecordBatchReader({row_group_}, column_projection_,
                                                &NextBatch.record_batch_reader));
    return MakeFunctionIterator(std::move(NextBatch));
  }

 private:
  int row_group_;
  std::vector<int> column_projection_;
  std::shared_ptr<parquet::arrow::FileReader> reader_;
};

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
static Result<std::shared_ptr<SchemaManifest>> GetSchemaManifest(
    const M& metadata, const parquet::ArrowReaderProperties& properties) {
  auto manifest = std::make_shared<SchemaManifest>();
  const std::shared_ptr<const ::arrow::KeyValueMetadata>& key_value_metadata = nullptr;
  RETURN_NOT_OK(SchemaManifest::Make(metadata.schema(), key_value_metadata, properties,
                                     manifest.get()));
  return manifest;
}

static std::shared_ptr<Expression> ColumnChunkStatisticsAsExpression(
    const SchemaField& schema_field, const parquet::RowGroupMetaData& metadata) {
  // For the remaining of this function, failure to extract/parse statistics
  // are ignored by returning nullptr. The goal is two fold. First
  // avoid an optimization which breaks the computation. Second, allow the
  // following columns to maybe succeed in extracting column statistics.

  // For now, only leaf (primitive) types are supported.
  if (!schema_field.is_leaf()) {
    return nullptr;
  }

  auto column_metadata = metadata.ColumnChunk(schema_field.column_index);
  auto statistics = column_metadata->statistics();
  if (statistics == nullptr) {
    return nullptr;
  }

  const auto& field = schema_field.field;
  auto field_expr = field_ref(field->name());

  // Optimize for corner case where all values are nulls
  if (statistics->num_values() == statistics->null_count()) {
    return equal(std::move(field_expr), scalar(MakeNullScalar(field->type())));
  }

  std::shared_ptr<Scalar> min, max;
  if (!StatisticsAsScalars(*statistics, &min, &max).ok()) {
    return nullptr;
  }

  auto maybe_min = min->CastTo(field->type());
  auto maybe_max = max->CastTo(field->type());
  if (maybe_min.ok() && maybe_max.ok()) {
    min = maybe_min.MoveValueUnsafe();
    max = maybe_max.MoveValueUnsafe();
    return and_(greater_equal(field_expr, scalar(min)),
                less_equal(field_expr, scalar(max)));
  }

  return nullptr;
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

bool ParquetFileFormat::Equals(const FileFormat& other) const {
  if (other.type_name() != type_name()) return false;

  const auto& other_reader_options =
      checked_cast<const ParquetFileFormat&>(other).reader_options;

  // FIXME implement comparison for decryption options
  // FIXME extract these to scan time options so comparison is unnecessary
  return reader_options.use_buffered_stream == other_reader_options.use_buffered_stream &&
         reader_options.buffer_size == other_reader_options.buffer_size &&
         reader_options.dict_columns == other_reader_options.dict_columns;
}

ParquetFileFormat::ParquetFileFormat(const parquet::ReaderProperties& reader_properties) {
  reader_options.use_buffered_stream = reader_properties.is_buffered_stream_enabled();
  reader_options.buffer_size = reader_properties.buffer_size();
  reader_options.file_decryption_properties =
      reader_properties.file_decryption_properties();
}

Result<bool> ParquetFileFormat::IsSupported(const FileSource& source) const {
  try {
    ARROW_ASSIGN_OR_RAISE(auto input, source.Open());
    auto reader =
        parquet::ParquetFileReader::Open(std::move(input), MakeReaderProperties(*this));
    std::shared_ptr<parquet::FileMetaData> metadata = reader->metadata();
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

  ARROW_ASSIGN_OR_RAISE(auto input, source.Open());
  std::unique_ptr<parquet::ParquetFileReader> reader;
  try {
    reader = parquet::ParquetFileReader::Open(std::move(input), std::move(properties));
  } catch (const ::parquet::ParquetException& e) {
    return Status::IOError("Could not open parquet input source '", source.path(),
                           "': ", e.what());
  }

  std::shared_ptr<parquet::FileMetaData> metadata = reader->metadata();
  auto arrow_properties = MakeArrowReaderProperties(*this, *metadata);

  if (options) {
    arrow_properties.set_batch_size(options->batch_size);
  }

  if (context && !context->use_threads) {
    arrow_properties.set_use_threads(reader_options.enable_parallel_column_conversion);
  }

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  RETURN_NOT_OK(parquet::arrow::FileReader::Make(
      pool, std::move(reader), std::move(arrow_properties), &arrow_reader));
  return std::move(arrow_reader);
}

Result<ScanTaskIterator> ParquetFileFormat::ScanFile(std::shared_ptr<ScanOptions> options,
                                                     std::shared_ptr<ScanContext> context,
                                                     FileFragment* fragment) const {
  auto* parquet_fragment = checked_cast<ParquetFileFragment*>(fragment);
  std::vector<int> row_groups;

  bool pre_filtered = false;
  auto empty = [] { return MakeEmptyIterator<std::shared_ptr<ScanTask>>(); };

  // If RowGroup metadata is cached completely we can pre-filter RowGroups before opening
  // a FileReader, potentially avoiding IO altogether if all RowGroups are excluded due to
  // prior statistics knowledge. In the case where a RowGroup doesn't have statistics
  // metdata, it will not be excluded.
  if (parquet_fragment->metadata() != nullptr) {
    ARROW_ASSIGN_OR_RAISE(row_groups,
                          parquet_fragment->FilterRowGroups(*options->filter));

    pre_filtered = true;
    if (row_groups.empty()) empty();
  }

  // Open the reader and pay the real IO cost.
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<parquet::arrow::FileReader> reader,
                        GetReader(fragment->source(), options.get(), context.get()));

  // Ensure that parquet_fragment has FileMetaData
  RETURN_NOT_OK(parquet_fragment->EnsureCompleteMetadata(reader.get()));

  if (!pre_filtered) {
    // row groups were not already filtered; do this now
    ARROW_ASSIGN_OR_RAISE(row_groups,
                          parquet_fragment->FilterRowGroups(*options->filter));

    if (row_groups.empty()) empty();
  }

  auto column_projection = InferColumnProjection(*reader, *options);
  ScanTaskVector tasks(row_groups.size());

  for (size_t i = 0; i < row_groups.size(); ++i) {
    tasks[i] = std::make_shared<ParquetScanTask>(row_groups[i], column_projection, reader,
                                                 options, context);
  }

  return MakeVectorIterator(std::move(tasks));
}

Result<std::shared_ptr<ParquetFileFragment>> ParquetFileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Expression> partition_expression,
    std::shared_ptr<Schema> physical_schema, std::vector<int> row_groups) {
  return std::shared_ptr<ParquetFileFragment>(new ParquetFileFragment(
      std::move(source), shared_from_this(), std::move(partition_expression),
      std::move(physical_schema), std::move(row_groups)));
}

Result<std::shared_ptr<FileFragment>> ParquetFileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Expression> partition_expression,
    std::shared_ptr<Schema> physical_schema) {
  return std::shared_ptr<FileFragment>(new ParquetFileFragment(
      std::move(source), shared_from_this(), std::move(partition_expression),
      std::move(physical_schema), util::nullopt));
}

//
// ParquetFileWriter, ParquetFileWriteOptions
//

std::shared_ptr<FileWriteOptions> ParquetFileFormat::DefaultWriteOptions() {
  std::shared_ptr<ParquetFileWriteOptions> options(
      new ParquetFileWriteOptions(shared_from_this()));
  options->writer_properties = parquet::default_writer_properties();
  options->arrow_writer_properties = parquet::default_arrow_writer_properties();
  return options;
}

Result<std::shared_ptr<FileWriter>> ParquetFileFormat::MakeWriter(
    std::shared_ptr<io::OutputStream> destination, std::shared_ptr<Schema> schema,
    std::shared_ptr<FileWriteOptions> options) const {
  if (!Equals(*options->format())) {
    return Status::TypeError("Mismatching format/write options");
  }

  auto parquet_options = checked_pointer_cast<ParquetFileWriteOptions>(options);

  std::unique_ptr<parquet::arrow::FileWriter> parquet_writer;
  RETURN_NOT_OK(parquet::arrow::FileWriter::Open(
      *schema, default_memory_pool(), destination, parquet_options->writer_properties,
      parquet_options->arrow_writer_properties, &parquet_writer));

  return std::shared_ptr<FileWriter>(
      new ParquetFileWriter(std::move(parquet_writer), std::move(parquet_options)));
}

ParquetFileWriter::ParquetFileWriter(std::shared_ptr<parquet::arrow::FileWriter> writer,
                                     std::shared_ptr<ParquetFileWriteOptions> options)
    : FileWriter(writer->schema(), std::move(options)),
      parquet_writer_(std::move(writer)) {}

Status ParquetFileWriter::Write(const std::shared_ptr<RecordBatch>& batch) {
  ARROW_ASSIGN_OR_RAISE(auto table, Table::FromRecordBatches(batch->schema(), {batch}));
  return parquet_writer_->WriteTable(*table, batch->num_rows());
}

Status ParquetFileWriter::Finish() { return parquet_writer_->Close(); }

//
// ParquetFileFragment
//

ParquetFileFragment::ParquetFileFragment(FileSource source,
                                         std::shared_ptr<FileFormat> format,
                                         std::shared_ptr<Expression> partition_expression,
                                         std::shared_ptr<Schema> physical_schema,
                                         util::optional<std::vector<int>> row_groups)
    : FileFragment(std::move(source), std::move(format), std::move(partition_expression),
                   std::move(physical_schema)),
      parquet_format_(checked_cast<ParquetFileFormat&>(*format_)),
      row_groups_(std::move(row_groups)) {}

Status ParquetFileFragment::EnsureCompleteMetadata(parquet::arrow::FileReader* reader) {
  auto lock = physical_schema_mutex_.Lock();
  if (metadata_ != nullptr) {
    return Status::OK();
  }

  if (reader == nullptr) {
    lock.Unlock();
    ARROW_ASSIGN_OR_RAISE(auto reader, parquet_format_.GetReader(source_));
    return EnsureCompleteMetadata(reader.get());
  }

  std::shared_ptr<Schema> schema;
  RETURN_NOT_OK(reader->GetSchema(&schema));
  if (physical_schema_ && !physical_schema_->Equals(*schema)) {
    return Status::Invalid("Fragment initialized with physical schema ",
                           *physical_schema_, " but ", source_.path(), " has schema ",
                           *schema);
  }
  physical_schema_ = std::move(schema);

  if (!row_groups_) {
    row_groups_ = internal::Iota(reader->num_row_groups());
  }

  ARROW_ASSIGN_OR_RAISE(
      auto manifest,
      GetSchemaManifest(*reader->parquet_reader()->metadata(), reader->properties()));
  return SetMetadata(reader->parquet_reader()->metadata(), std::move(manifest));
}

Status ParquetFileFragment::SetMetadata(
    std::shared_ptr<parquet::FileMetaData> metadata,
    std::shared_ptr<parquet::arrow::SchemaManifest> manifest) {
  DCHECK(row_groups_.has_value());

  metadata_ = std::move(metadata);
  manifest_ = std::move(manifest);

  statistics_expressions_.resize(row_groups_->size(), scalar(true));
  statistics_expressions_complete_.resize(physical_schema_->num_fields(), false);

  for (int row_group : *row_groups_) {
    // Ensure RowGroups are indexing valid RowGroups before augmenting.
    if (row_group < metadata_->num_row_groups()) continue;

    return Status::IndexError("ParquetFileFragment references row group ", row_group,
                              " but ", source_.path(), " only has ",
                              metadata_->num_row_groups(), " row groups");
  }

  return Status::OK();
}

Result<FragmentVector> ParquetFileFragment::SplitByRowGroup(
    const std::shared_ptr<Expression>& predicate) {
  RETURN_NOT_OK(EnsureCompleteMetadata());
  ARROW_ASSIGN_OR_RAISE(auto row_groups, FilterRowGroups(*predicate));

  FragmentVector fragments(row_groups.size());
  int i = 0;
  for (int row_group : row_groups) {
    ARROW_ASSIGN_OR_RAISE(auto fragment,
                          parquet_format_.MakeFragment(source_, partition_expression(),
                                                       physical_schema_, {row_group}));

    RETURN_NOT_OK(fragment->SetMetadata(metadata_, manifest_));
    fragments[i++] = std::move(fragment);
  }

  return fragments;
}

Result<std::shared_ptr<Fragment>> ParquetFileFragment::Subset(
    const std::shared_ptr<Expression>& predicate) {
  RETURN_NOT_OK(EnsureCompleteMetadata());
  ARROW_ASSIGN_OR_RAISE(auto row_groups, FilterRowGroups(*predicate));
  return Subset(std::move(row_groups));
}

Result<std::shared_ptr<Fragment>> ParquetFileFragment::Subset(
    std::vector<int> row_groups) {
  RETURN_NOT_OK(EnsureCompleteMetadata());
  ARROW_ASSIGN_OR_RAISE(auto new_fragment, parquet_format_.MakeFragment(
                                               source_, partition_expression(),
                                               physical_schema_, std::move(row_groups)));

  RETURN_NOT_OK(new_fragment->SetMetadata(metadata_, manifest_));
  return new_fragment;
}

inline void FoldingAnd(std::shared_ptr<Expression>* l, std::shared_ptr<Expression> r) {
  if ((*l)->Equals(true)) {
    *l = std::move(r);
  } else {
    *l = and_(std::move(*l), std::move(r));
  }
}

Result<std::vector<int>> ParquetFileFragment::FilterRowGroups(
    const Expression& predicate) {
  auto lock = physical_schema_mutex_.Lock();

  DCHECK_NE(metadata_, nullptr);
  RETURN_NOT_OK(predicate.Validate(*physical_schema_));

  for (FieldRef ref : FieldsInExpression(predicate)) {
    ARROW_ASSIGN_OR_RAISE(auto path, ref.FindOneOrNone(*physical_schema_));

    if (!path) continue;
    if (statistics_expressions_complete_[path[0]]) continue;
    statistics_expressions_complete_[path[0]] = true;

    const SchemaField& schema_field = manifest_->schema_fields[path[0]];
    int i = 0;
    for (int row_group : *row_groups_) {
      auto row_group_metadata = metadata_->RowGroup(row_group);

      if (auto minmax =
              ColumnChunkStatisticsAsExpression(schema_field, *row_group_metadata)) {
        FoldingAnd(&statistics_expressions_[i], std::move(minmax));
      }

      ++i;
    }
  }

  auto simplified_predicate = predicate.Assume(partition_expression_);
  if (!simplified_predicate->IsSatisfiable()) {
    return std::vector<int>{};
  }

  std::vector<int> row_groups;
  for (size_t i = 0; i < row_groups_->size(); ++i) {
    if (simplified_predicate->IsSatisfiableWith(statistics_expressions_[i])) {
      row_groups.push_back(row_groups_->at(i));
    }
  }

  return row_groups;
}

//
// ParquetDatasetFactory
//

static inline Result<std::string> FileFromRowGroup(
    fs::FileSystem* filesystem, const std::string& base_path,
    const parquet::RowGroupMetaData& row_group, bool validate_column_chunk_paths) {
  constexpr auto prefix = "Extracting file path from RowGroup failed. ";

  if (row_group.num_columns() == 0) {
    return Status::Invalid(prefix,
                           "RowGroup must have a least one column to extract path.");
  }

  auto path = row_group.ColumnChunk(0)->file_path();
  if (path == "") {
    return Status::Invalid(
        prefix,
        "The column chunks' file paths should be set, but got an empty file path.");
  }

  if (validate_column_chunk_paths) {
    for (int i = 1; i < row_group.num_columns(); ++i) {
      const auto& column_path = row_group.ColumnChunk(i)->file_path();
      if (column_path != path) {
        return Status::Invalid(prefix, "Path '", column_path, "' not equal to path '",
                               path, ", for ColumnChunk at index ", i,
                               "; ColumnChunks in a RowGroup must have the same path.");
      }
    }
  }

  path = fs::internal::JoinAbstractPath(
      std::vector<std::string>{base_path, std::move(path)});
  // Normalizing path is required for Windows.
  return filesystem->NormalizePath(std::move(path));
}

Result<std::shared_ptr<Schema>> GetSchema(
    const parquet::FileMetaData& metadata,
    const parquet::ArrowReaderProperties& properties) {
  std::shared_ptr<Schema> schema;
  RETURN_NOT_OK(parquet::arrow::FromParquetSchema(
      metadata.schema(), properties, metadata.key_value_metadata(), &schema));
  return schema;
}

Result<std::shared_ptr<DatasetFactory>> ParquetDatasetFactory::Make(
    const std::string& metadata_path, std::shared_ptr<fs::FileSystem> filesystem,
    std::shared_ptr<ParquetFileFormat> format, ParquetFactoryOptions options) {
  // Paths in ColumnChunk are relative to the `_metadata` file. Thus, the base
  // directory of all parquet files is `dirname(metadata_path)`.
  auto dirname = arrow::fs::internal::GetAbstractPathParent(metadata_path).first;
  return Make({metadata_path, filesystem}, dirname, filesystem, std::move(format),
              std::move(options));
}

Result<std::shared_ptr<DatasetFactory>> ParquetDatasetFactory::Make(
    const FileSource& metadata_source, const std::string& base_path,
    std::shared_ptr<fs::FileSystem> filesystem, std::shared_ptr<ParquetFileFormat> format,
    ParquetFactoryOptions options) {
  DCHECK_NE(filesystem, nullptr);
  DCHECK_NE(format, nullptr);

  // By automatically setting the options base_dir to the metadata's base_path,
  // we provide a better experience for user providing Partitioning that are
  // relative to the base_dir instead of the full path.
  if (options.partition_base_dir.empty()) {
    options.partition_base_dir = base_path;
  }

  ARROW_ASSIGN_OR_RAISE(auto reader, format->GetReader(metadata_source));
  std::shared_ptr<parquet::FileMetaData> metadata = reader->parquet_reader()->metadata();

  if (metadata->num_columns() == 0) {
    return Status::Invalid(
        "ParquetDatasetFactory must contain a schema with at least one column");
  }

  auto properties = MakeArrowReaderProperties(*format, *metadata);
  ARROW_ASSIGN_OR_RAISE(auto physical_schema, GetSchema(*metadata, properties));
  ARROW_ASSIGN_OR_RAISE(auto manifest, GetSchemaManifest(*metadata, properties));

  std::unordered_map<std::string, std::vector<int>> path_to_row_group_ids;

  for (int i = 0; i < metadata->num_row_groups(); i++) {
    auto row_group = metadata->RowGroup(i);
    ARROW_ASSIGN_OR_RAISE(auto path,
                          FileFromRowGroup(filesystem.get(), base_path, *row_group,
                                           options.validate_column_chunk_paths));

    // Insert the path, or increase the count of row groups. It will be assumed that the
    // RowGroup of a file are ordered exactly as in the metadata file.
    auto row_groups = &path_to_row_group_ids.insert({std::move(path), {}}).first->second;
    row_groups->emplace_back(i);
  }

  return std::shared_ptr<DatasetFactory>(new ParquetDatasetFactory(
      std::move(filesystem), std::move(format), std::move(metadata), std::move(manifest),
      std::move(physical_schema), base_path, std::move(options),
      std::move(path_to_row_group_ids)));
}

Result<std::vector<std::shared_ptr<FileFragment>>>
ParquetDatasetFactory::CollectParquetFragments(const Partitioning& partitioning) {
  std::vector<std::shared_ptr<FileFragment>> fragments(path_to_row_group_ids_.size());

  size_t i = 0;
  for (const auto& e : path_to_row_group_ids_) {
    const auto& path = e.first;
    auto metadata_subset = metadata_->Subset(e.second);

    auto row_groups = internal::Iota(metadata_subset->num_row_groups());

    auto partition_expression =
        partitioning.Parse(StripPrefixAndFilename(path, options_.partition_base_dir))
            .ValueOr(scalar(true));

    ARROW_ASSIGN_OR_RAISE(
        auto fragment,
        format_->MakeFragment({path, filesystem_}, std::move(partition_expression),
                              physical_schema_, std::move(row_groups)));

    RETURN_NOT_OK(fragment->SetMetadata(metadata_subset, manifest_));
    fragments[i++] = std::move(fragment);
  }

  return fragments;
}

Result<std::vector<std::shared_ptr<Schema>>> ParquetDatasetFactory::InspectSchemas(
    InspectOptions options) {
  // The physical_schema from the _metadata file is always yielded
  std::vector<std::shared_ptr<Schema>> schemas = {physical_schema_};

  if (auto factory = options_.partitioning.factory()) {
    // Gather paths found in RowGroups' ColumnChunks.
    std::vector<std::string> stripped(path_to_row_group_ids_.size());

    size_t i = 0;
    for (const auto& e : path_to_row_group_ids_) {
      stripped[i++] = StripPrefixAndFilename(e.first, options_.partition_base_dir);
    }
    ARROW_ASSIGN_OR_RAISE(auto partition_schema, factory->Inspect(stripped));

    schemas.push_back(std::move(partition_schema));
  } else {
    schemas.push_back(options_.partitioning.partitioning()->schema());
  }

  return schemas;
}

Result<std::shared_ptr<Dataset>> ParquetDatasetFactory::Finish(FinishOptions options) {
  std::shared_ptr<Schema> schema = options.schema;
  bool schema_missing = schema == nullptr;
  if (schema_missing) {
    ARROW_ASSIGN_OR_RAISE(schema, Inspect(options.inspect_options));
  }

  std::shared_ptr<Partitioning> partitioning = options_.partitioning.partitioning();
  if (partitioning == nullptr) {
    auto factory = options_.partitioning.factory();
    ARROW_ASSIGN_OR_RAISE(partitioning, factory->Finish(schema));
  }

  ARROW_ASSIGN_OR_RAISE(auto fragments, CollectParquetFragments(*partitioning));
  return FileSystemDataset::Make(std::move(schema), scalar(true), format_, filesystem_,
                                 std::move(fragments));
}

}  // namespace dataset
}  // namespace arrow
