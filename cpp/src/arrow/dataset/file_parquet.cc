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
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/compute/exec.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/scanner.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/table.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/range.h"
#include "arrow/util/tracing_internal.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/arrow/writer.h"
#include "parquet/file_reader.h"
#include "parquet/properties.h"
#include "parquet/statistics.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;
using internal::Iota;

namespace dataset {

using parquet::arrow::SchemaField;
using parquet::arrow::SchemaManifest;
using parquet::arrow::StatisticsAsScalars;

namespace {

parquet::ReaderProperties MakeReaderProperties(
    const ParquetFileFormat& format, ParquetFragmentScanOptions* parquet_scan_options,
    MemoryPool* pool = default_memory_pool()) {
  // Can't mutate pool after construction
  parquet::ReaderProperties properties(pool);
  if (parquet_scan_options->reader_properties->is_buffered_stream_enabled()) {
    properties.enable_buffered_stream();
  } else {
    properties.disable_buffered_stream();
  }
  properties.set_buffer_size(parquet_scan_options->reader_properties->buffer_size());
  properties.file_decryption_properties(
      parquet_scan_options->reader_properties->file_decryption_properties());
  properties.set_thrift_string_size_limit(
      parquet_scan_options->reader_properties->thrift_string_size_limit());
  properties.set_thrift_container_size_limit(
      parquet_scan_options->reader_properties->thrift_container_size_limit());
  return properties;
}

parquet::ArrowReaderProperties MakeArrowReaderProperties(
    const ParquetFileFormat& format, const parquet::FileMetaData& metadata) {
  parquet::ArrowReaderProperties properties(/* use_threads = */ false);
  for (const std::string& name : format.reader_options.dict_columns) {
    auto column_index = metadata.schema()->ColumnIndex(name);
    properties.set_read_dictionary(column_index, true);
  }
  properties.set_coerce_int96_timestamp_unit(
      format.reader_options.coerce_int96_timestamp_unit);
  return properties;
}

template <typename M>
Result<std::shared_ptr<SchemaManifest>> GetSchemaManifest(
    const M& metadata, const parquet::ArrowReaderProperties& properties) {
  auto manifest = std::make_shared<SchemaManifest>();
  const std::shared_ptr<const ::arrow::KeyValueMetadata>& key_value_metadata = nullptr;
  RETURN_NOT_OK(SchemaManifest::Make(metadata.schema(), key_value_metadata, properties,
                                     manifest.get()));
  return manifest;
}

bool IsNan(const Scalar& value) {
  if (value.is_valid) {
    if (value.type->id() == Type::FLOAT) {
      const FloatScalar& float_scalar = checked_cast<const FloatScalar&>(value);
      return std::isnan(float_scalar.value);
    } else if (value.type->id() == Type::DOUBLE) {
      const DoubleScalar& double_scalar = checked_cast<const DoubleScalar&>(value);
      return std::isnan(double_scalar.value);
    }
  }
  return false;
}

std::optional<compute::Expression> ColumnChunkStatisticsAsExpression(
    const SchemaField& schema_field, const parquet::RowGroupMetaData& metadata) {
  // For the remaining of this function, failure to extract/parse statistics
  // are ignored by returning nullptr. The goal is two fold. First
  // avoid an optimization which breaks the computation. Second, allow the
  // following columns to maybe succeed in extracting column statistics.

  // For now, only leaf (primitive) types are supported.
  if (!schema_field.is_leaf()) {
    return std::nullopt;
  }

  auto column_metadata = metadata.ColumnChunk(schema_field.column_index);
  auto statistics = column_metadata->statistics();
  const auto& field = schema_field.field;

  if (statistics == nullptr) {
    return std::nullopt;
  }

  return ParquetFileFragment::EvaluateStatisticsAsExpression(*field, *statistics);
}

void AddColumnIndices(const SchemaField& schema_field,
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

Status ResolveOneFieldRef(
    const SchemaManifest& manifest, const FieldRef& field_ref,
    const std::unordered_map<std::string, const SchemaField*>& field_lookup,
    const std::unordered_set<std::string>& duplicate_fields,
    std::vector<int>* columns_selection) {
  if (const std::string* name = field_ref.name()) {
    auto it = field_lookup.find(*name);
    if (it != field_lookup.end()) {
      AddColumnIndices(*it->second, columns_selection);
    } else if (duplicate_fields.find(*name) != duplicate_fields.end()) {
      // We shouldn't generally get here because SetProjection will reject such references
      return Status::Invalid("Ambiguous reference to column '", *name,
                             "' which occurs more than once");
    }
    // "Virtual" column: field is not in file but is in the ScanOptions.
    // Ignore it here, as projection will pad the batch with a null column.
    return Status::OK();
  }

  const SchemaField* toplevel = nullptr;
  const SchemaField* field = nullptr;
  if (const std::vector<FieldRef>* refs = field_ref.nested_refs()) {
    // Only supports a sequence of names
    for (const auto& ref : *refs) {
      if (const std::string* name = ref.name()) {
        if (!field) {
          // First lookup, top-level field
          auto it = field_lookup.find(*name);
          if (it != field_lookup.end()) {
            field = it->second;
            toplevel = field;
          } else if (duplicate_fields.find(*name) != duplicate_fields.end()) {
            return Status::Invalid("Ambiguous reference to column '", *name,
                                   "' which occurs more than once");
          } else {
            // Virtual column
            return Status::OK();
          }
        } else {
          const SchemaField* result = nullptr;
          for (const auto& child : field->children) {
            if (child.field->name() == *name) {
              if (!result) {
                result = &child;
              } else {
                return Status::Invalid("Ambiguous nested reference to column '", *name,
                                       "' which occurs more than once in field ",
                                       field->field->ToString());
              }
            }
          }
          if (!result) {
            // Virtual column
            return Status::OK();
          }
          field = result;
        }
        continue;
      }
      return Status::NotImplemented("Inferring column projection from FieldRef ",
                                    field_ref.ToString());
    }
  } else {
    return Status::NotImplemented("Inferring column projection from FieldRef ",
                                  field_ref.ToString());
  }

  if (field) {
    // TODO(ARROW-1888): support fine-grained column projection. We should be
    // able to materialize only the child fields requested, and not the entire
    // top-level field.
    // Right now, if enabled, projection/filtering will fail when they cast the
    // physical schema to the dataset schema.
    AddColumnIndices(*toplevel, columns_selection);
  }
  return Status::OK();
}

// Compute the column projection based on the scan options
Result<std::vector<int>> InferColumnProjection(const parquet::arrow::FileReader& reader,
                                               const ScanOptions& options) {
  auto manifest = reader.manifest();
  // Checks if the field is needed in either the projection or the filter.
  auto field_refs = options.MaterializedFields();

  // Build a lookup table from top level field name to field metadata.
  // This is to avoid quadratic-time mapping of projected fields to
  // column indices, in the common case of selecting top level
  // columns. For nested fields, we will pay the cost of a linear scan
  // assuming for now that this is relatively rare, but this can be
  // optimized. (Also, we don't want to pay the cost of building all
  // the lookup tables up front if they're rarely used.)
  std::unordered_map<std::string, const SchemaField*> field_lookup;
  std::unordered_set<std::string> duplicate_fields;
  for (const auto& schema_field : manifest.schema_fields) {
    const auto it = field_lookup.emplace(schema_field.field->name(), &schema_field);
    if (!it.second) {
      duplicate_fields.emplace(schema_field.field->name());
    }
  }

  std::vector<int> columns_selection;
  for (const auto& ref : field_refs) {
    RETURN_NOT_OK(ResolveOneFieldRef(manifest, ref, field_lookup, duplicate_fields,
                                     &columns_selection));
  }
  return columns_selection;
}

Status WrapSourceError(const Status& status, const std::string& path) {
  return status.WithMessage("Could not open Parquet input source '", path,
                            "': ", status.message());
}

Result<bool> IsSupportedParquetFile(const ParquetFileFormat& format,
                                    const FileSource& source) {
  BEGIN_PARQUET_CATCH_EXCEPTIONS
  try {
    ARROW_ASSIGN_OR_RAISE(auto input, source.Open());
    ARROW_ASSIGN_OR_RAISE(
        auto parquet_scan_options,
        GetFragmentScanOptions<ParquetFragmentScanOptions>(
            kParquetTypeName, nullptr, format.default_fragment_scan_options));
    auto reader = parquet::ParquetFileReader::Open(
        std::move(input), MakeReaderProperties(format, parquet_scan_options.get()));
    std::shared_ptr<parquet::FileMetaData> metadata = reader->metadata();
    return metadata != nullptr && metadata->can_decompress();
  } catch (const ::parquet::ParquetInvalidOrCorruptedFileException& e) {
    ARROW_UNUSED(e);
    return false;
  }
  END_PARQUET_CATCH_EXCEPTIONS
}

}  // namespace

std::optional<compute::Expression> ParquetFileFragment::EvaluateStatisticsAsExpression(
    const Field& field, const parquet::Statistics& statistics) {
  auto field_expr = compute::field_ref(field.name());

  // Optimize for corner case where all values are nulls
  if (statistics.num_values() == 0 && statistics.null_count() > 0) {
    return is_null(std::move(field_expr));
  }

  std::shared_ptr<Scalar> min, max;
  if (!StatisticsAsScalars(statistics, &min, &max).ok()) {
    return std::nullopt;
  }

  auto maybe_min = min->CastTo(field.type());
  auto maybe_max = max->CastTo(field.type());

  if (maybe_min.ok() && maybe_max.ok()) {
    min = maybe_min.MoveValueUnsafe();
    max = maybe_max.MoveValueUnsafe();

    if (min->Equals(*max)) {
      auto single_value = compute::equal(field_expr, compute::literal(std::move(min)));

      if (statistics.null_count() == 0) {
        return single_value;
      }
      return compute::or_(std::move(single_value), is_null(std::move(field_expr)));
    }

    auto lower_bound = compute::greater_equal(field_expr, compute::literal(min));
    auto upper_bound = compute::less_equal(field_expr, compute::literal(max));
    compute::Expression in_range;

    // Since the minimum & maximum values are NaN, useful statistics
    // cannot be extracted for checking the presence of a value within
    // range
    if (IsNan(*min) && IsNan(*max)) {
      return std::nullopt;
    }

    // If either minimum or maximum is NaN, it should be ignored for the
    // range computation
    if (IsNan(*min)) {
      in_range = std::move(upper_bound);
    } else if (IsNan(*max)) {
      in_range = std::move(lower_bound);
    } else {
      in_range = compute::and_(std::move(lower_bound), std::move(upper_bound));
    }

    if (statistics.null_count() != 0) {
      return compute::or_(std::move(in_range), compute::is_null(field_expr));
    }
    return in_range;
  }
  return std::nullopt;
}

ParquetFileFormat::ParquetFileFormat()
    : FileFormat(std::make_shared<ParquetFragmentScanOptions>()) {}

bool ParquetFileFormat::Equals(const FileFormat& other) const {
  if (other.type_name() != type_name()) return false;

  const auto& other_reader_options =
      checked_cast<const ParquetFileFormat&>(other).reader_options;

  // FIXME implement comparison for decryption options
  return (reader_options.dict_columns == other_reader_options.dict_columns &&
          reader_options.coerce_int96_timestamp_unit ==
              other_reader_options.coerce_int96_timestamp_unit);
}

ParquetFileFormat::ParquetFileFormat(const parquet::ReaderProperties& reader_properties)
    : FileFormat(std::make_shared<ParquetFragmentScanOptions>()) {
  auto* default_scan_opts =
      static_cast<ParquetFragmentScanOptions*>(default_fragment_scan_options.get());
  *default_scan_opts->reader_properties = reader_properties;
}

Result<bool> ParquetFileFormat::IsSupported(const FileSource& source) const {
  auto maybe_is_supported = IsSupportedParquetFile(*this, source);
  if (!maybe_is_supported.ok()) {
    return WrapSourceError(maybe_is_supported.status(), source.path());
  }
  return maybe_is_supported;
}

Result<std::shared_ptr<Schema>> ParquetFileFormat::Inspect(
    const FileSource& source) const {
  auto scan_options = std::make_shared<ScanOptions>();
  ARROW_ASSIGN_OR_RAISE(auto reader, GetReader(source, scan_options));
  std::shared_ptr<Schema> schema;
  RETURN_NOT_OK(reader->GetSchema(&schema));
  return schema;
}

Result<std::shared_ptr<parquet::arrow::FileReader>> ParquetFileFormat::GetReader(
    const FileSource& source, const std::shared_ptr<ScanOptions>& options) const {
  return GetReaderAsync(source, options, nullptr).result();
}

Result<std::shared_ptr<parquet::arrow::FileReader>> ParquetFileFormat::GetReader(
    const FileSource& source, const std::shared_ptr<ScanOptions>& options,
    const std::shared_ptr<parquet::FileMetaData>& metadata) const {
  return GetReaderAsync(source, options, metadata).result();
}

Future<std::shared_ptr<parquet::arrow::FileReader>> ParquetFileFormat::GetReaderAsync(
    const FileSource& source, const std::shared_ptr<ScanOptions>& options) const {
  return GetReaderAsync(source, options, nullptr);
}

Future<std::shared_ptr<parquet::arrow::FileReader>> ParquetFileFormat::GetReaderAsync(
    const FileSource& source, const std::shared_ptr<ScanOptions>& options,
    const std::shared_ptr<parquet::FileMetaData>& metadata) const {
  ARROW_ASSIGN_OR_RAISE(
      auto parquet_scan_options,
      GetFragmentScanOptions<ParquetFragmentScanOptions>(kParquetTypeName, options.get(),
                                                         default_fragment_scan_options));
  auto properties =
      MakeReaderProperties(*this, parquet_scan_options.get(), options->pool);
  ARROW_ASSIGN_OR_RAISE(auto input, source.Open());
  // TODO(ARROW-12259): workaround since we have Future<(move-only type)>
  auto reader_fut = parquet::ParquetFileReader::OpenAsync(
      std::move(input), std::move(properties), metadata);
  auto path = source.path();
  auto self = checked_pointer_cast<const ParquetFileFormat>(shared_from_this());
  return reader_fut.Then(
      [=](const std::unique_ptr<parquet::ParquetFileReader>&) mutable
      -> Result<std::shared_ptr<parquet::arrow::FileReader>> {
        ARROW_ASSIGN_OR_RAISE(std::unique_ptr<parquet::ParquetFileReader> reader,
                              reader_fut.MoveResult());
        std::shared_ptr<parquet::FileMetaData> metadata = reader->metadata();
        auto arrow_properties = MakeArrowReaderProperties(*self, *metadata);
        arrow_properties.set_batch_size(options->batch_size);
        // Must be set here since the sync ScanTask handles pre-buffering itself
        arrow_properties.set_pre_buffer(
            parquet_scan_options->arrow_reader_properties->pre_buffer());
        arrow_properties.set_cache_options(
            parquet_scan_options->arrow_reader_properties->cache_options());
        arrow_properties.set_io_context(
            parquet_scan_options->arrow_reader_properties->io_context());
        arrow_properties.set_use_threads(options->use_threads);
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        RETURN_NOT_OK(parquet::arrow::FileReader::Make(options->pool, std::move(reader),
                                                       std::move(arrow_properties),
                                                       &arrow_reader));
        return std::move(arrow_reader);
      },
      [path](
          const Status& status) -> Result<std::shared_ptr<parquet::arrow::FileReader>> {
        return WrapSourceError(status, path);
      });
}

struct SlicingGenerator {
  SlicingGenerator(RecordBatchGenerator source, int64_t batch_size)
      : state(std::make_shared<State>(source, batch_size)) {}

  Future<std::shared_ptr<RecordBatch>> operator()() {
    if (state->current) {
      return state->SliceOffABatch();
    } else {
      auto state_capture = state;
      return state->source().Then(
          [state_capture](const std::shared_ptr<RecordBatch>& next) {
            if (IsIterationEnd(next)) {
              return next;
            }
            state_capture->current = next;
            return state_capture->SliceOffABatch();
          });
    }
  }

  struct State {
    State(RecordBatchGenerator source, int64_t batch_size)
        : source(std::move(source)), current(), batch_size(batch_size) {}

    std::shared_ptr<RecordBatch> SliceOffABatch() {
      if (current->num_rows() <= batch_size) {
        auto sliced = current;
        current = nullptr;
        return sliced;
      }
      auto slice = current->Slice(0, batch_size);
      current = current->Slice(batch_size);
      return slice;
    }

    RecordBatchGenerator source;
    std::shared_ptr<RecordBatch> current;
    int64_t batch_size;
  };
  std::shared_ptr<State> state;
};

Result<RecordBatchGenerator> ParquetFileFormat::ScanBatchesAsync(
    const std::shared_ptr<ScanOptions>& options,
    const std::shared_ptr<FileFragment>& file) const {
  auto parquet_fragment = checked_pointer_cast<ParquetFileFragment>(file);
  std::vector<int> row_groups;
  bool pre_filtered = false;
  // If RowGroup metadata is cached completely we can pre-filter RowGroups before opening
  // a FileReader, potentially avoiding IO altogether if all RowGroups are excluded due to
  // prior statistics knowledge. In the case where a RowGroup doesn't have statistics
  // metadata, it will not be excluded.
  if (parquet_fragment->metadata() != nullptr) {
    ARROW_ASSIGN_OR_RAISE(row_groups, parquet_fragment->FilterRowGroups(options->filter));
    pre_filtered = true;
    if (row_groups.empty()) return MakeEmptyGenerator<std::shared_ptr<RecordBatch>>();
  }
  // Open the reader and pay the real IO cost.
  auto make_generator =
      [this, options, parquet_fragment, pre_filtered,
       row_groups](const std::shared_ptr<parquet::arrow::FileReader>& reader) mutable
      -> Result<RecordBatchGenerator> {
    // Ensure that parquet_fragment has FileMetaData
    RETURN_NOT_OK(parquet_fragment->EnsureCompleteMetadata(reader.get()));
    if (!pre_filtered) {
      // row groups were not already filtered; do this now
      ARROW_ASSIGN_OR_RAISE(row_groups,
                            parquet_fragment->FilterRowGroups(options->filter));
      if (row_groups.empty()) return MakeEmptyGenerator<std::shared_ptr<RecordBatch>>();
    }
    ARROW_ASSIGN_OR_RAISE(auto column_projection,
                          InferColumnProjection(*reader, *options));
    ARROW_ASSIGN_OR_RAISE(
        auto parquet_scan_options,
        GetFragmentScanOptions<ParquetFragmentScanOptions>(
            kParquetTypeName, options.get(), default_fragment_scan_options));
    int batch_readahead = options->batch_readahead;
    int64_t rows_to_readahead = batch_readahead * options->batch_size;
    ARROW_ASSIGN_OR_RAISE(auto generator,
                          reader->GetRecordBatchGenerator(
                              reader, row_groups, column_projection,
                              ::arrow::internal::GetCpuThreadPool(), rows_to_readahead));
    RecordBatchGenerator sliced =
        SlicingGenerator(std::move(generator), options->batch_size);
    if (batch_readahead == 0) {
      return sliced;
    }
    RecordBatchGenerator sliced_readahead =
        MakeSerialReadaheadGenerator(std::move(sliced), batch_readahead);
    return sliced_readahead;
  };
  auto generator = MakeFromFuture(
      GetReaderAsync(parquet_fragment->source(), options, parquet_fragment->metadata())
          .Then(std::move(make_generator)));
  WRAP_ASYNC_GENERATOR_WITH_CHILD_SPAN(
      generator, "arrow::dataset::ParquetFileFormat::ScanBatchesAsync::Next");
  return generator;
}

Future<std::optional<int64_t>> ParquetFileFormat::CountRows(
    const std::shared_ptr<FileFragment>& file, compute::Expression predicate,
    const std::shared_ptr<ScanOptions>& options) {
  auto parquet_file = checked_pointer_cast<ParquetFileFragment>(file);
  if (parquet_file->metadata()) {
    ARROW_ASSIGN_OR_RAISE(auto maybe_count,
                          parquet_file->TryCountRows(std::move(predicate)));
    return Future<std::optional<int64_t>>::MakeFinished(maybe_count);
  } else {
    return DeferNotOk(options->io_context.executor()->Submit(
        [parquet_file, predicate]() -> Result<std::optional<int64_t>> {
          RETURN_NOT_OK(parquet_file->EnsureCompleteMetadata());
          return parquet_file->TryCountRows(predicate);
        }));
  }
}

Result<std::shared_ptr<ParquetFileFragment>> ParquetFileFormat::MakeFragment(
    FileSource source, compute::Expression partition_expression,
    std::shared_ptr<Schema> physical_schema, std::vector<int> row_groups) {
  return std::shared_ptr<ParquetFileFragment>(new ParquetFileFragment(
      std::move(source), shared_from_this(), std::move(partition_expression),
      std::move(physical_schema), std::move(row_groups)));
}

Result<std::shared_ptr<FileFragment>> ParquetFileFormat::MakeFragment(
    FileSource source, compute::Expression partition_expression,
    std::shared_ptr<Schema> physical_schema) {
  return std::shared_ptr<FileFragment>(new ParquetFileFragment(
      std::move(source), shared_from_this(), std::move(partition_expression),
      std::move(physical_schema), std::nullopt));
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
    std::shared_ptr<FileWriteOptions> options,
    fs::FileLocator destination_locator) const {
  if (!Equals(*options->format())) {
    return Status::TypeError("Mismatching format/write options");
  }

  auto parquet_options = checked_pointer_cast<ParquetFileWriteOptions>(options);

  std::unique_ptr<parquet::arrow::FileWriter> parquet_writer;
  ARROW_ASSIGN_OR_RAISE(parquet_writer, parquet::arrow::FileWriter::Open(
                                            *schema, default_memory_pool(), destination,
                                            parquet_options->writer_properties,
                                            parquet_options->arrow_writer_properties));

  return std::shared_ptr<FileWriter>(
      new ParquetFileWriter(std::move(destination), std::move(parquet_writer),
                            std::move(parquet_options), std::move(destination_locator)));
}

ParquetFileWriter::ParquetFileWriter(std::shared_ptr<io::OutputStream> destination,
                                     std::shared_ptr<parquet::arrow::FileWriter> writer,
                                     std::shared_ptr<ParquetFileWriteOptions> options,
                                     fs::FileLocator destination_locator)
    : FileWriter(writer->schema(), std::move(options), std::move(destination),
                 std::move(destination_locator)),
      parquet_writer_(std::move(writer)) {}

Status ParquetFileWriter::Write(const std::shared_ptr<RecordBatch>& batch) {
  ARROW_ASSIGN_OR_RAISE(auto table, Table::FromRecordBatches(batch->schema(), {batch}));
  return parquet_writer_->WriteTable(*table, batch->num_rows());
}

Future<> ParquetFileWriter::FinishInternal() {
  return DeferNotOk(destination_locator_.filesystem->io_context().executor()->Submit(
      [this]() { return parquet_writer_->Close(); }));
}

//
// ParquetFileFragment
//

ParquetFileFragment::ParquetFileFragment(FileSource source,
                                         std::shared_ptr<FileFormat> format,
                                         compute::Expression partition_expression,
                                         std::shared_ptr<Schema> physical_schema,
                                         std::optional<std::vector<int>> row_groups)
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
    auto scan_options = std::make_shared<ScanOptions>();
    ARROW_ASSIGN_OR_RAISE(auto reader, parquet_format_.GetReader(source_, scan_options));
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
    row_groups_ = Iota(reader->num_row_groups());
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

  statistics_expressions_.resize(row_groups_->size(), compute::literal(true));
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
    compute::Expression predicate) {
  RETURN_NOT_OK(EnsureCompleteMetadata());
  ARROW_ASSIGN_OR_RAISE(auto row_groups, FilterRowGroups(predicate));

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
    compute::Expression predicate) {
  RETURN_NOT_OK(EnsureCompleteMetadata());
  ARROW_ASSIGN_OR_RAISE(auto row_groups, FilterRowGroups(predicate));
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

inline void FoldingAnd(compute::Expression* l, compute::Expression r) {
  if (*l == compute::literal(true)) {
    *l = std::move(r);
  } else {
    *l = and_(std::move(*l), std::move(r));
  }
}

Result<std::vector<int>> ParquetFileFragment::FilterRowGroups(
    compute::Expression predicate) {
  std::vector<int> row_groups;
  ARROW_ASSIGN_OR_RAISE(auto expressions, TestRowGroups(std::move(predicate)));

  auto lock = physical_schema_mutex_.Lock();
  DCHECK(expressions.empty() || (expressions.size() == row_groups_->size()));
  for (size_t i = 0; i < expressions.size(); i++) {
    if (expressions[i].IsSatisfiable()) {
      row_groups.push_back(row_groups_->at(i));
    }
  }
  return row_groups;
}

Result<std::vector<compute::Expression>> ParquetFileFragment::TestRowGroups(
    compute::Expression predicate) {
  auto lock = physical_schema_mutex_.Lock();

  DCHECK_NE(metadata_, nullptr);
  ARROW_ASSIGN_OR_RAISE(
      predicate, SimplifyWithGuarantee(std::move(predicate), partition_expression_));

  if (!predicate.IsSatisfiable()) {
    return std::vector<compute::Expression>{};
  }

  for (const FieldRef& ref : FieldsInExpression(predicate)) {
    ARROW_ASSIGN_OR_RAISE(auto match, ref.FindOneOrNone(*physical_schema_));

    if (match.empty()) continue;
    if (statistics_expressions_complete_[match[0]]) continue;
    statistics_expressions_complete_[match[0]] = true;

    const SchemaField& schema_field = manifest_->schema_fields[match[0]];
    int i = 0;
    for (int row_group : *row_groups_) {
      auto row_group_metadata = metadata_->RowGroup(row_group);

      if (auto minmax =
              ColumnChunkStatisticsAsExpression(schema_field, *row_group_metadata)) {
        FoldingAnd(&statistics_expressions_[i], std::move(*minmax));
        ARROW_ASSIGN_OR_RAISE(statistics_expressions_[i],
                              statistics_expressions_[i].Bind(*physical_schema_));
      }

      ++i;
    }
  }

  std::vector<compute::Expression> row_groups(row_groups_->size());
  for (size_t i = 0; i < row_groups_->size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto row_group_predicate,
                          SimplifyWithGuarantee(predicate, statistics_expressions_[i]));
    row_groups[i] = std::move(row_group_predicate);
  }
  return row_groups;
}

Result<std::optional<int64_t>> ParquetFileFragment::TryCountRows(
    compute::Expression predicate) {
  DCHECK_NE(metadata_, nullptr);
  if (ExpressionHasFieldRefs(predicate)) {
    ARROW_ASSIGN_OR_RAISE(auto expressions, TestRowGroups(std::move(predicate)));
    int64_t rows = 0;
    for (size_t i = 0; i < row_groups_->size(); i++) {
      // If the row group is entirely excluded, exclude it from the row count
      if (!expressions[i].IsSatisfiable()) continue;
      // Unless the row group is entirely included, bail out of fast path
      if (expressions[i] != compute::literal(true)) return std::nullopt;
      BEGIN_PARQUET_CATCH_EXCEPTIONS
      rows += metadata()->RowGroup((*row_groups_)[i])->num_rows();
      END_PARQUET_CATCH_EXCEPTIONS
    }
    return rows;
  }
  return metadata()->num_rows();
}

//
// ParquetFragmentScanOptions
//

ParquetFragmentScanOptions::ParquetFragmentScanOptions() {
  reader_properties = std::make_shared<parquet::ReaderProperties>();
  arrow_reader_properties =
      std::make_shared<parquet::ArrowReaderProperties>(/*use_threads=*/false);
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

  auto scan_options = std::make_shared<ScanOptions>();
  ARROW_ASSIGN_OR_RAISE(auto reader, format->GetReader(metadata_source, scan_options));
  std::shared_ptr<parquet::FileMetaData> metadata = reader->parquet_reader()->metadata();

  if (metadata->num_columns() == 0) {
    return Status::Invalid(
        "ParquetDatasetFactory must contain a schema with at least one column");
  }

  auto properties = MakeArrowReaderProperties(*format, *metadata);
  ARROW_ASSIGN_OR_RAISE(auto physical_schema, GetSchema(*metadata, properties));
  ARROW_ASSIGN_OR_RAISE(auto manifest, GetSchemaManifest(*metadata, properties));

  std::vector<std::pair<std::string, std::vector<int>>> paths_with_row_group_ids;
  std::unordered_map<std::string, int> paths_to_index;

  for (int i = 0; i < metadata->num_row_groups(); i++) {
    auto row_group = metadata->RowGroup(i);
    ARROW_ASSIGN_OR_RAISE(auto path,
                          FileFromRowGroup(filesystem.get(), base_path, *row_group,
                                           options.validate_column_chunk_paths));

    // Insert the path, or increase the count of row groups. It will be assumed that the
    // RowGroup of a file are ordered exactly as in the metadata file.
    auto inserted_index = paths_to_index.emplace(
        std::move(path), static_cast<int>(paths_with_row_group_ids.size()));
    if (inserted_index.second) {
      paths_with_row_group_ids.push_back({inserted_index.first->first, {}});
    }
    paths_with_row_group_ids[inserted_index.first->second].second.push_back(i);
  }

  return std::shared_ptr<DatasetFactory>(new ParquetDatasetFactory(
      std::move(filesystem), std::move(format), std::move(metadata), std::move(manifest),
      std::move(physical_schema), base_path, std::move(options),
      std::move(paths_with_row_group_ids)));
}

Result<std::vector<std::shared_ptr<FileFragment>>>
ParquetDatasetFactory::CollectParquetFragments(const Partitioning& partitioning) {
  std::vector<std::shared_ptr<FileFragment>> fragments(paths_with_row_group_ids_.size());

  size_t i = 0;
  for (const auto& e : paths_with_row_group_ids_) {
    const auto& path = e.first;
    auto metadata_subset = metadata_->Subset(e.second);

    auto row_groups = Iota(metadata_subset->num_row_groups());

    auto partition_expression =
        partitioning.Parse(StripPrefix(path, options_.partition_base_dir))
            .ValueOr(compute::literal(true));

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
    std::vector<std::string> stripped(paths_with_row_group_ids_.size());

    size_t i = 0;
    for (const auto& e : paths_with_row_group_ids_) {
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
  return FileSystemDataset::Make(std::move(schema), compute::literal(true), format_,
                                 filesystem_, std::move(fragments),
                                 std::move(partitioning));
}

}  // namespace dataset
}  // namespace arrow
