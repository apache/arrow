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

#include <algorithm>
#include <limits>
#include <memory>
#include <mutex>
#include <stack>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/acero/exec_plan.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/expression.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/io/caching.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/type_fwd.h"
#include "arrow/memory_pool.h"
#include "arrow/table.h"
#include "arrow/type_fwd.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/async_generator_fwd.h"
#include "arrow/util/bit_util.h"
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

  compute::Expression field_expr = compute::field_ref(field->name());

  return ParquetFileFragment::EvaluateStatisticsAsExpression(
      field_expr, schema_field.field->type(), *statistics);
}

inline void FoldingAnd(compute::Expression* l, compute::Expression r) {
  if (*l == compute::literal(true)) {
    *l = std::move(r);
  } else {
    *l = and_(std::move(*l), std::move(r));
  }
}

Result<std::vector<std::string>> ColumnNamesFromManifest(const SchemaManifest& manifest) {
  std::vector<std::string> names;
  names.reserve(manifest.schema_fields.size());
  for (const auto& schema_field : manifest.schema_fields) {
    names.push_back(schema_field.field->name());
  }
  return names;
}

Result<const SchemaField*> ResolvePath(const FieldPath& path,
                                       const SchemaManifest& manifest) {
  const SchemaField* itr;
  DCHECK_LT(path[0], static_cast<int32_t>(manifest.schema_fields.size()));
  itr = &manifest.schema_fields[path[0]];
  for (std::size_t i = 1; i < path.indices().size(); i++) {
    // This should be guaranteed by evolution but maybe we should be more flexible
    // to account for bugs in the evolution strategy?
    DCHECK(!itr->is_leaf());
    DCHECK_LT(path[i], static_cast<int32_t>(itr->children.size()));
    itr = &itr->children[path[i]];
  }
  return itr;
}

// Opening a file reader async is a little bit of a nuisance since future's don't
// currently support moving the result and the file reader expects a unique_ptr to a
// parquet reader.  This helper works around this problem with a bit of a hack.
Future<std::shared_ptr<parquet::arrow::FileReader>> OpenFileReaderAsync(
    std::shared_ptr<io::RandomAccessFile> file,
    const parquet::ReaderProperties& properties,
    std::function<Result<parquet::ArrowReaderProperties>(const parquet::FileMetaData&)>
        arrow_properties_factory,
    MemoryPool* memory_pool,
    std::shared_ptr<parquet::FileMetaData> file_metadata = nullptr) {
  auto reader_fut =
      parquet::ParquetFileReader::OpenAsync(std::move(file), properties, file_metadata);
  return reader_fut.Then([reader_fut, arrow_properties_factory, memory_pool](
                             const std::unique_ptr<parquet::ParquetFileReader>&) mutable
                         -> Result<std::shared_ptr<parquet::arrow::FileReader>> {
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<parquet::ParquetFileReader> reader,
                          reader_fut.MoveResult());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ARROW_ASSIGN_OR_RAISE(parquet::ArrowReaderProperties arrow_properties,
                          arrow_properties_factory(*reader->metadata()));
    RETURN_NOT_OK(parquet::arrow::FileReader::Make(
        memory_pool, std::move(reader), std::move(arrow_properties), &arrow_reader));
    return arrow_reader;
  });
}

class ParquetInspectedFragment : public InspectedFragment {
 public:
  explicit ParquetInspectedFragment(
      std::vector<int> row_groups, std::shared_ptr<parquet::FileMetaData> file_metadata,
      std::shared_ptr<parquet::arrow::SchemaManifest> manifest,
      std::shared_ptr<Schema> schema, std::vector<std::string> column_names)
      : InspectedFragment(std::move(column_names)),
        row_groups_(std::move(row_groups)),
        file_metadata_(std::move(file_metadata)),
        manifest_(std::move(manifest)),
        schema_(std::move(schema)),
        statistics_expressions_(row_groups_.size(), compute::literal(true)) {
    // "empty" means "all row groups"
    if (row_groups_.empty()) {
      row_groups_.resize(file_metadata_->num_row_groups());
      std::iota(row_groups_.begin(), row_groups_.end(), 0);
      statistics_expressions_.resize(row_groups_.size(), compute::literal(true));
    }
    // Note, this is only a lower bound, it could be higher if there were lots of nested
    // refs but that is pretty unlikely
    statistics_expressions_complete_.reserve(schema_->num_fields());
  }

  Result<std::vector<compute::Expression>> TestRowGroups(compute::Expression predicate) {
    if (!predicate.IsBound()) {
      ARROW_ASSIGN_OR_RAISE(predicate, predicate.Bind(*schema_));
    }

    std::lock_guard lg(mutex_);

    if (!predicate.IsSatisfiable()) {
      return std::vector<compute::Expression>{};
    }

    for (const FieldRef& ref : FieldsInExpression(predicate)) {
      ARROW_ASSIGN_OR_RAISE(auto match, ref.FindOneOrNone(*schema_));

      if (match.empty()) continue;

      ARROW_ASSIGN_OR_RAISE(const SchemaField* schema_field,
                            ResolvePath(match, *manifest_));
      if (!schema_field->is_leaf()) {
        // Statistics are only kept for leaves
        continue;
      }

      if (statistics_expressions_complete_.find(match) !=
          statistics_expressions_complete_.end()) {
        // We've already accounted for this field's statistics
        continue;
      }
      statistics_expressions_complete_.insert(match);

      for (int row_group_idx = 0; row_group_idx < static_cast<int>(row_groups_.size());
           row_group_idx++) {
        int row_group = row_groups_[row_group_idx];
        auto row_group_metadata = file_metadata_->RowGroup(row_group);
        std::shared_ptr<parquet::Statistics> col_stats =
            row_group_metadata->ColumnChunk(schema_field->column_index)->statistics();
        if (col_stats == nullptr) {
          continue;
        }

        compute::Expression match_ref = compute::field_ref(match);
        if (auto minmax = ParquetFileFragment::EvaluateStatisticsAsExpression(
                match_ref, schema_field->field->type(), *col_stats)) {
          FoldingAnd(&statistics_expressions_[row_group_idx], std::move(*minmax));
          ARROW_ASSIGN_OR_RAISE(statistics_expressions_[row_group_idx],
                                statistics_expressions_[row_group_idx].Bind(*schema_));
        }
      }
    }

    std::vector<compute::Expression> row_groups(row_groups_.size());
    for (size_t i = 0; i < row_groups_.size(); ++i) {
      ARROW_ASSIGN_OR_RAISE(auto row_group_predicate,
                            SimplifyWithGuarantee(predicate, statistics_expressions_[i]));
      row_groups[i] = std::move(row_group_predicate);
    }
    return row_groups;
  }

  Result<std::vector<int>> FilterRowGroups(compute::Expression predicate) {
    std::vector<int> row_groups;
    ARROW_ASSIGN_OR_RAISE(auto expressions, TestRowGroups(std::move(predicate)));

    std::lock_guard lg(mutex_);
    DCHECK(expressions.empty() || (expressions.size() == row_groups_.size()));
    for (size_t i = 0; i < expressions.size(); i++) {
      if (expressions[i].IsSatisfiable()) {
        row_groups.push_back(row_groups_.at(i));
      }
    }
    return row_groups;
  }

  const std::shared_ptr<parquet::FileMetaData>& file_metadata() const {
    return file_metadata_;
  }

 private:
  // The row groups that are included in this fragment
  std::vector<int> row_groups_;
  // The parquet file metadata
  std::shared_ptr<parquet::FileMetaData> file_metadata_;
  // A helper utility that bridges parquet file metadata and the arrow schema
  std::shared_ptr<parquet::arrow::SchemaManifest> manifest_;
  // The arrow schema of the parquet file (fragment schema)
  std::shared_ptr<Schema> schema_;
  // Cached expressions computed from the row group statistics.  One per row group.
  std::vector<compute::Expression> statistics_expressions_;
  // A cached flag that tells us whether we have calculated statistics for a given column
  // or not.
  std::unordered_set<FieldPath, FieldPath::Hash> statistics_expressions_complete_;
  // Guards concurrent access to statistics_expressions_ and
  // statistics_expressions_complete_ which, being cached and lazily computed, may be
  // updated during calls to Test/FilterRowGroups.
  std::mutex mutex_;
};

parquet::ReaderProperties MakeReaderProperties(
    const ParquetFragmentScanOptions* parquet_scan_options,
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
    const ParquetFileFormat::ReaderOptions& reader_options,
    const parquet::FileMetaData& metadata) {
  parquet::ArrowReaderProperties properties(/* use_threads = */ false);
  for (const std::string& name : reader_options.dict_columns) {
    auto column_index = metadata.schema()->ColumnIndex(name);
    properties.set_read_dictionary(column_index, true);
  }
  properties.set_coerce_int96_timestamp_unit(reader_options.coerce_int96_timestamp_unit);
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

// Converts a field ref into a position-independent ref (containing only a sequence of
// names) based on the dataset schema. Returns `false` if no conversion was needed.
Result<FieldRef> MaybeConvertFieldRef(FieldRef ref, const Schema& dataset_schema) {
  if (ARROW_PREDICT_TRUE(ref.IsNameSequence())) {
    return std::move(ref);
  }

  ARROW_ASSIGN_OR_RAISE(auto path, ref.FindOne(dataset_schema));
  std::vector<FieldRef> named_refs;
  named_refs.reserve(path.indices().size());

  const FieldVector* child_fields = &dataset_schema.fields();
  for (auto index : path) {
    const auto& child_field = *(*child_fields)[index];
    named_refs.emplace_back(child_field.name());
    child_fields = &child_field.type()->fields();
  }

  return named_refs.size() == 1 ? std::move(named_refs[0])
                                : FieldRef(std::move(named_refs));
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
  for (auto& ref : field_refs) {
    // In the (unlikely) absence of a known dataset schema, we require that all
    // materialized refs are named.
    if (options.dataset_schema) {
      ARROW_ASSIGN_OR_RAISE(
          ref, MaybeConvertFieldRef(std::move(ref), *options.dataset_schema));
    }
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
        std::move(input), MakeReaderProperties(parquet_scan_options.get()));
    std::shared_ptr<parquet::FileMetaData> metadata = reader->metadata();
    return metadata != nullptr && metadata->can_decompress();
  } catch (const ::parquet::ParquetInvalidOrCorruptedFileException& e) {
    ARROW_UNUSED(e);
    return false;
  }
  END_PARQUET_CATCH_EXCEPTIONS
}

class ParquetFragmentScanner : public FragmentScanner {
 public:
  explicit ParquetFragmentScanner(
      std::string path, std::shared_ptr<parquet::FileMetaData> file_metadata,
      std::vector<int> desired_row_groups, const ParquetFragmentScanOptions* scan_options,
      const ParquetFileFormat::ReaderOptions* format_reader_options,
      compute::ExecContext* exec_context)
      : path_(std::move(path)),
        file_metadata_(std::move(file_metadata)),
        desired_row_groups_(std::move(desired_row_groups)),
        scan_options_(scan_options),
        format_reader_options_(format_reader_options),
        exec_context_(exec_context) {}

  AsyncGenerator<std::shared_ptr<RecordBatch>> RunScanTask(int task_number) override {
    int row_group_number = desired_row_groups_[task_number];
    AsyncGenerator<std::shared_ptr<RecordBatch>> row_group_batches =
        file_reader_->ReadRowGroupAsync(row_group_number, desired_columns_,
                                        exec_context_->executor());
    return MakeMappedGenerator(
        row_group_batches,
        [this](const std::shared_ptr<RecordBatch>& batch) { return Reshape(batch); });
  }

  Result<std::shared_ptr<RecordBatch>> Reshape(
      const std::shared_ptr<RecordBatch>& batch) {
    std::vector<std::shared_ptr<Array>> arrays;
    std::vector<std::shared_ptr<Field>> fields;
    for (const auto& path : selection_paths_) {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> path_arr, path.Get(*batch));
      fields.push_back(field("", path_arr->type()));
      arrays.push_back(std::move(path_arr));
    }
    return RecordBatch::Make(schema(std::move(fields)), batch->num_rows(),
                             std::move(arrays));
  }

  int NumScanTasks() override { return static_cast<int>(desired_row_groups_.size()); }

  int NumBatchesInScanTask(int task_number) override {
    int row_group_number = desired_row_groups_[task_number];
    if (scan_options_->allow_jumbo_values) {
      return FragmentScanner::kUnknownNumberOfBatches;
    }
    int64_t num_rows = file_metadata_->RowGroup(row_group_number)->num_rows();
    int64_t num_batches = bit_util::CeilDiv(num_rows, static_cast<int64_t>(batch_size_));
    return static_cast<int>(num_batches);
  }

  // These are the parquet-specific reader properties.  Some of these properties are set
  // from the plan (e.g. memory pool) and some of these properties are controlled by the
  // user via ParquetFragmentScanOptions
  parquet::ReaderProperties MakeParquetReaderProperties() {
    parquet::ReaderProperties reader_properties;
    parquet::ReaderProperties properties(exec_context_->memory_pool());
    switch (scan_options_->scan_strategy) {
      case ParquetScanStrategy::kLeastMemory:
        // This makes it so we don't need to load the entire row group into memory
        // but introduces a memcpy of the I/O data, In theory we should be able to prevent
        // this memcpy if it is becoming a bottleneck
        properties.enable_buffered_stream();
        // 8MiB for reads tends to be a pretty safe balance between too many small reads
        // and too few large reads.
        properties.set_buffer_size(8 * 1024 * 1024);
        break;
      case ParquetScanStrategy::kMaxSpeed:
        // I'm not actually convinced this does lead to better performance.  More
        // experiments are needed.  However, the way this is documented, it appears that
        // should be the case.
        properties.disable_buffered_stream();
        break;
      case ParquetScanStrategy::kCustom:
        // Use what the user provided if custom
        if (scan_options_->reader_properties->is_buffered_stream_enabled()) {
          properties.enable_buffered_stream();
        } else {
          properties.disable_buffered_stream();
        }
        properties.set_buffer_size(scan_options_->reader_properties->buffer_size());
        break;
    }
    // These properties are unrelated to a RAM / CPU tradeoff and we always use what the
    // user provided.
    properties.file_decryption_properties(
        scan_options_->reader_properties->file_decryption_properties());
    properties.set_thrift_string_size_limit(
        scan_options_->reader_properties->thrift_string_size_limit());
    properties.set_thrift_container_size_limit(
        scan_options_->reader_properties->thrift_container_size_limit());
    return properties;
  }

  // These are properties that control how we convert from the Arrow-unaware low level
  // parquet reader to the arrow aware reader.  Similar to the reader properties these
  // come both from the plan itself as well as from user options
  //
  // In addition, and regrettably, some options come from the format object itself.
  // TODO(GH-35211): Simplify this so all options come from ParquetFragmentScanOptions
  parquet::ArrowReaderProperties MakeParquetArrowReaderProperties() {
    parquet::ArrowReaderProperties properties;

    // These properties are controlled by the plan
    properties.set_io_context(io::default_io_context());

    // These properties are controlled by the user, but simplified
    switch (scan_options_->scan_strategy) {
      // There's not much point in reading large batches that will just get sliced up by
      // the source node anyways.
      case ParquetScanStrategy::kLeastMemory:
        properties.set_batch_size(acero::ExecPlan::kMaxBatchSize);
        // Pre-buffering requires reading the entire row-group all at once
        properties.set_pre_buffer(false);
        // This does not actually mean we will use threads.  The reader's async methods
        // take a CPU executor.  If that is a serial executor then columns will be
        // processed serially.
        properties.set_use_threads(true);
        break;
      case ParquetScanStrategy::kMaxSpeed:
        // This batch_size emulates some historical behavior and is probably redundant.
        // Since we've disabled stream buffering we are reading entire row groups into
        // memory so there isn't all that much reason to have any kind of batch size
        properties.set_batch_size(64 * 1024 * 1024);
        properties.set_pre_buffer(true);
        properties.set_cache_options(io::CacheOptions::LazyDefaults());
        // see comment above about threads
        properties.set_use_threads(true);
        break;
      case ParquetScanStrategy::kCustom:
        properties.set_batch_size(scan_options_->arrow_reader_properties->batch_size());
        properties.set_pre_buffer(scan_options_->arrow_reader_properties->pre_buffer());
        properties.set_cache_options(
            scan_options_->arrow_reader_properties->cache_options());
        properties.set_use_threads(scan_options_->arrow_reader_properties->use_threads());
        break;
    }

    // These options are unrelated to the CPU/RAM tradeoff and we always take from
    // the user
    for (const std::string& name : format_reader_options_->dict_columns) {
      auto column_index = file_metadata_->schema()->ColumnIndex(name);
      properties.set_read_dictionary(column_index, true);
    }
    properties.set_coerce_int96_timestamp_unit(
        format_reader_options_->coerce_int96_timestamp_unit);
    return properties;
  }

  Status CheckRowGroupSizes(int64_t batch_size) {
    // This seems extremely unlikely (by default a row group would need 64Bi rows) but
    // better safe than sorry since these properties are both int64_t and we would get an
    // overflow since we assume int32_t in NumBatchesInScanTask
    for (int row_group = 0; row_group < file_metadata_->num_row_groups(); row_group++) {
      int64_t num_rows = file_metadata_->RowGroup(row_group)->num_rows();
      if (bit_util::CeilDiv(num_rows, batch_size) > std::numeric_limits<int32_t>::max()) {
        return Status::NotImplemented("A single row group with more than 2^31 batches");
      }
    }
    return Status::OK();
  }

  void AddColumnIndices(const SchemaField& field, std::vector<int32_t>* indices) {
    if (field.is_leaf()) {
      indices->push_back(field.column_index);
    }
    for (const auto& child : field.children) {
      AddColumnIndices(child, indices);
    }
  }

  using SchemaFieldToPathMap = std::unordered_multimap<const SchemaField*, FieldPath*>;
  // This struct is a recursive helper.  See the comment on CalculateProjection for
  // details.  In here we walk through the file schema and calculate the expected output
  // schema for a given set of column indices
  struct PartialSchemaResolver {
    bool Resolve(const std::vector<int32_t>& column_indices,
                 SchemaFieldToPathMap* node_to_paths, const SchemaField& field) {
      bool field_is_included = false;
      if (field.is_leaf()) {
        if (field.column_index == next_included_index) {
          field_is_included = true;
          next_included_index_index++;
          if (next_included_index_index < column_indices.size()) {
            next_included_index = column_indices[next_included_index_index];
          } else {
            next_included_index = -1;
          }
        }
      }
      int num_included_children = 0;
      for (const auto& child : field.children) {
        current_path.push_back(num_included_children);
        if (Resolve(column_indices, node_to_paths, child)) {
          num_included_children++;
          field_is_included = true;
        }
        current_path.pop_back();
      }
      auto found_paths = node_to_paths->equal_range(&field);
      for (auto& item = found_paths.first; item != found_paths.second; item++) {
        *item->second = FieldPath(current_path);
      }
      return field_is_included;
    }

    Status Resolve(const std::vector<int32_t>& column_indices,
                   SchemaFieldToPathMap* node_to_paths, const SchemaManifest& manifest) {
      next_included_index_index = 0;
      next_included_index = column_indices[next_included_index_index];
      int num_included_fields = 0;
      for (const auto& schema_field : manifest.schema_fields) {
        current_path.push_back(num_included_fields);
        const SchemaField* field = &schema_field;
        if (Resolve(column_indices, node_to_paths, *field)) {
          num_included_fields++;
        }
        current_path.pop_back();
      }
      return Status::OK();
    }

    std::size_t next_included_index_index = 0;
    int next_included_index = -1;
    std::vector<int32_t> current_path;
  };

  // We are given `selection`, which is a vector of paths into the file schema.
  //
  // The parquet reader needs a vector of leaf indices.  For non-nested fields this
  // is easy.  For nested fields it is a bit more complex.  Consider the schema:
  //
  // A   B   C
  //     |
  //  E - - D
  //        |
  //     F - - G
  //
  // A and C are top-level flat fields.  B and D are nested structs.  The leaf indices
  // are A(0), E(1), F(2), G(3), and C(4).
  //
  // If the user asks for field F we must include 2 in the indices we send to the file
  // reader. A more challenging task is that we must also figure out the path to F in
  // the batches the file reader will be returning.
  //
  // For example, if F is the only selection then {2} will be the only index we send to
  // the file reader and returned batches will have a schema that includes B, D, and F.
  // The path to F in those batches will be 0,0,0.  If the user asks for A, E, and F then
  // the returned schema will include A, B, E, D, and F.  The path to F in those batches
  // will be 1,1,0.
  //
  // If the user asks for a non-leaf field then we must make sure to add all leaf indices
  // under that field.  In the above example, if the user asks for D then we must add both
  // F(2) and G(3) to the column indices we send to the parquet reader.
  Status CalculateProjection(const parquet::FileMetaData& metadata,
                             const FragmentSelection& selection,
                             const SchemaManifest& manifest) {
    if (selection.columns().empty()) {
      return Status::OK();
    }
    selection_paths_.resize(selection.columns().size());
    // A map from each node to the paths we need to fill in for that node
    // (these will be items in selection_paths_)
    std::unordered_multimap<const SchemaField*, FieldPath*> node_to_paths;
    for (std::size_t column_idx = 0; column_idx < selection.columns().size();
         column_idx++) {
      const FragmentSelectionColumn& selected_column = selection.columns()[column_idx];
      ARROW_ASSIGN_OR_RAISE(const SchemaField* selected_field,
                            ResolvePath(selected_column.path, manifest));
      AddColumnIndices(*selected_field, &desired_columns_);
      node_to_paths.insert({selected_field, &selection_paths_[column_idx]});
    }

    // Sort the leaf indices and remove duplicates
    std::sort(desired_columns_.begin(), desired_columns_.end());
    desired_columns_.erase(std::unique(desired_columns_.begin(), desired_columns_.end()),
                           desired_columns_.end());

    // Now we calculate the expected output schema and use that to calculate the expected
    // output paths.  A node is included in this schema if there is at least one leaf
    // under that node.
    return PartialSchemaResolver().Resolve(desired_columns_, &node_to_paths, manifest);
    // As we return we can expect that both desired_columns_ and selection_paths_
    // are properly initialized
  }

  Future<> Initialize(std::shared_ptr<io::RandomAccessFile> file,
                      const FragmentScanRequest& request) {
    parquet::ReaderProperties properties = MakeParquetReaderProperties();
    auto arrow_properties_factory =
        [this](const parquet::FileMetaData&) -> Result<parquet::ArrowReaderProperties> {
      parquet::ArrowReaderProperties arrow_properties =
          MakeParquetArrowReaderProperties();
      ARROW_RETURN_NOT_OK(CheckRowGroupSizes(arrow_properties.batch_size()));
      batch_size_ = static_cast<int32_t>(arrow_properties.batch_size());
      if (batch_size_ > std::numeric_limits<int32_t>::max()) {
        return Status::NotImplemented("Scanner batch size > int32_t max");
      }
      return arrow_properties;
    };
    Future<std::shared_ptr<parquet::arrow::FileReader>> file_reader_fut =
        OpenFileReaderAsync(std::move(file), properties, arrow_properties_factory,
                            exec_context_->memory_pool(), file_metadata_);
    return file_reader_fut.Then(
        [this, request](
            const std::shared_ptr<parquet::arrow::FileReader>& arrow_reader) -> Status {
          RETURN_NOT_OK(CalculateProjection(*file_metadata_, *request.fragment_selection,
                                            arrow_reader->manifest()));
          file_reader_ = arrow_reader;
          return Status::OK();
        },
        [this](const Status& status) -> Status {
          return WrapSourceError(status, path_);
        });
  }

  static Future<std::shared_ptr<FragmentScanner>> Make(
      std::shared_ptr<io::RandomAccessFile> file, std::string path,
      const ParquetFragmentScanOptions* scan_options,
      const ParquetFileFormat::ReaderOptions* format_reader_options,
      const FragmentScanRequest& request, ParquetInspectedFragment* inspection,
      compute::ExecContext* exec_context) {
    ARROW_ASSIGN_OR_RAISE(std::vector<int> desired_row_groups,
                          inspection->FilterRowGroups(request.filter));
    // Construct a fragment scanner, initialize it, and return it
    std::shared_ptr<ParquetFragmentScanner> parquet_fragment_scanner =
        std::make_shared<ParquetFragmentScanner>(
            std::move(path), inspection->file_metadata(), std::move(desired_row_groups),
            scan_options, format_reader_options, exec_context);
    return parquet_fragment_scanner->Initialize(std::move(file), request)
        .Then([fragment_scanner = std::static_pointer_cast<FragmentScanner>(
                   parquet_fragment_scanner)]() { return fragment_scanner; });
  }

 private:
  // These properties are set during construction
  std::string path_;
  std::shared_ptr<parquet::FileMetaData> file_metadata_;
  std::vector<int> desired_row_groups_;
  const ParquetFragmentScanOptions* scan_options_;
  const ParquetFileFormat::ReaderOptions* format_reader_options_;
  compute::ExecContext* exec_context_;

  // These are set during Initialize
  std::vector<int> desired_columns_;
  std::vector<FieldPath> selection_paths_;
  std::shared_ptr<parquet::arrow::FileReader> file_reader_;
  int32_t batch_size_;
};

}  // namespace

std::optional<compute::Expression> ParquetFileFragment::EvaluateStatisticsAsExpression(
    const compute::Expression& field_expr, const std::shared_ptr<DataType>& field_type,
    const parquet::Statistics& statistics) {
  // Optimize for corner case where all values are nulls
  if (statistics.num_values() == 0 && statistics.null_count() > 0) {
    return is_null(std::move(field_expr));
  }

  std::shared_ptr<Scalar> min, max;
  if (!StatisticsAsScalars(statistics, &min, &max).ok()) {
    return std::nullopt;
  }

  auto maybe_min = min->CastTo(field_type);
  auto maybe_max = max->CastTo(field_type);

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

Future<std::shared_ptr<InspectedFragment>> ParquetFileFormat::InspectFragment(
    const FileFragment& fragment, const FileSource& source,
    const FragmentScanOptions* format_options, compute::ExecContext* exec_context) const {
  const auto& parquet_fragment = checked_cast<const ParquetFileFragment&>(fragment);
  ARROW_ASSIGN_OR_RAISE(const ParquetFragmentScanOptions* parquet_scan_options,
                        GetFragmentScanOptions<ParquetFragmentScanOptions>(
                            format_options, kParquetTypeName));
  auto properties =
      MakeReaderProperties(parquet_scan_options, exec_context->memory_pool());
  auto arrow_properties_factory =
      [reader_opts = reader_options](const parquet::FileMetaData& file_metadata) {
        return MakeArrowReaderProperties(reader_opts, file_metadata);
      };
  ARROW_ASSIGN_OR_RAISE(auto input, source.Open());
  return OpenFileReaderAsync(input, properties, arrow_properties_factory,
                             exec_context->memory_pool())
      .Then([row_groups = parquet_fragment.row_groups()](
                const std::shared_ptr<parquet::arrow::FileReader>& file_reader) mutable
            -> Result<std::shared_ptr<InspectedFragment>> {
        ARROW_ASSIGN_OR_RAISE(std::vector<std::string> column_names,
                              ColumnNamesFromManifest(file_reader->manifest()));
        std::shared_ptr<Schema> file_schema;
        ARROW_RETURN_NOT_OK(file_reader->GetSchema(&file_schema));
        auto manifest_copy =
            std::make_shared<parquet::arrow::SchemaManifest>(file_reader->manifest());
        return std::make_shared<ParquetInspectedFragment>(
            std::move(row_groups), file_reader->parquet_reader()->metadata(),
            std::move(manifest_copy), file_schema, std::move(column_names));
      });
}

Future<std::shared_ptr<FragmentScanner>> ParquetFileFormat::BeginScan(
    const FileSource& source, const FragmentScanRequest& request,
    InspectedFragment* inspected_fragment, const FragmentScanOptions* format_options,
    compute::ExecContext* exec_context) const {
  ARROW_ASSIGN_OR_RAISE(const ParquetFragmentScanOptions* parquet_scan_options,
                        GetFragmentScanOptions<ParquetFragmentScanOptions>(
                            format_options, kParquetTypeName));
  auto* inspection = checked_cast<ParquetInspectedFragment*>(inspected_fragment);
  ARROW_ASSIGN_OR_RAISE(auto input, source.Open());
  return ParquetFragmentScanner::Make(std::move(input), source.path(),
                                      parquet_scan_options, &reader_options, request,
                                      inspection, exec_context);
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
  auto properties = MakeReaderProperties(parquet_scan_options.get(), options->pool);
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
        auto arrow_properties =
            MakeArrowReaderProperties(self->reader_options, *metadata);
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

  auto properties = MakeArrowReaderProperties(format->reader_options, *metadata);
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
