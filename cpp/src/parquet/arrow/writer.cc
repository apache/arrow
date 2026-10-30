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

#include "parquet/arrow/writer.h"

#include <algorithm>
#include <cmath>
#include <deque>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/concatenate.h"
#include "arrow/extension_type.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/base64.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/parallel.h"

#include "parquet/arrow/path_internal.h"
#include "parquet/arrow/reader_internal.h"
#include "parquet/arrow/schema.h"
#include "parquet/column_writer.h"
#include "parquet/exception.h"
#include "parquet/file_writer.h"
#include "parquet/platform.h"
#include "parquet/schema.h"

using arrow::Array;
using arrow::ArrayData;
using arrow::BinaryArray;
using arrow::BooleanArray;
using arrow::ChunkedArray;
using arrow::DataType;
using arrow::DictionaryArray;
using arrow::ExtensionArray;
using arrow::ExtensionType;
using arrow::Field;
using arrow::FieldVector;
using arrow::FixedSizeBinaryArray;
using arrow::ListArray;
using arrow::MemoryPool;
using arrow::NumericArray;
using arrow::PrimitiveArray;
using arrow::RecordBatch;
using arrow::ResizableBuffer;
using arrow::Result;
using arrow::Status;
using arrow::Table;
using arrow::TimeUnit;

using arrow::internal::checked_cast;

using parquet::ParquetFileWriter;
using parquet::ParquetVersion;
using parquet::schema::GroupNode;

namespace parquet::arrow {

namespace {

int CalculateLeafCount(const DataType* type) {
  if (type->id() == ::arrow::Type::EXTENSION) {
    type = checked_cast<const ExtensionType&>(*type).storage_type().get();
  }
  // Note num_fields() can be 0 for an empty struct type
  if (!::arrow::is_nested(type->id())) {
    // Primitive type.
    return 1;
  }

  int num_leaves = 0;
  for (const auto& field : type->fields()) {
    num_leaves += CalculateLeafCount(field->type().get());
  }
  return num_leaves;
}

// Determines if the |schema_field|'s root ancestor is nullable.
bool HasNullableRoot(const SchemaManifest& schema_manifest,
                     const SchemaField* schema_field) {
  DCHECK(schema_field != nullptr);
  const SchemaField* current_field = schema_field;
  bool nullable = schema_field->field->nullable();
  while (current_field != nullptr) {
    nullable = current_field->field->nullable();
    current_field = schema_manifest.GetParent(current_field);
  }
  return nullable;
}

// Returns the root ancestor of |schema_field|.
const SchemaField* GetRootField(const SchemaManifest& schema_manifest,
                                const SchemaField* schema_field) {
  while (const SchemaField* parent = schema_manifest.GetParent(schema_field)) {
    schema_field = parent;
  }
  return schema_field;
}

// Returns a nested type of the same kind as |type| with the given child |fields|.
Result<std::shared_ptr<DataType>> WithChildFields(const DataType& type,
                                                  FieldVector fields) {
  switch (type.id()) {
    case ::arrow::Type::STRUCT:
      return ::arrow::struct_(std::move(fields));
    case ::arrow::Type::LIST:
      return ::arrow::list(std::move(fields[0]));
    case ::arrow::Type::LARGE_LIST:
      return ::arrow::large_list(std::move(fields[0]));
    case ::arrow::Type::LIST_VIEW:
      return ::arrow::list_view(std::move(fields[0]));
    case ::arrow::Type::LARGE_LIST_VIEW:
      return ::arrow::large_list_view(std::move(fields[0]));
    case ::arrow::Type::FIXED_SIZE_LIST:
      return ::arrow::fixed_size_list(
          std::move(fields[0]),
          checked_cast<const ::arrow::FixedSizeListType&>(type).list_size());
    case ::arrow::Type::MAP:
      return std::make_shared<::arrow::MapType>(
          std::move(fields[0]),
          checked_cast<const ::arrow::MapType&>(type).keys_sorted());
    default:
      return Status::NotImplemented("Cannot rebuild nested type ", type.ToString());
  }
}

// The element of a FINITE_VECTOR-annotated group is a required Parquet field, but the
// corresponding Arrow FixedSizeList element field may be nullable. Mark such
// element fields non-nullable so that the computed definition levels match the
// Parquet schema. The absence of nulls is checked when the leaf values are
// written (see CheckFiniteVectorElements).
//
// |schema_field| comes from the SchemaManifest built from the Parquet schema,
// where a FixedSizeList is only inferred from a FINITE_VECTOR-annotated group.
Result<std::shared_ptr<ArrayData>> MakeFiniteVectorElementsRequired(
    const std::shared_ptr<ArrayData>& data, const SchemaField& schema_field) {
  std::shared_ptr<DataType> type = data->type;
  // Extension types may be nested; schema conversion unwraps them all.
  while (type->id() == ::arrow::Type::EXTENSION) {
    type = checked_cast<const ExtensionType&>(*type).storage_type();
  }
  const int num_fields = type->num_fields();
  if (!::arrow::is_nested(type->id()) ||
      static_cast<size_t>(num_fields) != schema_field.children.size() ||
      static_cast<size_t>(num_fields) != data->child_data.size()) {
    return data;
  }

  bool modified = false;
  std::vector<std::shared_ptr<ArrayData>> children(num_fields);
  for (int i = 0; i < num_fields; ++i) {
    ARROW_ASSIGN_OR_RAISE(
        children[i],
        MakeFiniteVectorElementsRequired(data->child_data[i], schema_field.children[i]));
    modified |= children[i] != data->child_data[i];
  }
  const bool is_vector =
      type->id() == ::arrow::Type::FIXED_SIZE_LIST &&
      schema_field.field->type()->id() == ::arrow::Type::FIXED_SIZE_LIST &&
      type->field(0)->nullable();
  if (!modified && !is_vector) {
    return data;
  }

  FieldVector fields = type->fields();
  for (int i = 0; i < num_fields; ++i) {
    fields[i] = fields[i]->WithType(children[i]->type);
  }
  if (is_vector) {
    fields[0] = fields[0]->WithNullable(false);
  }
  // Extension types are dropped; only the storage is needed for writing.
  auto out = data->Copy();
  ARROW_ASSIGN_OR_RAISE(out->type, WithChildFields(*type, std::move(fields)));
  out->child_data = std::move(children);
  return out;
}

// Checks that the values written for the element of a FINITE_VECTOR-annotated group
// are non-null and finite.
Status CheckFiniteVectorElements(const Array& values, const ColumnDescriptor& descr) {
  const Array* storage = &values;
  while (storage->type_id() == ::arrow::Type::EXTENSION) {
    storage = checked_cast<const ExtensionArray&>(*storage).storage().get();
  }
  if (storage->type_id() == ::arrow::Type::DICTIONARY) {
    // Excluded when the schema is derived (see IsFiniteVectorEligibleValueType); the
    // checks below need decoded values.
    return Status::Invalid("FINITE_VECTOR column '", descr.path()->ToDotString(),
                           "' cannot be written from dictionary-encoded values");
  }
  if (storage->null_count() != 0) {
    return Status::Invalid("FINITE_VECTOR column '", descr.path()->ToDotString(),
                           "' contains a null element");
  }
  bool finite = true;
  switch (storage->type_id()) {
    case ::arrow::Type::HALF_FLOAT: {
      const auto& array = checked_cast<const ::arrow::HalfFloatArray&>(*storage);
      // All exponent bits set means infinity or NaN
      finite = std::none_of(array.raw_values(), array.raw_values() + array.length(),
                            [](uint16_t v) { return (v & 0x7c00) == 0x7c00; });
    } break;
    case ::arrow::Type::FLOAT: {
      const auto& array = checked_cast<const ::arrow::FloatArray&>(*storage);
      finite = std::all_of(array.raw_values(), array.raw_values() + array.length(),
                           [](float v) { return std::isfinite(v); });
    } break;
    case ::arrow::Type::DOUBLE: {
      const auto& array = checked_cast<const ::arrow::DoubleArray&>(*storage);
      finite = std::all_of(array.raw_values(), array.raw_values() + array.length(),
                           [](double v) { return std::isfinite(v); });
    } break;
    default:
      break;
  }
  if (!finite) {
    return Status::Invalid("FINITE_VECTOR column '", descr.path()->ToDotString(),
                           "' contains a non-finite element");
  }
  return Status::OK();
}

// Returns true if |descr| is the element column of a FINITE_VECTOR-annotated group.
bool IsFiniteVectorElement(const ColumnDescriptor& descr) {
  const schema::Node* list = descr.schema_node()->parent();
  return list != nullptr && list->parent() != nullptr &&
         list->parent()->logical_type()->is_finite_vector();
}

Status GetSchemaMetadata(const ::arrow::Schema& schema, ::arrow::MemoryPool* pool,
                         const ArrowWriterProperties& properties,
                         std::shared_ptr<const KeyValueMetadata>* out) {
  if (!properties.store_schema()) {
    *out = nullptr;
    return Status::OK();
  }

  static const std::string kArrowSchemaKey = "ARROW:schema";
  std::shared_ptr<KeyValueMetadata> result;
  if (schema.metadata()) {
    result = schema.metadata()->Copy();
  } else {
    result = ::arrow::key_value_metadata({}, {});
  }

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> serialized,
                        ::arrow::ipc::SerializeSchema(schema, pool));

  // The serialized schema is not UTF-8, which is required for Thrift
  std::string schema_as_string = serialized->ToString();
  std::string schema_base64 = ::arrow::util::base64_encode(schema_as_string);
  result->Append(kArrowSchemaKey, std::move(schema_base64));
  *out = std::move(result);
  return Status::OK();
}

// Manages writing nested parquet columns with support for all nested types
// supported by parquet.
class ArrowColumnWriterV2 {
 public:
  // Constructs a new object (use Make() method below to construct from
  // A ChunkedArray).
  // level_builders should contain one MultipathLevelBuilder per chunk of the
  // Arrow-column to write.
  ArrowColumnWriterV2(std::vector<std::unique_ptr<MultipathLevelBuilder>> level_builders,
                      int start_leaf_column_index, int leaf_count,
                      RowGroupWriter* row_group_writer)
      : level_builders_(std::move(level_builders)),
        start_leaf_column_index_(start_leaf_column_index),
        leaf_count_(leaf_count),
        row_group_writer_(row_group_writer) {}

  // Writes out all leaf parquet columns to the RowGroupWriter that this
  // object was constructed with.  Each leaf column is written fully before
  // the next column is written (i.e. no buffering is assumed).
  //
  // Columns are written in DFS order.
  Status Write(ArrowWriteContext* ctx) {
    for (int leaf_idx = 0; leaf_idx < leaf_count_; leaf_idx++) {
      ColumnWriter* column_writer;
      if (row_group_writer_->buffered()) {
        const int column_index = start_leaf_column_index_ + leaf_idx;
        PARQUET_CATCH_NOT_OK(column_writer = row_group_writer_->column(column_index));
      } else {
        PARQUET_CATCH_NOT_OK(column_writer = row_group_writer_->NextColumn());
      }
      const bool is_vector_element = IsFiniteVectorElement(*column_writer->descr());
      for (auto& level_builder : level_builders_) {
        RETURN_NOT_OK(level_builder->Write(
            leaf_idx, ctx, [&](const MultipathLevelBuilderResult& result) {
              size_t visited_component_size = result.post_list_visited_elements.size();
              DCHECK_GT(visited_component_size, 0);
              std::shared_ptr<Array> values_array;
              if (visited_component_size == 1) {
                const ElementRange& range = result.post_list_visited_elements[0];
                values_array = result.leaf_array->Slice(range.start, range.Size());
              } else {
                // Multiple leaf ranges can be produced when child values are
                // skipped, such as null fixed-size-list slots, or when
                // list-view ranges are non-contiguous. Concatenate the slices
                // in logical write order.
                ::arrow::ArrayVector arrays;
                arrays.reserve(visited_component_size);
                for (const auto& range : result.post_list_visited_elements) {
                  DCHECK(!range.Empty());
                  arrays.push_back(result.leaf_array->Slice(range.start, range.Size()));
                }
                ARROW_ASSIGN_OR_RAISE(values_array,
                                      ::arrow::Concatenate(arrays, ctx->memory_pool));
              }
              if (is_vector_element) {
                RETURN_NOT_OK(
                    CheckFiniteVectorElements(*values_array, *column_writer->descr()));
              }

              return column_writer->WriteArrow(result.def_levels, result.rep_levels,
                                               result.def_rep_level_count, *values_array,
                                               ctx, result.leaf_is_nullable);
            }));
      }

      if (!row_group_writer_->buffered()) {
        PARQUET_CATCH_NOT_OK(column_writer->Close());
      }
    }
    return Status::OK();
  }

  // Make a new object by converting each chunk in |data| to a MultipathLevelBuilder.
  //
  // It is necessary to create a new builder per array because the MultipathlevelBuilder
  // extracts the data necessary for writing each leaf column at construction time.
  // (it optimizes based on null count) and with slicing via |offset| ephemeral
  // chunks are created which need to be tracked across each leaf column-write.
  // This decision could potentially be revisited if we wanted to use "buffered"
  // RowGroupWriters (we could construct each builder on demand in that case).
  static ::arrow::Result<std::unique_ptr<ArrowColumnWriterV2>> Make(
      const ChunkedArray& data, int64_t offset, const int64_t size,
      const SchemaManifest& schema_manifest, RowGroupWriter* row_group_writer,
      int start_leaf_column_index = -1) {
    int64_t absolute_position = 0;
    int chunk_index = 0;
    int64_t chunk_offset = 0;
    if (data.length() == 0) {
      return std::make_unique<ArrowColumnWriterV2>(
          std::vector<std::unique_ptr<MultipathLevelBuilder>>{}, start_leaf_column_index,
          CalculateLeafCount(data.type().get()), row_group_writer);
    }
    while (chunk_index < data.num_chunks() && absolute_position < offset) {
      const int64_t chunk_length = data.chunk(chunk_index)->length();
      if (absolute_position + chunk_length > offset) {
        // Relative offset into the chunk to reach the desired start offset for
        // writing
        chunk_offset = offset - absolute_position;
        break;
      } else {
        ++chunk_index;
        absolute_position += chunk_length;
      }
    }

    if (absolute_position >= data.length()) {
      return Status::Invalid("Cannot write data at offset past end of chunked array");
    }

    int64_t values_written = 0;
    std::vector<std::unique_ptr<MultipathLevelBuilder>> builders;
    const int leaf_count = CalculateLeafCount(data.type().get());
    bool is_nullable = false;
    const SchemaField* root_field = nullptr;

    int column_index = 0;
    if (row_group_writer->buffered()) {
      column_index = start_leaf_column_index;
    } else {
      // The row_group_writer hasn't been advanced yet so add 1 to the current
      // which is the one this instance will start writing for.
      column_index = row_group_writer->current_column() + 1;
    }

    for (int leaf_offset = 0; leaf_offset < leaf_count; ++leaf_offset) {
      const SchemaField* schema_field = nullptr;
      RETURN_NOT_OK(
          schema_manifest.GetColumnField(column_index + leaf_offset, &schema_field));
      bool nullable_root = HasNullableRoot(schema_manifest, schema_field);
      if (leaf_offset == 0) {
        is_nullable = nullable_root;
        root_field = GetRootField(schema_manifest, schema_field);
      }

// Don't validate common ancestry for all leafs if not in debug.
#ifndef NDEBUG
      break;
#else
      if (is_nullable != nullable_root) {
        return Status::UnknownError(
            "Unexpected mismatched nullability between column index",
            column_index + leaf_offset, " and ", column_index);
      }
#endif
    }
    while (values_written < size) {
      const Array& chunk = *data.chunk(chunk_index);
      const int64_t available_values = chunk.length() - chunk_offset;
      const int64_t chunk_write_size = std::min(size - values_written, available_values);

      // The chunk offset here will be 0 except for possibly the first chunk
      // because of the advancing logic above
      std::shared_ptr<Array> array_to_write = chunk.Slice(chunk_offset, chunk_write_size);
      if (root_field != nullptr) {
        ARROW_ASSIGN_OR_RAISE(
            std::shared_ptr<ArrayData> prepared,
            MakeFiniteVectorElementsRequired(array_to_write->data(), *root_field));
        if (prepared != array_to_write->data()) {
          array_to_write = ::arrow::MakeArray(std::move(prepared));
        }
      }

      if (array_to_write->length() > 0) {
        ARROW_ASSIGN_OR_RAISE(std::unique_ptr<MultipathLevelBuilder> builder,
                              MultipathLevelBuilder::Make(*array_to_write, is_nullable));
        if (leaf_count != builder->GetLeafCount()) {
          return Status::UnknownError("data type leaf_count != builder_leaf_count",
                                      leaf_count, " ", builder->GetLeafCount());
        }
        builders.emplace_back(std::move(builder));
      }

      if (chunk_write_size == available_values) {
        chunk_offset = 0;
        ++chunk_index;
      }
      values_written += chunk_write_size;
    }
    return std::make_unique<ArrowColumnWriterV2>(std::move(builders), column_index,
                                                 leaf_count, row_group_writer);
  }

  int leaf_count() const { return leaf_count_; }

 private:
  // One builder per column-chunk.
  std::vector<std::unique_ptr<MultipathLevelBuilder>> level_builders_;
  int start_leaf_column_index_;
  int leaf_count_;
  RowGroupWriter* row_group_writer_;
};

}  // namespace

// ----------------------------------------------------------------------
// FileWriter implementation

class FileWriterImpl : public FileWriter {
 public:
  FileWriterImpl(std::shared_ptr<::arrow::Schema> schema, MemoryPool* pool,
                 std::unique_ptr<ParquetFileWriter> writer,
                 std::shared_ptr<ArrowWriterProperties> arrow_properties)
      : schema_(std::move(schema)),
        writer_(std::move(writer)),
        row_group_writer_(nullptr),
        column_write_context_(pool, arrow_properties.get()),
        arrow_properties_(std::move(arrow_properties)),
        closed_(false) {
    if (arrow_properties_->use_threads()) {
      parallel_column_write_contexts_.reserve(schema_->num_fields());
      for (int i = 0; i < schema_->num_fields(); ++i) {
        // Explicitly create each ArrowWriteContext object to avoid unintentional
        // call of the copy constructor. Otherwise, the buffers in the type of
        // shared_ptr will be shared among all contexts.
        parallel_column_write_contexts_.emplace_back(pool, arrow_properties_.get());
      }
    }
  }

  Status Init() {
    return SchemaManifest::Make(writer_->schema(), /*schema_metadata=*/nullptr,
                                default_arrow_reader_properties(), &schema_manifest_);
  }

  Status NewRowGroup() override {
    RETURN_NOT_OK(CheckClosed());
    if (row_group_writer_ != nullptr) {
      PARQUET_CATCH_NOT_OK(row_group_writer_->Close());
    }
    PARQUET_CATCH_NOT_OK(row_group_writer_ = writer_->AppendRowGroup());
    return Status::OK();
  }

  Status Close() override {
    if (!closed_) {
      // Make idempotent
      closed_ = true;
      if (row_group_writer_ != nullptr) {
        PARQUET_CATCH_NOT_OK(row_group_writer_->Close());
      }
      PARQUET_CATCH_NOT_OK(writer_->Close());
    }
    return Status::OK();
  }

  Status CheckClosed() const {
    if (closed_) {
      return Status::Invalid("Operation on closed file");
    }
    return Status::OK();
  }

  Status WriteColumnChunk(const Array& data) override {
    // A bit awkward here since cannot instantiate ChunkedArray from const Array&
    auto chunk = ::arrow::MakeArray(data.data());
    auto chunked_array = std::make_shared<::arrow::ChunkedArray>(chunk);
    return WriteColumnChunk(chunked_array, 0, data.length());
  }

  Status WriteColumnChunk(const std::shared_ptr<ChunkedArray>& data, int64_t offset,
                          int64_t size) override {
    RETURN_NOT_OK(CheckClosed());
    if (arrow_properties_->engine_version() == ArrowWriterProperties::V2 ||
        arrow_properties_->engine_version() == ArrowWriterProperties::V1) {
      if (row_group_writer_->buffered()) {
        return Status::Invalid("Cannot write column chunk into the buffered row group.");
      }
      ARROW_ASSIGN_OR_RAISE(
          std::unique_ptr<ArrowColumnWriterV2> writer,
          ArrowColumnWriterV2::Make(*data, offset, size, schema_manifest_,
                                    row_group_writer_));
      return writer->Write(&column_write_context_);
    }

    return Status::NotImplemented("Unknown engine version.");
  }

  Status WriteColumnChunk(const std::shared_ptr<::arrow::ChunkedArray>& data) override {
    return WriteColumnChunk(data, 0, data->length());
  }

  std::shared_ptr<::arrow::Schema> schema() const override { return schema_; }

  Status WriteTable(const Table& table, int64_t chunk_size) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(table.Validate());

    if (chunk_size <= 0 && table.num_rows() > 0) {
      return Status::Invalid("chunk size per row_group must be greater than 0");
    } else if (!table.schema()->Equals(*schema_, false)) {
      return Status::Invalid("table schema does not match this writer's. table:'",
                             table.schema()->ToString(), "' this:'", schema_->ToString(),
                             "'");
    } else if (chunk_size > this->properties().max_row_group_length()) {
      chunk_size = this->properties().max_row_group_length();
    }

    auto WriteRowGroup = [&](int64_t offset, int64_t size) {
      RETURN_NOT_OK(NewRowGroup());
      for (int i = 0; i < table.num_columns(); i++) {
        RETURN_NOT_OK(WriteColumnChunk(table.column(i), offset, size));
      }
      return Status::OK();
    };

    if (table.num_rows() == 0) {
      // Append a row group with 0 rows
      RETURN_NOT_OK(
          WriteRowGroup(0, 0).OrElse([&](auto&&) { PARQUET_IGNORE_NOT_OK(Close()); }));
    }

    for (int chunk = 0; chunk * chunk_size < table.num_rows(); chunk++) {
      int64_t offset = chunk * chunk_size;
      RETURN_NOT_OK(WriteRowGroup(offset, std::min(chunk_size, table.num_rows() - offset))
                        .OrElse([&](auto&&) { PARQUET_IGNORE_NOT_OK(Close()); }));
    }
    return Status::OK();
  }

  Status NewBufferedRowGroup() override {
    RETURN_NOT_OK(CheckClosed());
    if (row_group_writer_ != nullptr) {
      PARQUET_CATCH_NOT_OK(row_group_writer_->Close());
    }
    PARQUET_CATCH_NOT_OK(row_group_writer_ = writer_->AppendBufferedRowGroup());
    return Status::OK();
  }

  Status WriteRecordBatch(const RecordBatch& batch) override {
    RETURN_NOT_OK(CheckClosed());
    if (batch.num_rows() == 0) {
      return Status::OK();
    }

    // Max number of rows allowed in a row group.
    const int64_t max_row_group_length = this->properties().max_row_group_length();

    // Initialize a new buffered row group writer if necessary.
    if (row_group_writer_ == nullptr || !row_group_writer_->buffered() ||
        row_group_writer_->num_rows() >= max_row_group_length) {
      RETURN_NOT_OK(NewBufferedRowGroup());
    }

    auto WriteBatch = [&](int64_t offset, int64_t size) {
      std::vector<std::unique_ptr<ArrowColumnWriterV2>> writers;
      int column_index_start = 0;

      for (int i = 0; i < batch.num_columns(); i++) {
        ChunkedArray chunked_array{batch.column(i)};
        ARROW_ASSIGN_OR_RAISE(
            std::unique_ptr<ArrowColumnWriterV2> writer,
            ArrowColumnWriterV2::Make(chunked_array, offset, size, schema_manifest_,
                                      row_group_writer_, column_index_start));
        column_index_start += writer->leaf_count();
        if (arrow_properties_->use_threads()) {
          writers.emplace_back(std::move(writer));
        } else {
          RETURN_NOT_OK(writer->Write(&column_write_context_));
        }
      }

      if (arrow_properties_->use_threads()) {
        DCHECK_EQ(parallel_column_write_contexts_.size(), writers.size());
        RETURN_NOT_OK(::arrow::internal::ParallelFor(
            static_cast<int>(writers.size()),
            [&](int i) { return writers[i]->Write(&parallel_column_write_contexts_[i]); },
            arrow_properties_->executor()));
      }

      return Status::OK();
    };

    int64_t offset = 0;
    while (offset < batch.num_rows()) {
      const int64_t batch_size =
          std::min(max_row_group_length - row_group_writer_->num_rows(),
                   batch.num_rows() - offset);
      RETURN_NOT_OK(WriteBatch(offset, batch_size));
      offset += batch_size;

      // Flush current row group writer and create a new writer if it is full.
      if (row_group_writer_->num_rows() >= max_row_group_length &&
          offset < batch.num_rows()) {
        RETURN_NOT_OK(NewBufferedRowGroup());
      }
    }

    return Status::OK();
  }

  const WriterProperties& properties() const { return *writer_->properties(); }

  ::arrow::MemoryPool* memory_pool() const override {
    return column_write_context_.memory_pool;
  }

  const std::shared_ptr<FileMetaData> metadata() const override {
    return writer_->metadata();
  }

  /// \brief Append the key-value metadata to the file metadata
  ::arrow::Status AddKeyValueMetadata(
      const std::shared_ptr<const ::arrow::KeyValueMetadata>& key_value_metadata)
      override {
    PARQUET_CATCH_NOT_OK(writer_->AddKeyValueMetadata(key_value_metadata));
    return Status::OK();
  }

 private:
  friend class FileWriter;

  std::shared_ptr<::arrow::Schema> schema_;

  SchemaManifest schema_manifest_;

  std::unique_ptr<ParquetFileWriter> writer_;
  RowGroupWriter* row_group_writer_;
  ArrowWriteContext column_write_context_;
  std::shared_ptr<ArrowWriterProperties> arrow_properties_;
  bool closed_;

  /// If arrow_properties_.use_threads() is true, the vector size is equal to
  /// schema_->num_fields() to make it thread-safe. Otherwise, the vector is
  /// empty and column_write_context_ above is shared by all columns.
  std::vector<ArrowWriteContext> parallel_column_write_contexts_;
};

FileWriter::~FileWriter() {}

Status FileWriter::Make(::arrow::MemoryPool* pool,
                        std::unique_ptr<ParquetFileWriter> writer,
                        std::shared_ptr<::arrow::Schema> schema,
                        std::shared_ptr<ArrowWriterProperties> arrow_properties,
                        std::unique_ptr<FileWriter>* out) {
  std::unique_ptr<FileWriterImpl> impl(new FileWriterImpl(
      std::move(schema), pool, std::move(writer), std::move(arrow_properties)));
  RETURN_NOT_OK(impl->Init());
  *out = std::move(impl);
  return Status::OK();
}

Result<std::unique_ptr<FileWriter>> FileWriter::Open(
    const ::arrow::Schema& schema, ::arrow::MemoryPool* pool,
    std::shared_ptr<::arrow::io::OutputStream> sink,
    std::shared_ptr<WriterProperties> properties,
    std::shared_ptr<ArrowWriterProperties> arrow_properties) {
  std::shared_ptr<SchemaDescriptor> parquet_schema;
  RETURN_NOT_OK(
      ToParquetSchema(&schema, *properties, *arrow_properties, &parquet_schema));

  auto schema_node = std::static_pointer_cast<GroupNode>(parquet_schema->schema_root());

  std::shared_ptr<const KeyValueMetadata> metadata;
  RETURN_NOT_OK(GetSchemaMetadata(schema, pool, *arrow_properties, &metadata));

  std::unique_ptr<ParquetFileWriter> base_writer;
  PARQUET_CATCH_NOT_OK(base_writer = ParquetFileWriter::Open(std::move(sink), schema_node,
                                                             std::move(properties),
                                                             std::move(metadata)));

  std::unique_ptr<FileWriter> writer;
  auto schema_ptr = std::make_shared<::arrow::Schema>(schema);
  RETURN_NOT_OK(Make(pool, std::move(base_writer), std::move(schema_ptr),
                     std::move(arrow_properties), &writer));

  return writer;
}

Status WriteFileMetaData(const FileMetaData& file_metadata,
                         ::arrow::io::OutputStream* sink) {
  PARQUET_CATCH_NOT_OK(::parquet::WriteFileMetaData(file_metadata, sink));
  return Status::OK();
}

Status WriteMetaDataFile(const FileMetaData& file_metadata,
                         ::arrow::io::OutputStream* sink) {
  PARQUET_CATCH_NOT_OK(::parquet::WriteMetaDataFile(file_metadata, sink));
  return Status::OK();
}

Status WriteTable(const ::arrow::Table& table, ::arrow::MemoryPool* pool,
                  std::shared_ptr<::arrow::io::OutputStream> sink, int64_t chunk_size,
                  std::shared_ptr<WriterProperties> properties,
                  std::shared_ptr<ArrowWriterProperties> arrow_properties) {
  std::unique_ptr<FileWriter> writer;
  ARROW_ASSIGN_OR_RAISE(
      writer, FileWriter::Open(*table.schema(), pool, std::move(sink),
                               std::move(properties), std::move(arrow_properties)));
  RETURN_NOT_OK(writer->WriteTable(table, chunk_size));
  return writer->Close();
}

}  // namespace parquet::arrow
