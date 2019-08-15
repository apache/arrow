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
#include <deque>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer-builder.h"
#include "arrow/ipc/writer.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/visitor_inline.h"

#include "parquet/arrow/reader_internal.h"
#include "parquet/arrow/schema.h"
#include "parquet/column_writer.h"
#include "parquet/exception.h"
#include "parquet/file_writer.h"
#include "parquet/platform.h"
#include "parquet/schema.h"

using arrow::Array;
using arrow::BinaryArray;
using arrow::BooleanArray;
using arrow::ChunkedArray;
using arrow::Decimal128Array;
using arrow::DictionaryArray;
using arrow::Field;
using arrow::FixedSizeBinaryArray;
using Int16BufferBuilder = arrow::TypedBufferBuilder<int16_t>;
using arrow::ListArray;
using arrow::MemoryPool;
using arrow::NumericArray;
using arrow::PrimitiveArray;
using arrow::ResizableBuffer;
using arrow::Status;
using arrow::Table;
using arrow::TimeUnit;

using parquet::ParquetFileWriter;
using parquet::ParquetVersion;
using parquet::schema::GroupNode;

namespace parquet {
namespace arrow {

namespace {

class LevelBuilder {
 public:
  explicit LevelBuilder(MemoryPool* pool, const SchemaField* schema_field,
                        const SchemaManifest* schema_manifest)
      : def_levels_(pool),
        rep_levels_(pool),
        schema_field_(schema_field),
        schema_manifest_(schema_manifest) {}

  Status VisitInline(const Array& array);

  template <typename T>
  typename std::enable_if<std::is_base_of<::arrow::FlatArray, T>::value, Status>::type
  Visit(const T& array) {
    array_offsets_.push_back(static_cast<int32_t>(array.offset()));
    valid_bitmaps_.push_back(array.null_bitmap_data());
    null_counts_.push_back(array.null_count());
    values_array_ = std::make_shared<T>(array.data());
    return Status::OK();
  }

  Status Visit(const DictionaryArray& array) {
    // Only currently handle DictionaryArray where the dictionary is a
    // primitive type
    if (array.dict_type()->value_type()->num_children() > 0) {
      return Status::NotImplemented(
          "Writing DictionaryArray with nested dictionary "
          "type not yet supported");
    }
    array_offsets_.push_back(static_cast<int32_t>(array.offset()));
    valid_bitmaps_.push_back(array.null_bitmap_data());
    null_counts_.push_back(array.null_count());
    values_array_ = std::make_shared<DictionaryArray>(array.data());
    return Status::OK();
  }

  Status Visit(const ListArray& array) {
    array_offsets_.push_back(static_cast<int32_t>(array.offset()));
    valid_bitmaps_.push_back(array.null_bitmap_data());
    null_counts_.push_back(array.null_count());
    offsets_.push_back(array.raw_value_offsets());

    // Min offset isn't always zero in the case of sliced Arrays.
    min_offset_idx_ = array.value_offset(min_offset_idx_);
    max_offset_idx_ = array.value_offset(max_offset_idx_);

    return VisitInline(*array.values());
  }

#define NOT_IMPLEMENTED_VISIT(ArrowTypePrefix)                             \
  Status Visit(const ::arrow::ArrowTypePrefix##Array& array) {             \
    return Status::NotImplemented("Level generation for " #ArrowTypePrefix \
                                  " not supported yet");                   \
  }

  NOT_IMPLEMENTED_VISIT(LargeList)
  NOT_IMPLEMENTED_VISIT(Map)
  NOT_IMPLEMENTED_VISIT(FixedSizeList)
  NOT_IMPLEMENTED_VISIT(Struct)
  NOT_IMPLEMENTED_VISIT(Union)
  NOT_IMPLEMENTED_VISIT(Extension)

#undef NOT_IMPLEMENTED_VISIT

  Status ExtractNullability() {
    // Walk upwards to extract nullability
    const SchemaField* current_field = schema_field_;
    while (current_field != nullptr) {
      nullable_.push_front(current_field->field->nullable());
      if (current_field->field->type()->num_children() > 1) {
        return Status::NotImplemented(
            "Fields with more than one child are not supported.");
      } else {
        current_field = schema_manifest_->GetParent(current_field);
      }
    }
    return Status::OK();
  }

  Status GenerateLevels(const Array& array, int64_t* values_offset, int64_t* num_values,
                        int64_t* num_levels,
                        const std::shared_ptr<ResizableBuffer>& def_levels_scratch,
                        std::shared_ptr<Buffer>* def_levels_out,
                        std::shared_ptr<Buffer>* rep_levels_out,
                        std::shared_ptr<Array>* values_array) {
    // Work downwards to extract bitmaps and offsets
    min_offset_idx_ = 0;
    max_offset_idx_ = array.length();
    RETURN_NOT_OK(VisitInline(array));
    *num_values = max_offset_idx_ - min_offset_idx_;
    *values_offset = min_offset_idx_;
    *values_array = values_array_;

    RETURN_NOT_OK(ExtractNullability());

    // Generate the levels.
    if (nullable_.size() == 1) {
      // We have a PrimitiveArray
      *rep_levels_out = nullptr;
      if (nullable_[0]) {
        RETURN_NOT_OK(
            def_levels_scratch->Resize(array.length() * sizeof(int16_t), false));
        auto def_levels_ptr =
            reinterpret_cast<int16_t*>(def_levels_scratch->mutable_data());
        if (array.null_count() == 0) {
          std::fill(def_levels_ptr, def_levels_ptr + array.length(), 1);
        } else if (array.null_count() == array.length()) {
          std::fill(def_levels_ptr, def_levels_ptr + array.length(), 0);
        } else {
          ::arrow::internal::BitmapReader valid_bits_reader(
              array.null_bitmap_data(), array.offset(), array.length());
          for (int i = 0; i < array.length(); i++) {
            def_levels_ptr[i] = valid_bits_reader.IsSet() ? 1 : 0;
            valid_bits_reader.Next();
          }
        }

        *def_levels_out = def_levels_scratch;
      } else {
        *def_levels_out = nullptr;
      }
      *num_levels = array.length();
    } else {
      // Note it is hard to estimate memory consumption due to zero length
      // arrays otherwise we would preallocate.  An upper boun on memory
      // is the sum of the length of each list array  + number of elements
      // but this might be too loose of an upper bound so we choose to use
      // safe methods.
      RETURN_NOT_OK(rep_levels_.Append(0));
      RETURN_NOT_OK(HandleListEntries(0, 0, 0, array.length()));

      RETURN_NOT_OK(def_levels_.Finish(def_levels_out));
      RETURN_NOT_OK(rep_levels_.Finish(rep_levels_out));
      *num_levels = (*rep_levels_out)->size() / sizeof(int16_t);
    }

    return Status::OK();
  }

  Status HandleList(int16_t def_level, int16_t rep_level, int64_t index) {
    if (nullable_[rep_level]) {
      if (null_counts_[rep_level] == 0 ||
          BitUtil::GetBit(valid_bitmaps_[rep_level], index + array_offsets_[rep_level])) {
        return HandleNonNullList(static_cast<int16_t>(def_level + 1), rep_level, index);
      } else {
        return def_levels_.Append(def_level);
      }
    } else {
      return HandleNonNullList(def_level, rep_level, index);
    }
  }

  Status HandleNonNullList(int16_t def_level, int16_t rep_level, int64_t index) {
    const int32_t inner_offset = offsets_[rep_level][index];
    const int32_t inner_length = offsets_[rep_level][index + 1] - inner_offset;
    const int64_t recursion_level = rep_level + 1;
    if (inner_length == 0) {
      return def_levels_.Append(def_level);
    }
    if (recursion_level < static_cast<int64_t>(offsets_.size())) {
      return HandleListEntries(static_cast<int16_t>(def_level + 1),
                               static_cast<int16_t>(rep_level + 1), inner_offset,
                               inner_length);
    }
    // We have reached the leaf: primitive list, handle remaining nullables
    const bool nullable_level = nullable_[recursion_level];
    const int64_t level_null_count = null_counts_[recursion_level];
    const uint8_t* level_valid_bitmap = valid_bitmaps_[recursion_level];

    if (inner_length >= 1) {
      RETURN_NOT_OK(
          rep_levels_.Append(inner_length - 1, static_cast<int16_t>(rep_level + 1)));
    }

    // Special case: this is a null array (all elements are null)
    if (level_null_count && level_valid_bitmap == nullptr) {
      return def_levels_.Append(inner_length, static_cast<int16_t>(def_level + 1));
    }
    for (int64_t i = 0; i < inner_length; i++) {
      if (nullable_level &&
          ((level_null_count == 0) ||
           BitUtil::GetBit(level_valid_bitmap,
                           inner_offset + i + array_offsets_[recursion_level]))) {
        // Non-null element in a null level
        RETURN_NOT_OK(def_levels_.Append(static_cast<int16_t>(def_level + 2)));
      } else {
        // This can be produced in two cases:
        //  * elements are nullable and this one is null
        //   (i.e. max_def_level = def_level + 2)
        //  * elements are non-nullable (i.e. max_def_level = def_level + 1)
        RETURN_NOT_OK(def_levels_.Append(static_cast<int16_t>(def_level + 1)));
      }
    }
    return Status::OK();
  }

  Status HandleListEntries(int16_t def_level, int16_t rep_level, int64_t offset,
                           int64_t length) {
    for (int64_t i = 0; i < length; i++) {
      if (i > 0) {
        RETURN_NOT_OK(rep_levels_.Append(rep_level));
      }
      RETURN_NOT_OK(HandleList(def_level, rep_level, offset + i));
    }
    return Status::OK();
  }

 private:
  Int16BufferBuilder def_levels_;
  Int16BufferBuilder rep_levels_;

  const SchemaField* schema_field_;
  const SchemaManifest* schema_manifest_;

  std::vector<int64_t> null_counts_;
  std::vector<const uint8_t*> valid_bitmaps_;
  std::vector<const int32_t*> offsets_;
  std::vector<int32_t> array_offsets_;
  std::deque<bool> nullable_;

  int64_t min_offset_idx_;
  int64_t max_offset_idx_;
  std::shared_ptr<Array> values_array_;
};

Status LevelBuilder::VisitInline(const Array& array) {
  return VisitArrayInline(array, this);
}

Status GetLeafType(const ::arrow::DataType& type, ::arrow::Type::type* leaf_type) {
  if (type.id() == ::arrow::Type::LIST || type.id() == ::arrow::Type::STRUCT) {
    if (type.num_children() != 1) {
      return Status::Invalid("Nested column branch had multiple children: ", type);
    }
    return GetLeafType(*type.child(0)->type(), leaf_type);
  } else {
    *leaf_type = type.id();
    return Status::OK();
  }
}

class ArrowColumnWriter {
 public:
  ArrowColumnWriter(ArrowWriteContext* ctx, ColumnWriter* column_writer,
                    const SchemaField* schema_field,
                    const SchemaManifest* schema_manifest)
      : ctx_(ctx),
        writer_(column_writer),
        schema_field_(schema_field),
        schema_manifest_(schema_manifest) {}

  Status Write(const Array& data) {
    if (data.length() == 0) {
      // Write nothing when length is 0
      return Status::OK();
    }

    ::arrow::Type::type values_type;
    RETURN_NOT_OK(GetLeafType(*data.type(), &values_type));

    std::shared_ptr<Array> _values_array;
    int64_t values_offset = 0;
    int64_t num_levels = 0;
    int64_t num_values = 0;
    LevelBuilder level_builder(ctx_->memory_pool, schema_field_, schema_manifest_);
    std::shared_ptr<Buffer> def_levels_buffer, rep_levels_buffer;
    RETURN_NOT_OK(level_builder.GenerateLevels(
        data, &values_offset, &num_values, &num_levels, ctx_->def_levels_buffer,
        &def_levels_buffer, &rep_levels_buffer, &_values_array));
    const int16_t* def_levels = nullptr;
    if (def_levels_buffer) {
      def_levels = reinterpret_cast<const int16_t*>(def_levels_buffer->data());
    }
    const int16_t* rep_levels = nullptr;
    if (rep_levels_buffer) {
      rep_levels = reinterpret_cast<const int16_t*>(rep_levels_buffer->data());
    }
    std::shared_ptr<Array> values_array = _values_array->Slice(values_offset, num_values);
    return writer_->WriteArrow(def_levels, rep_levels, num_levels, *values_array, ctx_);
  }

  Status Write(const ChunkedArray& data, int64_t offset, const int64_t size) {
    if (data.length() == 0) {
      return Status::OK();
    }

    int64_t absolute_position = 0;
    int chunk_index = 0;
    int64_t chunk_offset = 0;
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
    while (values_written < size) {
      const Array& chunk = *data.chunk(chunk_index);
      const int64_t available_values = chunk.length() - chunk_offset;
      const int64_t chunk_write_size = std::min(size - values_written, available_values);

      // The chunk offset here will be 0 except for possibly the first chunk
      // because of the advancing logic above
      std::shared_ptr<Array> array_to_write = chunk.Slice(chunk_offset, chunk_write_size);
      RETURN_NOT_OK(Write(*array_to_write));

      if (chunk_write_size == available_values) {
        chunk_offset = 0;
        ++chunk_index;
      }
      values_written += chunk_write_size;
    }

    return Status::OK();
  }

  Status Close() {
    PARQUET_CATCH_NOT_OK(writer_->Close());
    return Status::OK();
  }

 private:
  ArrowWriteContext* ctx_;
  ColumnWriter* writer_;
  const SchemaField* schema_field_;
  const SchemaManifest* schema_manifest_;
};

}  // namespace

// ----------------------------------------------------------------------
// FileWriter implementation

class FileWriterImpl : public FileWriter {
 public:
  FileWriterImpl(const std::shared_ptr<::arrow::Schema>& schema, MemoryPool* pool,
                 std::unique_ptr<ParquetFileWriter> writer,
                 const std::shared_ptr<ArrowWriterProperties>& arrow_properties)
      : schema_(schema),
        writer_(std::move(writer)),
        row_group_writer_(nullptr),
        column_write_context_(pool, arrow_properties.get()),
        arrow_properties_(arrow_properties),
        closed_(false) {}

  Status Init() {
    return BuildSchemaManifest(writer_->schema(), /*schema_metadata=*/nullptr,
                               default_arrow_reader_properties(), &schema_manifest_);
  }

  Status NewRowGroup(int64_t chunk_size) override {
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

  Status WriteColumnChunk(const Array& data) override {
    // A bit awkward here since cannot instantiate ChunkedArray from const Array&
    ::arrow::ArrayVector chunks = {::arrow::MakeArray(data.data())};
    auto chunked_array = std::make_shared<::arrow::ChunkedArray>(chunks);
    return WriteColumnChunk(chunked_array, 0, data.length());
  }

  Status WriteColumnChunk(const std::shared_ptr<ChunkedArray>& data, int64_t offset,
                          int64_t size) override {
    ColumnWriter* column_writer;
    PARQUET_CATCH_NOT_OK(column_writer = row_group_writer_->NextColumn());

    const SchemaField* schema_field;
    RETURN_NOT_OK(schema_manifest_.GetColumnField(row_group_writer_->current_column(),
                                                  &schema_field));
    ArrowColumnWriter arrow_writer(&column_write_context_, column_writer, schema_field,
                                   &schema_manifest_);
    RETURN_NOT_OK(arrow_writer.Write(*data, offset, size));
    return arrow_writer.Close();
  }

  Status WriteColumnChunk(const std::shared_ptr<::arrow::ChunkedArray>& data) override {
    return WriteColumnChunk(data, 0, data->length());
  }

  Status WriteTable(const Table& table, int64_t chunk_size) override {
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
      RETURN_NOT_OK(NewRowGroup(size));
      for (int i = 0; i < table.num_columns(); i++) {
        RETURN_NOT_OK(WriteColumnChunk(table.column(i), offset, size));
      }
      return Status::OK();
    };

    if (table.num_rows() == 0) {
      // Append a row group with 0 rows
      RETURN_NOT_OK_ELSE(WriteRowGroup(0, 0), PARQUET_IGNORE_NOT_OK(Close()));
      return Status::OK();
    }

    for (int chunk = 0; chunk * chunk_size < table.num_rows(); chunk++) {
      int64_t offset = chunk * chunk_size;
      RETURN_NOT_OK_ELSE(
          WriteRowGroup(offset, std::min(chunk_size, table.num_rows() - offset)),
          PARQUET_IGNORE_NOT_OK(Close()));
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

 private:
  friend class FileWriter;

  std::shared_ptr<::arrow::Schema> schema_;

  SchemaManifest schema_manifest_;

  std::unique_ptr<ParquetFileWriter> writer_;
  RowGroupWriter* row_group_writer_;
  ArrowWriteContext column_write_context_;
  std::shared_ptr<ArrowWriterProperties> arrow_properties_;
  bool closed_;
};

FileWriter::~FileWriter() {}

Status FileWriter::Make(::arrow::MemoryPool* pool,
                        std::unique_ptr<ParquetFileWriter> writer,
                        const std::shared_ptr<::arrow::Schema>& schema,
                        const std::shared_ptr<ArrowWriterProperties>& arrow_properties,
                        std::unique_ptr<FileWriter>* out) {
  std::unique_ptr<FileWriterImpl> impl(
      new FileWriterImpl(schema, pool, std::move(writer), arrow_properties));
  RETURN_NOT_OK(impl->Init());
  *out = std::move(impl);
  return Status::OK();
}

Status FileWriter::Open(const ::arrow::Schema& schema, ::arrow::MemoryPool* pool,
                        const std::shared_ptr<::arrow::io::OutputStream>& sink,
                        const std::shared_ptr<WriterProperties>& properties,
                        std::unique_ptr<FileWriter>* writer) {
  return Open(schema, pool, sink, properties, default_arrow_writer_properties(), writer);
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

  ::arrow::ipc::DictionaryMemo dict_memo;
  std::shared_ptr<Buffer> serialized;
  RETURN_NOT_OK(::arrow::ipc::SerializeSchema(schema, &dict_memo, pool, &serialized));
  result->Append(kArrowSchemaKey, serialized->ToString());
  *out = result;
  return Status::OK();
}

Status FileWriter::Open(const ::arrow::Schema& schema, ::arrow::MemoryPool* pool,
                        const std::shared_ptr<::arrow::io::OutputStream>& sink,
                        const std::shared_ptr<WriterProperties>& properties,
                        const std::shared_ptr<ArrowWriterProperties>& arrow_properties,
                        std::unique_ptr<FileWriter>* writer) {
  std::shared_ptr<SchemaDescriptor> parquet_schema;
  RETURN_NOT_OK(
      ToParquetSchema(&schema, *properties, *arrow_properties, &parquet_schema));

  auto schema_node = std::static_pointer_cast<GroupNode>(parquet_schema->schema_root());

  std::shared_ptr<const KeyValueMetadata> metadata;
  RETURN_NOT_OK(GetSchemaMetadata(schema, pool, *arrow_properties, &metadata));

  std::unique_ptr<ParquetFileWriter> base_writer =
      ParquetFileWriter::Open(sink, schema_node, properties, metadata);

  auto schema_ptr = std::make_shared<::arrow::Schema>(schema);
  return Make(pool, std::move(base_writer), schema_ptr, arrow_properties, writer);
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
                  const std::shared_ptr<::arrow::io::OutputStream>& sink,
                  int64_t chunk_size, const std::shared_ptr<WriterProperties>& properties,
                  const std::shared_ptr<ArrowWriterProperties>& arrow_properties) {
  std::unique_ptr<FileWriter> writer;
  RETURN_NOT_OK(FileWriter::Open(*table.schema(), pool, sink, properties,
                                 arrow_properties, &writer));
  RETURN_NOT_OK(writer->WriteTable(table, chunk_size));
  return writer->Close();
}

}  // namespace arrow
}  // namespace parquet
