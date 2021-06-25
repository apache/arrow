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
#include "arrow/extension_type.h"
#include "arrow/ipc/writer.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/base64.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"
#include "arrow/visitor_inline.h"

#include "parquet/arrow/path_internal.h"
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
using arrow::DataType;
using arrow::DictionaryArray;
using arrow::ExtensionArray;
using arrow::ExtensionType;
using arrow::Field;
using arrow::FixedSizeBinaryArray;
using arrow::ListArray;
using arrow::MemoryPool;
using arrow::NumericArray;
using arrow::PrimitiveArray;
using arrow::ResizableBuffer;
using arrow::Status;
using arrow::Table;
using arrow::TimeUnit;

using arrow::internal::checked_cast;

using parquet::ParquetFileWriter;
using parquet::ParquetVersion;
using parquet::schema::GroupNode;

namespace parquet {
namespace arrow {

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

// Manages writing nested parquet columns with support for all nested types
// supported by parquet.
class ArrowColumnWriterV2 {
 public:
  // Constructs a new object (use Make() method below to construct from
  // A ChunkedArray).
  // level_builders should contain one MultipathLevelBuilder per chunk of the
  // Arrow-column to write.
  ArrowColumnWriterV2(std::vector<std::unique_ptr<MultipathLevelBuilder>> level_builders,
                      int leaf_count, RowGroupWriter* row_group_writer)
      : level_builders_(std::move(level_builders)),
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
      PARQUET_CATCH_NOT_OK(column_writer = row_group_writer_->NextColumn());
      for (auto& level_builder : level_builders_) {
        RETURN_NOT_OK(level_builder->Write(
            leaf_idx, ctx, [&](const MultipathLevelBuilderResult& result) {
              size_t visited_component_size = result.post_list_visited_elements.size();
              DCHECK_GT(visited_component_size, 0);
              if (visited_component_size != 1) {
                return Status::NotImplemented(
                    "Lists with non-zero length null components are not supported");
              }
              const ElementRange& range = result.post_list_visited_elements[0];
              std::shared_ptr<Array> values_array =
                  result.leaf_array->Slice(range.start, range.Size());

              return column_writer->WriteArrow(result.def_levels, result.rep_levels,
                                               result.def_rep_level_count, *values_array,
                                               ctx, result.leaf_is_nullable);
            }));
      }

      PARQUET_CATCH_NOT_OK(column_writer->Close());
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
      const SchemaManifest& schema_manifest, RowGroupWriter* row_group_writer) {
    int64_t absolute_position = 0;
    int chunk_index = 0;
    int64_t chunk_offset = 0;
    if (data.length() == 0) {
      return ::arrow::internal::make_unique<ArrowColumnWriterV2>(
          std::vector<std::unique_ptr<MultipathLevelBuilder>>{},
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
    // The row_group_writer hasn't been advanced yet so add 1 to the current
    // which is the one this instance will start writing for.
    int column_index = row_group_writer->current_column() + 1;
    for (int leaf_offset = 0; leaf_offset < leaf_count; ++leaf_offset) {
      const SchemaField* schema_field = nullptr;
      RETURN_NOT_OK(
          schema_manifest.GetColumnField(column_index + leaf_offset, &schema_field));
      bool nullable_root = HasNullableRoot(schema_manifest, schema_field);
      if (leaf_offset == 0) {
        is_nullable = nullable_root;
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
    return ::arrow::internal::make_unique<ArrowColumnWriterV2>(
        std::move(builders), leaf_count, row_group_writer);
  }

 private:
  // One builder per column-chunk.
  std::vector<std::unique_ptr<MultipathLevelBuilder>> level_builders_;
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
        closed_(false) {}

  Status Init() {
    return SchemaManifest::Make(writer_->schema(), /*schema_metadata=*/nullptr,
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
    auto chunk = ::arrow::MakeArray(data.data());
    auto chunked_array = std::make_shared<::arrow::ChunkedArray>(chunk);
    return WriteColumnChunk(chunked_array, 0, data.length());
  }

  Status WriteColumnChunk(const std::shared_ptr<ChunkedArray>& data, int64_t offset,
                          int64_t size) override {
    if (arrow_properties_->engine_version() == ArrowWriterProperties::V2 ||
        arrow_properties_->engine_version() == ArrowWriterProperties::V1) {
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
                        std::shared_ptr<::arrow::Schema> schema,
                        std::shared_ptr<ArrowWriterProperties> arrow_properties,
                        std::unique_ptr<FileWriter>* out) {
  std::unique_ptr<FileWriterImpl> impl(new FileWriterImpl(
      std::move(schema), pool, std::move(writer), std::move(arrow_properties)));
  RETURN_NOT_OK(impl->Init());
  *out = std::move(impl);
  return Status::OK();
}

Status FileWriter::Open(const ::arrow::Schema& schema, ::arrow::MemoryPool* pool,
                        std::shared_ptr<::arrow::io::OutputStream> sink,
                        std::shared_ptr<WriterProperties> properties,
                        std::unique_ptr<FileWriter>* writer) {
  return Open(std::move(schema), pool, std::move(sink), std::move(properties),
              default_arrow_writer_properties(), writer);
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
  std::string schema_base64 = ::arrow::util::base64_encode(
      reinterpret_cast<const unsigned char*>(schema_as_string.data()),
      static_cast<unsigned int>(schema_as_string.size()));
  result->Append(kArrowSchemaKey, schema_base64);
  *out = result;
  return Status::OK();
}

Status FileWriter::Open(const ::arrow::Schema& schema, ::arrow::MemoryPool* pool,
                        std::shared_ptr<::arrow::io::OutputStream> sink,
                        std::shared_ptr<WriterProperties> properties,
                        std::shared_ptr<ArrowWriterProperties> arrow_properties,
                        std::unique_ptr<FileWriter>* writer) {
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

  auto schema_ptr = std::make_shared<::arrow::Schema>(schema);
  return Make(pool, std::move(base_writer), std::move(schema_ptr),
              std::move(arrow_properties), writer);
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
  RETURN_NOT_OK(FileWriter::Open(*table.schema(), pool, std::move(sink),
                                 std::move(properties), std::move(arrow_properties),
                                 &writer));
  RETURN_NOT_OK(writer->WriteTable(table, chunk_size));
  return writer->Close();
}

}  // namespace arrow
}  // namespace parquet
