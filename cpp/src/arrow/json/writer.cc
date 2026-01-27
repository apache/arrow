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

#include "arrow/json/writer.h"

#include "arrow/array.h"
#include "arrow/io/interfaces.h"
#include "arrow/ipc/writer.h"
#include "arrow/json/rapidjson_defs.h"  // IWYU pragma: keep
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/iterator.h"
#include "arrow/visit_array_inline.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <limits>
#include <memory>
#include <string>

namespace arrow {
namespace json {

namespace rj = arrow::rapidjson;

namespace {

// \brief Builder that holds state for a single conversion.
//
// Implements Visit() methods for each type of Arrow Array that set the values
// of the corresponding fields in each row.
class RowBatchBuilder {
 public:
  explicit RowBatchBuilder(int64_t num_rows, bool emit_null = true)
      : field_(nullptr), emit_null_(emit_null) {
    // Reserve all of the space required up-front to avoid unnecessary resizing
    rows_.reserve(num_rows);

    for (int64_t i = 0; i < num_rows; ++i) {
      rows_.push_back(rj::Document());
      rows_[i].SetObject();
    }
  }

  /// \brief Set which field to convert.
  void SetField(const arrow::Field* field) { field_ = field; }

  /// \brief Retrieve converted rows from builder.
  std::vector<rj::Document> Rows() && { return std::move(rows_); }

  // Default implementation
  arrow::Status Visit(const arrow::Array& array) {
    return arrow::Status::NotImplemented(
        "Cannot convert to json document for array of type ", array.type()->ToString());
  }

  // Handles booleans, integers, floats, temporal types
  // (excludes interval types with struct C types)
  template <typename ArrayType, typename DataClass = typename ArrayType::TypeClass>
  arrow::enable_if_t<arrow::has_c_type<DataClass>::value &&
                         !arrow::is_interval_type<DataClass>::value,
                     arrow::Status>
  Visit(const ArrayType& array) {
    assert(static_cast<int64_t>(rows_.size()) == array.length());
    for (int64_t i = 0; i < array.length(); ++i) {
      if (array.IsNull(i) && !emit_null_) {
        continue;
      }
      rj::Value str_key(field_->name(), rows_[i].GetAllocator());
      if (array.IsNull(i)) {
        rows_[i].AddMember(str_key, rj::Value(), rows_[i].GetAllocator());
      } else {
        rows_[i].AddMember(str_key, array.Value(i), rows_[i].GetAllocator());
      }
    }
    return arrow::Status::OK();
  }

  // Handle string types
  template <typename ArrayType, typename T = typename ArrayType::TypeClass>
  arrow::enable_if_string_like<T, arrow::Status> Visit(const ArrayType& array) {
    assert(static_cast<int64_t>(rows_.size()) == array.length());
    for (int64_t i = 0; i < array.length(); ++i) {
      if (array.IsNull(i) && !emit_null_) {
        continue;
      }
      rj::Value str_key(field_->name(), rows_[i].GetAllocator());
      if (array.IsNull(i)) {
        rows_[i].AddMember(str_key, rj::Value(), rows_[i].GetAllocator());
      } else {
        std::string_view value_view = array.Value(i);
        rj::Value value;
        value.SetString(value_view.data(), static_cast<rj::SizeType>(value_view.size()),
                        rows_[i].GetAllocator());
        rows_[i].AddMember(str_key, value, rows_[i].GetAllocator());
      }
    }
    return arrow::Status::OK();
  }

  // Handle struct
  arrow::Status Visit(const arrow::StructArray& array) {
    const arrow::StructType* type = array.struct_type();

    assert(static_cast<int64_t>(rows_.size()) == array.length());

    RowBatchBuilder child_builder(rows_.size(), emit_null_);
    for (int i = 0; i < type->num_fields(); ++i) {
      const arrow::Field* child_field = type->field(i).get();
      child_builder.SetField(child_field);
      ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*array.field(i).get(), &child_builder));
    }
    std::vector<rj::Document> rows = std::move(child_builder).Rows();

    for (int64_t i = 0; i < array.length(); ++i) {
      if (array.IsNull(i) && !emit_null_) {
        continue;
      }
      rj::Value str_key(field_->name(), rows_[i].GetAllocator());
      if (array.IsNull(i)) {
        rows_[i].AddMember(str_key, rj::Value(), rows_[i].GetAllocator());
      } else {
        // Must copy value to new allocator
        rj::Value row_val;
        row_val.CopyFrom(rows[i], rows_[i].GetAllocator());
        rows_[i].AddMember(str_key, row_val, rows_[i].GetAllocator());
      }
    }
    return arrow::Status::OK();
  }

  // Handle list-like types
  template <typename ArrayType, typename T = typename ArrayType::TypeClass>
  arrow::enable_if_list_like<T, arrow::Status> Visit(const ArrayType& array) {
    assert(static_cast<int64_t>(rows_.size()) == array.length());
    // First create rows from values
    std::shared_ptr<arrow::Array> values = array.values();
    RowBatchBuilder child_builder(values->length(), emit_null_);
    const arrow::Field* value_field = array.list_type()->value_field().get();
    std::string value_field_name = value_field->name();
    child_builder.SetField(value_field);
    ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*values.get(), &child_builder));

    std::vector<rj::Document> rows = std::move(child_builder).Rows();

    int64_t values_i = 0;
    for (int64_t i = 0; i < array.length(); ++i) {
      if (array.IsNull(i) && !emit_null_) {
        continue;
      }
      rj::Document::AllocatorType& allocator = rows_[i].GetAllocator();
      rj::Value str_key(field_->name(), allocator);

      if (array.IsNull(i)) {
        rows_[i].AddMember(str_key, rj::Value(), allocator);
        continue;
      }

      auto array_len = array.value_length(i);
      rj::Value value;
      value.SetArray();
      value.Reserve(static_cast<rj::SizeType>(array_len), allocator);

      for (int64_t j = 0; j < array_len; ++j) {
        rj::Value row_val;
        // Must copy value to new allocator
        row_val.CopyFrom(rows[values_i][value_field_name], allocator);
        value.PushBack(row_val, allocator);
        ++values_i;
      }

      rows_[i].AddMember(str_key, value, allocator);
    }

    return arrow::Status::OK();
  }

  // Handle null
  arrow::Status Visit(const arrow::NullArray& array) {
    assert(static_cast<int64_t>(rows_.size()) == array.length());
    if (emit_null_) {
      for (int64_t i = 0; i < array.length(); ++i) {
        rj::Value str_key(field_->name(), rows_[i].GetAllocator());
        rows_[i].AddMember(str_key, rj::Value(), rows_[i].GetAllocator());
      }
    }
    return arrow::Status::OK();
  }

 private:
  const arrow::Field* field_;
  std::vector<rj::Document> rows_;
  bool emit_null_;
};

class JSONWriterImpl : public ipc::RecordBatchWriter {
 public:
  static Result<std::shared_ptr<JSONWriterImpl>> Make(
      io::OutputStream* sink, std::shared_ptr<io::OutputStream> owned_sink,
      std::shared_ptr<Schema> schema, const WriteOptions& options) {
    RETURN_NOT_OK(options.Validate());
    auto writer = std::make_shared<JSONWriterImpl>(sink, std::move(owned_sink),
                                                   std::move(schema), options);
    return writer;
  }

  Status WriteRecordBatch(const RecordBatch& batch) override {
    RowBatchBuilder builder{batch.num_rows(), options_.emit_null};

    for (int i = 0; i < batch.num_columns(); ++i) {
      builder.SetField(batch.schema()->field(i).get());
      ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*batch.column(i).get(), &builder));
    }

    for (const auto& doc : std::move(builder).Rows()) {
      rj::StringBuffer sb;
      rj::Writer<rj::StringBuffer> writer(sb);
      doc.Accept(writer);
      sb.Put('\n');
      RETURN_NOT_OK(sink_->Write(sb.GetString()));
    }
    stats_.num_record_batches++;
    return Status::OK();
  }

  Status WriteTable(const Table& table, int64_t max_chunksize) override {
    TableBatchReader reader(table);
    reader.set_chunksize(max_chunksize > 0 ? max_chunksize : options_.batch_size);
    std::shared_ptr<RecordBatch> batch;
    RETURN_NOT_OK(reader.ReadNext(&batch));
    while (batch != nullptr) {
      RETURN_NOT_OK(WriteRecordBatch(*batch));
      RETURN_NOT_OK(reader.ReadNext(&batch));
    }
    return Status::OK();
  }

  Status Close() override { return Status::OK(); }

  ipc::WriteStats stats() const override { return stats_; }

  JSONWriterImpl(io::OutputStream* sink, std::shared_ptr<io::OutputStream> owned_sink,
                 std::shared_ptr<Schema> schema, const WriteOptions& options)
      : sink_(sink),
        owned_sink_(std::move(owned_sink)),
        schema_(std::move(schema)),
        options_(options) {}

 private:
  io::OutputStream* sink_;
  std::shared_ptr<io::OutputStream> owned_sink_;
  const std::shared_ptr<Schema> schema_;
  const WriteOptions options_;
  ipc::WriteStats stats_;
};

}  // namespace

Status WriteJSON(const Table& table, const WriteOptions& options,
                 arrow::io::OutputStream* output) {
  ARROW_ASSIGN_OR_RAISE(auto writer, MakeJSONWriter(output, table.schema(), options));
  RETURN_NOT_OK(writer->WriteTable(table));
  return writer->Close();
}

Status WriteJSON(const RecordBatch& batch, const WriteOptions& options,
                 arrow::io::OutputStream* output) {
  ARROW_ASSIGN_OR_RAISE(auto writer, MakeJSONWriter(output, batch.schema(), options));
  RETURN_NOT_OK(writer->WriteRecordBatch(batch));
  return writer->Close();
}

Status WriteJSON(const std::shared_ptr<RecordBatchReader>& reader,
                 const WriteOptions& options, arrow::io::OutputStream* output) {
  ARROW_ASSIGN_OR_RAISE(auto writer, MakeJSONWriter(output, reader->schema(), options));
  std::shared_ptr<RecordBatch> batch;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(batch, reader->Next());
    if (batch == nullptr) break;
    RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  }
  return writer->Close();
}

ARROW_EXPORT
Result<std::shared_ptr<ipc::RecordBatchWriter>> MakeJSONWriter(
    std::shared_ptr<io::OutputStream> sink, const std::shared_ptr<Schema>& schema,
    const WriteOptions& options) {
  return JSONWriterImpl::Make(sink.get(), sink, schema, options);
}

ARROW_EXPORT
Result<std::shared_ptr<ipc::RecordBatchWriter>> MakeJSONWriter(
    io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
    const WriteOptions& options) {
  return JSONWriterImpl::Make(sink, nullptr, schema, options);
}

}  // namespace json
}  // namespace arrow
