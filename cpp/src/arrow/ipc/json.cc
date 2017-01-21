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

#include "arrow/ipc/json.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/ipc/json-internal.h"
#include "arrow/memory_pool.h"
#include "arrow/schema.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace ipc {

// ----------------------------------------------------------------------
// Writer implementation

class JsonWriter::JsonWriterImpl {
 public:
  explicit JsonWriterImpl(const std::shared_ptr<Schema>& schema) : schema_(schema) {
    writer_.reset(new RjWriter(string_buffer_));
  }

  Status Start() {
    writer_->StartObject();

    writer_->Key("schema");
    RETURN_NOT_OK(WriteJsonSchema(*schema_.get(), writer_.get()));

    // Record batches
    writer_->Key("batches");
    writer_->StartArray();
    return Status::OK();
  }

  Status Finish(std::string* result) {
    writer_->EndArray();  // Record batches
    writer_->EndObject();

    *result = string_buffer_.GetString();
    return Status::OK();
  }

  Status WriteRecordBatch(const RecordBatch& batch) {
    DCHECK_EQ(batch.num_columns(), schema_->num_fields());

    writer_->StartObject();
    writer_->Key("count");
    writer_->Int(batch.num_rows());

    writer_->Key("columns");
    writer_->StartArray();

    for (int i = 0; i < schema_->num_fields(); ++i) {
      const std::shared_ptr<Array>& column = batch.column(i);

      DCHECK_EQ(batch.num_rows(), column->length())
          << "Array length did not match record batch length";

      RETURN_NOT_OK(WriteJsonArray(schema_->field(i)->name, *column, writer_.get()));
    }

    writer_->EndArray();
    writer_->EndObject();
    return Status::OK();
  }

 private:
  std::shared_ptr<Schema> schema_;

  rj::StringBuffer string_buffer_;
  std::unique_ptr<RjWriter> writer_;
};

JsonWriter::JsonWriter(const std::shared_ptr<Schema>& schema) {
  impl_.reset(new JsonWriterImpl(schema));
}

JsonWriter::~JsonWriter() {}

Status JsonWriter::Open(
    const std::shared_ptr<Schema>& schema, std::unique_ptr<JsonWriter>* writer) {
  *writer = std::unique_ptr<JsonWriter>(new JsonWriter(schema));
  return (*writer)->impl_->Start();
}

Status JsonWriter::Finish(std::string* result) {
  return impl_->Finish(result);
}

Status JsonWriter::WriteRecordBatch(const RecordBatch& batch) {
  return impl_->WriteRecordBatch(batch);
}

// ----------------------------------------------------------------------
// Reader implementation

class JsonReader::JsonReaderImpl {
 public:
  JsonReaderImpl(MemoryPool* pool, const std::shared_ptr<Buffer>& data)
      : pool_(pool), data_(data) {}

  Status ParseAndReadSchema() {
    doc_.Parse(reinterpret_cast<const rj::Document::Ch*>(data_->data()),
        static_cast<size_t>(data_->size()));
    if (doc_.HasParseError()) { return Status::IOError("JSON parsing failed"); }

    auto it = doc_.FindMember("schema");
    RETURN_NOT_OBJECT("schema", it, doc_);
    RETURN_NOT_OK(ReadJsonSchema(it->value, &schema_));

    it = doc_.FindMember("batches");
    RETURN_NOT_ARRAY("batches", it, doc_);
    record_batches_ = &it->value;

    return Status::OK();
  }

  Status GetRecordBatch(int i, std::shared_ptr<RecordBatch>* batch) const {
    DCHECK_GE(i, 0) << "i out of bounds";
    DCHECK_LT(i, static_cast<int>(record_batches_->GetArray().Size()))
        << "i out of bounds";

    const auto& batch_val = record_batches_->GetArray()[i];
    DCHECK(batch_val.IsObject());

    const auto& batch_obj = batch_val.GetObject();

    auto it = batch_obj.FindMember("count");
    RETURN_NOT_INT("count", it, batch_obj);
    int32_t num_rows = static_cast<int32_t>(it->value.GetInt());

    it = batch_obj.FindMember("columns");
    RETURN_NOT_ARRAY("columns", it, batch_obj);
    const auto& json_columns = it->value.GetArray();

    std::vector<std::shared_ptr<Array>> columns(json_columns.Size());
    for (size_t i = 0; i < columns.size(); ++i) {
      const std::shared_ptr<DataType>& type = schema_->field(i)->type;
      RETURN_NOT_OK(ReadJsonArray(pool_, json_columns[i], type, &columns[i]));
    }

    *batch = std::make_shared<RecordBatch>(schema_, num_rows, columns);
    return Status::OK();
  }

  std::shared_ptr<Schema> schema() const { return schema_; }

  int num_record_batches() const {
    return static_cast<int>(record_batches_->GetArray().Size());
  }

 private:
  MemoryPool* pool_;
  std::shared_ptr<Buffer> data_;
  rj::Document doc_;

  const rj::Value* record_batches_;

  std::shared_ptr<Schema> schema_;
};

JsonReader::JsonReader(MemoryPool* pool, const std::shared_ptr<Buffer>& data) {
  impl_.reset(new JsonReaderImpl(pool, data));
}

JsonReader::~JsonReader() {}

Status JsonReader::Open(
    const std::shared_ptr<Buffer>& data, std::unique_ptr<JsonReader>* reader) {
  return Open(default_memory_pool(), data, reader);
}

Status JsonReader::Open(MemoryPool* pool, const std::shared_ptr<Buffer>& data,
    std::unique_ptr<JsonReader>* reader) {
  *reader = std::unique_ptr<JsonReader>(new JsonReader(pool, data));
  return (*reader)->impl_->ParseAndReadSchema();
}

std::shared_ptr<Schema> JsonReader::schema() const {
  return impl_->schema();
}

int JsonReader::num_record_batches() const {
  return impl_->num_record_batches();
}

Status JsonReader::GetRecordBatch(int i, std::shared_ptr<RecordBatch>* batch) const {
  return impl_->GetRecordBatch(i, batch);
}

}  // namespace ipc
}  // namespace arrow
