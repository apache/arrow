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

#include <cstddef>
#include <memory>
#include <string>

#include "arrow/buffer.h"
#include "arrow/ipc/json-internal.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

using std::size_t;

namespace arrow {
namespace ipc {
namespace internal {
namespace json {

// ----------------------------------------------------------------------
// Writer implementation

class JsonWriter::JsonWriterImpl {
 public:
  explicit JsonWriterImpl(const std::shared_ptr<Schema>& schema) : schema_(schema) {
    writer_.reset(new RjWriter(string_buffer_));
  }

  Status Start() {
    writer_->StartObject();
    RETURN_NOT_OK(json::WriteSchema(*schema_, writer_.get()));

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
    return json::WriteRecordBatch(batch, writer_.get());
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

Status JsonWriter::Open(const std::shared_ptr<Schema>& schema,
                        std::unique_ptr<JsonWriter>* writer) {
  *writer = std::unique_ptr<JsonWriter>(new JsonWriter(schema));
  return (*writer)->impl_->Start();
}

Status JsonWriter::Finish(std::string* result) { return impl_->Finish(result); }

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
    if (doc_.HasParseError()) {
      return Status::IOError("JSON parsing failed");
    }

    RETURN_NOT_OK(json::ReadSchema(doc_, pool_, &schema_));

    auto it = doc_.FindMember("batches");
    RETURN_NOT_ARRAY("batches", it, doc_);
    record_batches_ = &it->value;

    return Status::OK();
  }

  Status ReadRecordBatch(int i, std::shared_ptr<RecordBatch>* batch) const {
    DCHECK_GE(i, 0) << "i out of bounds";
    DCHECK_LT(i, static_cast<int>(record_batches_->GetArray().Size()))
        << "i out of bounds";

    return json::ReadRecordBatch(record_batches_->GetArray()[i], schema_, pool_, batch);
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

Status JsonReader::Open(const std::shared_ptr<Buffer>& data,
                        std::unique_ptr<JsonReader>* reader) {
  return Open(default_memory_pool(), data, reader);
}

Status JsonReader::Open(MemoryPool* pool, const std::shared_ptr<Buffer>& data,
                        std::unique_ptr<JsonReader>* reader) {
  *reader = std::unique_ptr<JsonReader>(new JsonReader(pool, data));
  return (*reader)->impl_->ParseAndReadSchema();
}

std::shared_ptr<Schema> JsonReader::schema() const { return impl_->schema(); }

int JsonReader::num_record_batches() const { return impl_->num_record_batches(); }

Status JsonReader::ReadRecordBatch(int i, std::shared_ptr<RecordBatch>* batch) const {
  return impl_->ReadRecordBatch(i, batch);
}

}  // namespace json
}  // namespace internal
}  // namespace ipc
}  // namespace arrow
