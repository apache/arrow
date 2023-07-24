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

#include "arrow/testing/json_integration.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/json_internal.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

using std::size_t;

namespace arrow {

using ipc::DictionaryFieldMapper;
using ipc::DictionaryMemo;

namespace testing {

// ----------------------------------------------------------------------
// Writer implementation

class IntegrationJsonWriter::Impl {
 public:
  explicit Impl(const std::shared_ptr<Schema>& schema)
      : schema_(schema), mapper_(*schema), first_batch_written_(false) {
    writer_.reset(new RjWriter(string_buffer_));
  }

  Status Start() {
    writer_->StartObject();
    RETURN_NOT_OK(json::WriteSchema(*schema_, mapper_, writer_.get()));
    return Status::OK();
  }

  Status FirstRecordBatch(const RecordBatch& batch) {
    ARROW_ASSIGN_OR_RAISE(const auto dictionaries, CollectDictionaries(batch, mapper_));

    // Write dictionaries, if any
    if (!dictionaries.empty()) {
      writer_->Key("dictionaries");
      writer_->StartArray();
      for (const auto& entry : dictionaries) {
        RETURN_NOT_OK(json::WriteDictionary(entry.first, entry.second, writer_.get()));
      }
      writer_->EndArray();
    }

    // Record batches
    writer_->Key("batches");
    writer_->StartArray();
    first_batch_written_ = true;
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

    if (!first_batch_written_) {
      RETURN_NOT_OK(FirstRecordBatch(batch));
    }
    return json::WriteRecordBatch(batch, writer_.get());
  }

 private:
  std::shared_ptr<Schema> schema_;
  DictionaryFieldMapper mapper_;

  bool first_batch_written_;

  rj::StringBuffer string_buffer_;
  std::unique_ptr<RjWriter> writer_;
};

IntegrationJsonWriter::IntegrationJsonWriter(const std::shared_ptr<Schema>& schema) {
  impl_.reset(new Impl(schema));
}

IntegrationJsonWriter::~IntegrationJsonWriter() {}

Status IntegrationJsonWriter::Open(const std::shared_ptr<Schema>& schema,
                                   std::unique_ptr<IntegrationJsonWriter>* writer) {
  *writer = std::unique_ptr<IntegrationJsonWriter>(new IntegrationJsonWriter(schema));
  return (*writer)->impl_->Start();
}

Status IntegrationJsonWriter::Finish(std::string* result) {
  return impl_->Finish(result);
}

Status IntegrationJsonWriter::WriteRecordBatch(const RecordBatch& batch) {
  return impl_->WriteRecordBatch(batch);
}

// ----------------------------------------------------------------------
// Reader implementation

class IntegrationJsonReader::Impl {
 public:
  Impl(MemoryPool* pool, const std::shared_ptr<Buffer>& data)
      : pool_(pool), data_(data), record_batches_(nullptr) {}

  Status ParseAndReadSchema() {
    doc_.Parse(reinterpret_cast<const rj::Document::Ch*>(data_->data()),
               static_cast<size_t>(data_->size()));
    if (doc_.HasParseError()) {
      return Status::IOError("JSON parsing failed");
    }

    RETURN_NOT_OK(json::ReadSchema(doc_, pool_, &dictionary_memo_, &schema_));

    auto it = std::as_const(doc_).FindMember("batches");
    RETURN_NOT_ARRAY("batches", it, doc_);
    record_batches_ = &it->value;

    return Status::OK();
  }

  Status ReadRecordBatch(int i, std::shared_ptr<RecordBatch>* batch) {
    DCHECK_GE(i, 0) << "i out of bounds";
    DCHECK_LT(i, static_cast<int>(record_batches_->GetArray().Size()))
        << "i out of bounds";

    return json::ReadRecordBatch(record_batches_->GetArray()[i], schema_,
                                 &dictionary_memo_, pool_, batch);
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
  DictionaryMemo dictionary_memo_;
};

IntegrationJsonReader::IntegrationJsonReader(MemoryPool* pool,
                                             const std::shared_ptr<Buffer>& data) {
  impl_.reset(new Impl(pool, data));
}

IntegrationJsonReader::~IntegrationJsonReader() {}

Status IntegrationJsonReader::Open(const std::shared_ptr<Buffer>& data,
                                   std::unique_ptr<IntegrationJsonReader>* reader) {
  return Open(default_memory_pool(), data, reader);
}

Status IntegrationJsonReader::Open(MemoryPool* pool, const std::shared_ptr<Buffer>& data,
                                   std::unique_ptr<IntegrationJsonReader>* reader) {
  *reader = std::unique_ptr<IntegrationJsonReader>(new IntegrationJsonReader(pool, data));
  return (*reader)->impl_->ParseAndReadSchema();
}

Status IntegrationJsonReader::Open(MemoryPool* pool,
                                   const std::shared_ptr<io::ReadableFile>& in_file,
                                   std::unique_ptr<IntegrationJsonReader>* reader) {
  ARROW_ASSIGN_OR_RAISE(int64_t file_size, in_file->GetSize());
  ARROW_ASSIGN_OR_RAISE(auto json_buffer, in_file->Read(file_size));
  return Open(pool, json_buffer, reader);
}

std::shared_ptr<Schema> IntegrationJsonReader::schema() const { return impl_->schema(); }

int IntegrationJsonReader::num_record_batches() const {
  return impl_->num_record_batches();
}

Status IntegrationJsonReader::ReadRecordBatch(int i,
                                              std::shared_ptr<RecordBatch>* batch) const {
  return impl_->ReadRecordBatch(i, batch);
}

}  // namespace testing
}  // namespace arrow
