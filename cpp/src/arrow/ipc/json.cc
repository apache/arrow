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

#include "arrow/ipc/json-internal.h"
#include "arrow/type.h"
#include "arrow/util/status.h"

namespace arrow {
namespace ipc {

class JsonWriter::JsonWriterImpl {
 public:
  JsonWriterImpl(const std::shared_ptr<Schema>& schema)
      : schema_(schema) {
    writer_.reset(new RjWriter(string_buffer_));
  }

  Status Start() {
    writer_->StartObject();

    writer_->Key("schema");
    RETURN_NOT_OK(WriteJsonSchema(schema_, writer_.get()));

    // Record batches
    writer_->Key("batches");
    writer_->StartArray();
    return Status::OK();
  }

  Status Finish() {
    writer_->EndArray();  // Record batches
    writer_->EndObject();
    return Status::OK();
  }

  Status WriteRecordBatch(const std::vector<std::shared_ptr<Array>>& columns, int32_t num_rows) {
    return Status::OK();
  }

 private:
  std::shared_ptr<Schema> schema_;

  rj::StringBuffer string_buffer_;
  std::unique_ptr<RjWriter> writer_;
};

JsonWriter::JsonWriter(const std::shared_ptr<Schema>& schema) {
  impl_.reset(new JsonWriteImpl(schema));
}

Status JsonWriter::Open(
    const std::shared_ptr<Schema>& schema, std::unique_ptr<JsonWriter>* writer) {
  *writer = std::unique_ptr<JsonWriter>(new JsonWriter(schema));
  return (*writer)->impl_->Start();
}

Status JsonWriter::Close() {
  return impl_->Close();
}

Status JsonWriter::WriteRecordBatch(
    const std::vector<std::shared_ptr<Array>>& columns, int32_t num_rows) {

}

}  // namespace ipc
}  // namespace arrow
