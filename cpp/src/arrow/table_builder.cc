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

#include "arrow/table_builder.h"

#include <memory>
#include <utility>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_base.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

namespace arrow {

// ----------------------------------------------------------------------
// RecordBatchBuilder

RecordBatchBuilder::RecordBatchBuilder(const std::shared_ptr<Schema>& schema,
                                       MemoryPool* pool, int64_t initial_capacity)
    : schema_(schema), initial_capacity_(initial_capacity), pool_(pool) {}

Result<std::unique_ptr<RecordBatchBuilder>> RecordBatchBuilder::Make(
    const std::shared_ptr<Schema>& schema, MemoryPool* pool) {
  return Make(schema, pool, kMinBuilderCapacity);
}

Result<std::unique_ptr<RecordBatchBuilder>> RecordBatchBuilder::Make(
    const std::shared_ptr<Schema>& schema, MemoryPool* pool, int64_t initial_capacity) {
  auto builder = std::unique_ptr<RecordBatchBuilder>(
      new RecordBatchBuilder(schema, pool, initial_capacity));
  RETURN_NOT_OK(builder->CreateBuilders());
  RETURN_NOT_OK(builder->InitBuilders());
  return builder;
}

Result<std::shared_ptr<RecordBatch>> RecordBatchBuilder::Flush(bool reset_builders) {
  std::vector<std::shared_ptr<Array>> fields;
  fields.resize(this->num_fields());

  int64_t length = 0;
  for (int i = 0; i < this->num_fields(); ++i) {
    RETURN_NOT_OK(raw_field_builders_[i]->Finish(&fields[i]));
    if (i > 0 && fields[i]->length() != length) {
      return Status::Invalid("All fields must be same length when calling Flush");
    }
    length = fields[i]->length();
  }

  // For certain types like dictionaries, types may not be fully
  // determined before we have flushed. Make sure that the RecordBatch
  // gets the correct types in schema.
  // See: #ARROW-9969
  std::vector<std::shared_ptr<Field>> schema_fields(schema_->fields());
  for (int i = 0; i < this->num_fields(); ++i) {
    if (!schema_fields[i]->type()->Equals(fields[i]->type())) {
      schema_fields[i] = schema_fields[i]->WithType(fields[i]->type());
    }
  }
  std::shared_ptr<Schema> schema =
      std::make_shared<Schema>(std::move(schema_fields), schema_->metadata());

  std::shared_ptr<RecordBatch> batch =
      RecordBatch::Make(std::move(schema), length, std::move(fields));

  if (reset_builders) {
    ARROW_RETURN_NOT_OK(InitBuilders());
  }

  return batch;
}

Result<std::shared_ptr<RecordBatch>> RecordBatchBuilder::Flush() { return Flush(true); }

void RecordBatchBuilder::SetInitialCapacity(int64_t capacity) {
  ARROW_CHECK_GT(capacity, 0) << "Initial capacity must be positive";
  initial_capacity_ = capacity;
}

Status RecordBatchBuilder::CreateBuilders() {
  field_builders_.resize(this->num_fields());
  raw_field_builders_.resize(this->num_fields());
  for (int i = 0; i < this->num_fields(); ++i) {
    RETURN_NOT_OK(MakeBuilder(pool_, schema_->field(i)->type(), &field_builders_[i]));
    raw_field_builders_[i] = field_builders_[i].get();
  }
  return Status::OK();
}

Status RecordBatchBuilder::InitBuilders() {
  for (int i = 0; i < this->num_fields(); ++i) {
    RETURN_NOT_OK(raw_field_builders_[i]->Reserve(initial_capacity_));
  }
  return Status::OK();
}

}  // namespace arrow
