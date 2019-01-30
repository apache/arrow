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

#include "gandiva/projector.h"

#include <memory>
#include <utility>
#include <vector>

#include "gandiva/cache.h"
#include "gandiva/expr_validator.h"
#include "gandiva/llvm_generator.h"
#include "gandiva/projector_cache_key.h"

namespace gandiva {

Projector::Projector(std::unique_ptr<LLVMGenerator> llvm_generator, SchemaPtr schema,
                     const FieldVector& output_fields,
                     std::shared_ptr<Configuration> configuration)
    : llvm_generator_(std::move(llvm_generator)),
      schema_(schema),
      output_fields_(output_fields),
      configuration_(configuration) {}

Status Projector::Make(SchemaPtr schema, const ExpressionVector& exprs,
                       std::shared_ptr<Projector>* projector) {
  return Projector::Make(schema, exprs, ConfigurationBuilder::DefaultConfiguration(),
                         projector);
}

Status Projector::Make(SchemaPtr schema, const ExpressionVector& exprs,
                       std::shared_ptr<Configuration> configuration,
                       std::shared_ptr<Projector>* projector) {
  ARROW_RETURN_IF(schema == nullptr, Status::Invalid("Schema cannot be null"));
  ARROW_RETURN_IF(exprs.empty(), Status::Invalid("Expressions cannot be empty"));
  ARROW_RETURN_IF(configuration == nullptr,
                  Status::Invalid("Configuration cannot be null"));

  // see if equivalent projector was already built
  static Cache<ProjectorCacheKey, std::shared_ptr<Projector>> cache;
  ProjectorCacheKey cache_key(schema, configuration, exprs);
  std::shared_ptr<Projector> cached_projector = cache.GetModule(cache_key);
  if (cached_projector != nullptr) {
    *projector = cached_projector;
    return Status::OK();
  }

  // Build LLVM generator, and generate code for the specified expressions
  std::unique_ptr<LLVMGenerator> llvm_gen;
  ARROW_RETURN_NOT_OK(LLVMGenerator::Make(configuration, &llvm_gen));

  // Run the validation on the expressions.
  // Return if any of the expression is invalid since
  // we will not be able to process further.
  ExprValidator expr_validator(llvm_gen->types(), schema);
  for (auto& expr : exprs) {
    ARROW_RETURN_NOT_OK(expr_validator.Validate(expr));
  }

  ARROW_RETURN_NOT_OK(llvm_gen->Build(exprs));

  // save the output field types. Used for validation at Evaluate() time.
  std::vector<FieldPtr> output_fields;
  output_fields.reserve(exprs.size());
  for (auto& expr : exprs) {
    output_fields.push_back(expr->result());
  }

  // Instantiate the projector with the completely built llvm generator
  *projector = std::shared_ptr<Projector>(
      new Projector(std::move(llvm_gen), schema, output_fields, configuration));
  cache.PutModule(cache_key, *projector);

  return Status::OK();
}

Status Projector::Evaluate(const arrow::RecordBatch& batch,
                           const ArrayDataVector& output_data_vecs) {
  ARROW_RETURN_NOT_OK(ValidateEvaluateArgsCommon(batch));
  ARROW_RETURN_IF(
      output_data_vecs.size() != output_fields_.size(),
      Status::Invalid("Number of output buffers must match number of fields"));

  int idx = 0;
  for (auto& array_data : output_data_vecs) {
    const auto output_field = output_fields_[idx];
    if (array_data == nullptr) {
      return Status::Invalid("Output array for field ", output_field->name(),
                             " should not be null");
    }

    ARROW_RETURN_NOT_OK(
        ValidateArrayDataCapacity(*array_data, *output_field, batch.num_rows()));
    ++idx;
  }

  return llvm_generator_->Execute(batch, output_data_vecs);
}

Status Projector::Evaluate(const arrow::RecordBatch& batch, arrow::MemoryPool* pool,
                           arrow::ArrayVector* output) {
  ARROW_RETURN_NOT_OK(ValidateEvaluateArgsCommon(batch));
  ARROW_RETURN_IF(output == nullptr, Status::Invalid("Output must be non-null."));
  ARROW_RETURN_IF(pool == nullptr, Status::Invalid("Memory pool must be non-null."));

  // Allocate the output data vecs.
  ArrayDataVector output_data_vecs;
  output_data_vecs.reserve(output_fields_.size());
  for (auto& field : output_fields_) {
    ArrayDataPtr output_data;

    ARROW_RETURN_NOT_OK(
        AllocArrayData(field->type(), batch.num_rows(), pool, &output_data));
    output_data_vecs.push_back(output_data);
  }

  // Execute the expression(s).
  ARROW_RETURN_NOT_OK(llvm_generator_->Execute(batch, output_data_vecs));

  // Create and return array arrays.
  output->clear();
  for (auto& array_data : output_data_vecs) {
    output->push_back(arrow::MakeArray(array_data));
  }

  return Status::OK();
}

// TODO : handle variable-len vectors
Status Projector::AllocArrayData(const DataTypePtr& type, int64_t num_records,
                                 arrow::MemoryPool* pool, ArrayDataPtr* array_data) {
  const auto* fw_type = dynamic_cast<const arrow::FixedWidthType*>(type.get());
  ARROW_RETURN_IF(fw_type == nullptr,
                  Status::Invalid("Unsupported output data type ", type));

  std::shared_ptr<arrow::Buffer> null_bitmap;
  int64_t bitmap_bytes = arrow::BitUtil::BytesForBits(num_records);
  ARROW_RETURN_NOT_OK(arrow::AllocateBuffer(pool, bitmap_bytes, &null_bitmap));

  std::shared_ptr<arrow::Buffer> data;
  int64_t data_len = arrow::BitUtil::BytesForBits(num_records * fw_type->bit_width());
  ARROW_RETURN_NOT_OK(arrow::AllocateBuffer(pool, data_len, &data));

  // This is not strictly required but valgrind gets confused and detects this
  // as uninitialized memory access. See arrow::util::SetBitTo().
  if (type->id() == arrow::Type::BOOL) {
    memset(data->mutable_data(), 0, data_len);
  }

  *array_data = arrow::ArrayData::Make(type, num_records, {null_bitmap, data});
  return Status::OK();
}

Status Projector::ValidateEvaluateArgsCommon(const arrow::RecordBatch& batch) {
  ARROW_RETURN_IF(!batch.schema()->Equals(*schema_),
                  Status::Invalid("Schema in RecordBatch must match schema in Make()"));
  ARROW_RETURN_IF(batch.num_rows() == 0,
                  Status::Invalid("RecordBatch must be non-empty."));

  return Status::OK();
}

Status Projector::ValidateArrayDataCapacity(const arrow::ArrayData& array_data,
                                            const arrow::Field& field,
                                            int64_t num_records) {
  ARROW_RETURN_IF(array_data.buffers.size() < 2,
                  Status::Invalid("ArrayData must have at least 2 buffers"));

  int64_t min_bitmap_len = arrow::BitUtil::BytesForBits(num_records);
  int64_t bitmap_len = array_data.buffers[0]->capacity();
  ARROW_RETURN_IF(bitmap_len < min_bitmap_len,
                  Status::Invalid("Bitmap buffer too small for ", field.name()));

  // verify size of data buffer.
  // TODO : handle variable-len vectors
  const auto& fw_type = dynamic_cast<const arrow::FixedWidthType&>(*field.type());
  int64_t min_data_len = arrow::BitUtil::BytesForBits(num_records * fw_type.bit_width());
  int64_t data_len = array_data.buffers[1]->capacity();
  ARROW_RETURN_IF(data_len < min_data_len,
                  Status::Invalid("Data buffer too small for ", field.name()));

  return Status::OK();
}

}  // namespace gandiva
