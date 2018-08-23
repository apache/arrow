// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "gandiva/projector.h"

#include <memory>
#include <utility>
#include <vector>

#include "codegen/cache.h"
#include "codegen/expr_validator.h"
#include "codegen/llvm_generator.h"
#include "codegen/projector_cache_key.h"
#include "gandiva/status.h"

namespace gandiva {

Projector::Projector(std::unique_ptr<LLVMGenerator> llvm_generator, SchemaPtr schema,
                     const FieldVector &output_fields,
                     std::shared_ptr<Configuration> configuration)
    : llvm_generator_(std::move(llvm_generator)),
      schema_(schema),
      output_fields_(output_fields),
      configuration_(configuration) {}

Status Projector::Make(SchemaPtr schema, const ExpressionVector &exprs,
                       std::shared_ptr<Projector> *projector) {
  return Projector::Make(schema, exprs, ConfigurationBuilder::DefaultConfiguration(),
                         projector);
}

Status Projector::Make(SchemaPtr schema, const ExpressionVector &exprs,
                       std::shared_ptr<Configuration> configuration,
                       std::shared_ptr<Projector> *projector) {
  GANDIVA_RETURN_FAILURE_IF_FALSE(schema != nullptr,
                                  Status::Invalid("schema cannot be null"));
  GANDIVA_RETURN_FAILURE_IF_FALSE(!exprs.empty(),
                                  Status::Invalid("expressions need to be non-empty"));
  GANDIVA_RETURN_FAILURE_IF_FALSE(configuration != nullptr,
                                  Status::Invalid("configuration cannot be null"));

  // see if equivalent projector was already built
  static Cache<ProjectorCacheKey, std::shared_ptr<Projector>> cache;
  ProjectorCacheKey cache_key(schema, configuration, exprs);
  std::shared_ptr<Projector> cached_projector = cache.GetCachedModule(cache_key);
  if (cached_projector != nullptr) {
    *projector = cached_projector;
    return Status::OK();
  }
  // Build LLVM generator, and generate code for the specified expressions
  std::unique_ptr<LLVMGenerator> llvm_gen;
  Status status = LLVMGenerator::Make(configuration, &llvm_gen);
  GANDIVA_RETURN_NOT_OK(status);

  // Run the validation on the expressions.
  // Return if any of the expression is invalid since
  // we will not be able to process further.
  ExprValidator expr_validator(llvm_gen->types(), schema);
  for (auto &expr : exprs) {
    status = expr_validator.Validate(expr);
    GANDIVA_RETURN_NOT_OK(status);
  }

  status = llvm_gen->Build(exprs);
  GANDIVA_RETURN_NOT_OK(status);

  // save the output field types. Used for validation at Evaluate() time.
  std::vector<FieldPtr> output_fields;
  for (auto &expr : exprs) {
    output_fields.push_back(expr->result());
  }

  // Instantiate the projector with the completely built llvm generator
  *projector = std::shared_ptr<Projector>(
      new Projector(std::move(llvm_gen), schema, output_fields, configuration));
  cache.CacheModule(cache_key, *projector);
  return Status::OK();
}

Status Projector::Evaluate(const arrow::RecordBatch &batch,
                           const ArrayDataVector &output_data_vecs) {
  Status status = ValidateEvaluateArgsCommon(batch);
  GANDIVA_RETURN_NOT_OK(status);

  if (output_data_vecs.size() != output_fields_.size()) {
    std::stringstream ss;
    ss << "number of buffers for output_data_vecs is " << output_data_vecs.size()
       << ", expected " << output_fields_.size();
    return Status::Invalid(ss.str());
  }

  int idx = 0;
  for (auto &array_data : output_data_vecs) {
    if (array_data == nullptr) {
      std::stringstream ss;
      ss << "array for output field " << output_fields_[idx]->name() << "is null.";
      return Status::Invalid(ss.str());
    }

    Status status =
        ValidateArrayDataCapacity(*array_data, *(output_fields_[idx]), batch.num_rows());
    GANDIVA_RETURN_NOT_OK(status);
    ++idx;
  }
  return llvm_generator_->Execute(batch, output_data_vecs);
}

Status Projector::Evaluate(const arrow::RecordBatch &batch, arrow::MemoryPool *pool,
                           arrow::ArrayVector *output) {
  Status status = ValidateEvaluateArgsCommon(batch);
  GANDIVA_RETURN_NOT_OK(status);

  if (output == nullptr) {
    return Status::Invalid("output must be non-null.");
  }

  if (pool == nullptr) {
    return Status::Invalid("memory pool must be non-null.");
  }

  // Allocate the output data vecs.
  ArrayDataVector output_data_vecs;
  for (auto &field : output_fields_) {
    ArrayDataPtr output_data;

    status = AllocArrayData(field->type(), batch.num_rows(), pool, &output_data);
    GANDIVA_RETURN_NOT_OK(status);

    output_data_vecs.push_back(output_data);
  }

  // Execute the expression(s).
  status = llvm_generator_->Execute(batch, output_data_vecs);
  GANDIVA_RETURN_NOT_OK(status);

  // Create and return array arrays.
  output->clear();
  for (auto &array_data : output_data_vecs) {
    output->push_back(arrow::MakeArray(array_data));
  }
  return Status::OK();
}

// TODO : handle variable-len vectors
Status Projector::AllocArrayData(const DataTypePtr &type, int num_records,
                                 arrow::MemoryPool *pool, ArrayDataPtr *array_data) {
  if (!arrow::is_primitive(type->id())) {
    return Status::Invalid("Unsupported output data type " + type->ToString());
  }

  arrow::Status astatus;
  std::shared_ptr<arrow::Buffer> null_bitmap;
  int64_t size = arrow::BitUtil::BytesForBits(num_records);
  astatus = arrow::AllocateBuffer(pool, size, &null_bitmap);
  GANDIVA_RETURN_ARROW_NOT_OK(astatus);

  std::shared_ptr<arrow::Buffer> data;
  const auto &fw_type = dynamic_cast<const arrow::FixedWidthType &>(*type);
  int64_t data_len = arrow::BitUtil::BytesForBits(num_records * fw_type.bit_width());
  astatus = arrow::AllocateBuffer(pool, data_len, &data);
  GANDIVA_RETURN_ARROW_NOT_OK(astatus);

  *array_data = arrow::ArrayData::Make(type, num_records, {null_bitmap, data});
  return Status::OK();
}

Status Projector::ValidateEvaluateArgsCommon(const arrow::RecordBatch &batch) {
  if (!batch.schema()->Equals(*schema_)) {
    return Status::Invalid("Schema in RecordBatch must match the schema in Make()");
  }
  if (batch.num_rows() == 0) {
    return Status::Invalid("RecordBatch must be non-empty.");
  }
  return Status::OK();
}

Status Projector::ValidateArrayDataCapacity(const arrow::ArrayData &array_data,
                                            const arrow::Field &field, int num_records) {
  // verify that there are atleast two buffers (validity and data).
  if (array_data.buffers.size() < 2) {
    std::stringstream ss;
    ss << "number of buffers for output field " << field.name() << "is "
       << array_data.buffers.size() << ", must have minimum 2.";
    return Status::Invalid(ss.str());
  }

  // verify size of bitmap buffer.
  int64_t min_bitmap_len = arrow::BitUtil::BytesForBits(num_records);
  int64_t bitmap_len = array_data.buffers[0]->capacity();
  if (bitmap_len < min_bitmap_len) {
    std::stringstream ss;
    ss << "bitmap buffer for output field " << field.name() << "has size " << bitmap_len
       << ", must have minimum size " << min_bitmap_len;
    return Status::Invalid(ss.str());
  }

  // verify size of data buffer.
  // TODO : handle variable-len vectors
  const auto &fw_type = dynamic_cast<const arrow::FixedWidthType &>(*field.type());
  int64_t min_data_len = arrow::BitUtil::BytesForBits(num_records * fw_type.bit_width());
  int64_t data_len = array_data.buffers[1]->capacity();
  if (data_len < min_data_len) {
    std::stringstream ss;
    ss << "data buffer for output field " << field.name() << "has size " << data_len
       << ", must have minimum size " << min_data_len;
    return Status::Invalid(ss.str());
  }
  return Status::OK();
}

}  // namespace gandiva
