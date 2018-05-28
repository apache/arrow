/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <memory>
#include <vector>
#include <utility>
#include "expr/evaluator.h"

namespace gandiva {

// TODO : exceptions
std::shared_ptr<Evaluator> Evaluator::Make(SchemaPtr schema,
                                           const ExpressionVector &exprs,
                                           arrow::MemoryPool *pool) {
  // TODO: validate schema
  // TODO : validate expressions (fields, function signatures, output types, ..)

  // Build LLVM generator, and generate code for the specified expressions
  std::unique_ptr<LLVMGenerator> llvm_gen(new LLVMGenerator());
  llvm_gen->Build(exprs);

  // save the output field types. Used for validation at Evaluate() time.
  std::vector<FieldPtr> output_fields;
  for (auto it = exprs.begin(); it != exprs.end(); ++it) {
    output_fields.push_back((*it)->result());
  }

  // Instantiate the evaluator with the completely built llvm generator
  return std::shared_ptr<Evaluator>(new Evaluator(std::move(llvm_gen),
                                                  schema,
                                                  output_fields,
                                                  pool));
}

arrow::ArrayVector Evaluator::Evaluate(const arrow::RecordBatch &batch) {
  DCHECK_EQ(batch.schema(), schema_);
  DCHECK_GT(batch.num_rows(), 0);

  arrow::ArrayVector outputs;
  for (auto it = output_fields_.begin(); it != output_fields_.end(); ++it) {
    auto field = *it;
    auto output = AllocArray(field->type(), batch.num_rows());
    outputs.push_back(output);
  }
  llvm_generator_->Execute(batch, outputs);
  return outputs;
}

// TODO : handle variable-len vectors
ArrayPtr Evaluator::AllocArray(DataTypePtr type, int length) {
  arrow::Status status;

  auto null_bitmap = std::make_shared<arrow::PoolBuffer>(pool_);
  status = null_bitmap->Resize(arrow::BitUtil::BytesForBits(length));
  DCHECK(status.ok());

  auto data = std::make_shared<arrow::PoolBuffer>(pool_);
  const auto &fw_type = dynamic_cast<const arrow::FixedWidthType&>(*type);
  status = data->Resize(((length * fw_type.bit_width()) + 7) / 8);
  DCHECK(status.ok());

  auto array_data = arrow::ArrayData::Make(type, length, { null_bitmap, data });
  return arrow::MakeArray(array_data);
}

} // namespace gandiva
