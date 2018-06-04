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
#ifndef GANDIVA_EXPR_PROJECTOR_H
#define GANDIVA_EXPR_PROJECTOR_H

#include <memory>
#include <utility>
#include <vector>
#include "gandiva/arrow.h"
#include "gandiva/expression.h"
#include "gandiva/status.h"

namespace gandiva {

class LLVMGenerator;

/// \brief projection using expressions.
///
/// A projector is built for a specific schema and vector of expressions.
/// Once the projector is built, it can be used to evaluate many row batches.
class Projector {
 public:
  /// Build a projector for the given schema to evaluate the vector of expressions.
  static Status Make(SchemaPtr schema,
                     const ExpressionVector &exprs,
                     arrow::MemoryPool *pool,
                     std::shared_ptr<Projector> *projector);

  /// Evaluate the specified record batch, and fill the output vectors.
  /// TODO : need a zero-copy variant if the caller can alloc the output vectors.
  arrow::ArrayVector Evaluate(const arrow::RecordBatch &batch);

 private:
  Projector(std::unique_ptr<LLVMGenerator> llvm_generator,
            SchemaPtr schema,
            const FieldVector &output_fields,
            arrow::MemoryPool *pool);

  /// Allocate an arrow array of length 'length'.
  ArrayPtr AllocArray(DataTypePtr type, int length);

  const std::unique_ptr<LLVMGenerator> llvm_generator_;
  const SchemaPtr schema_;
  const FieldVector output_fields_;
  arrow::MemoryPool *pool_;
};

} // namespace gandiva

#endif // GANDIVA_EXPR_PROJECTOR_H
