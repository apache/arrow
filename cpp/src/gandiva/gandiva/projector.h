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
  ///
  /// \param[in] : schema schema for the record batches, and the expressions.
  /// \param[in] : exprs vector of expressions.
  /// \param[in] : pool memory pool used to allocate output arrays (if required).
  /// \param[out]: projector the returned projector object
  static Status Make(SchemaPtr schema,
                     const ExpressionVector &exprs,
                     arrow::MemoryPool *pool,
                     std::shared_ptr<Projector> *projector);

  /// Evaluate the specified record batch, and return the allocated and populated output
  /// arrays. The output arrays will be allocated from the memory pool 'pool', and added
  /// to the vector 'output'.
  ///
  /// \param[in] : batch the record batch. schema should be the same as the one in 'Make'
  /// \param[out]: output the vector of allocated/populated arrays.
  Status Evaluate(const arrow::RecordBatch &batch,
                  arrow::ArrayVector *ouput);

  /// Evaluate the specified record batch, and populate the output arrays. The output
  /// arrays of sufficient capacity must be allocated by the caller.
  ///
  /// \param[in] : batch the record batch. schema should be the same as the one in 'Make'
  /// \param[in/out]: vector of arrays, the arrays are allocated by the caller and
  ///                 populated by Evaluate.
  Status Evaluate(const arrow::RecordBatch &batch,
                  const ArrayDataVector &output);

 private:
  Projector(std::unique_ptr<LLVMGenerator> llvm_generator,
            SchemaPtr schema,
            const FieldVector &output_fields,
            arrow::MemoryPool *pool);

  /// Allocate an ArrowData of length 'length'.
  Status AllocArrayData(const DataTypePtr &type,
                        int length,
                        ArrayDataPtr *array_data);

  /// Validate that the ArrayData has sufficient capacity to accomodate 'num_records'.
  Status ValidateArrayDataCapacity(const arrow::ArrayData &array_data,
                                   const arrow::Field &field,
                                   int num_records);

  /// Validate the common args for Evaluate() APIs.
  Status ValidateEvaluateArgsCommon(const arrow::RecordBatch &batch);

  const std::unique_ptr<LLVMGenerator> llvm_generator_;
  const SchemaPtr schema_;
  const FieldVector output_fields_;
  arrow::MemoryPool *pool_;
};

} // namespace gandiva

#endif // GANDIVA_EXPR_PROJECTOR_H
