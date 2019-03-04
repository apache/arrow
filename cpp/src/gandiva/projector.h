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

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/status.h"

#include "gandiva/arrow.h"
#include "gandiva/configuration.h"
#include "gandiva/expression.h"
#include "gandiva/selection_vector.h"
#include "gandiva/visibility.h"

namespace gandiva {

class LLVMGenerator;

/// \brief projection using expressions.
///
/// A projector is built for a specific schema and vector of expressions.
/// Once the projector is built, it can be used to evaluate many row batches.
class GANDIVA_EXPORT Projector {
 public:
  // Inline dtor will attempt to resolve the destructor for
  // LLVMGenerator on MSVC, so we compile the dtor in the object code
  ~Projector();

  /// Build a default projector for the given schema to evaluate
  /// the vector of expressions.
  ///
  /// \param[in] schema schema for the record batches, and the expressions.
  /// \param[in] exprs vector of expressions.
  /// \param[out] projector the returned projector object
  static Status Make(SchemaPtr schema, const ExpressionVector& exprs,
                     std::shared_ptr<Projector>* projector);

  /// Build a projector for the given schema to evaluate the vector of expressions.
  /// Customize the projector with runtime configuration.
  ///
  /// \param[in] schema schema for the record batches, and the expressions.
  /// \param[in] exprs vector of expressions.
  /// \param[in] configuration run time configuration.
  /// \param[out] projector the returned projector object
  static Status Make(SchemaPtr schema, const ExpressionVector& exprs,
                     std::shared_ptr<Configuration> configuration,
                     std::shared_ptr<Projector>* projector);

  /// Evaluate the specified record batch, and return the allocated and populated output
  /// arrays. The output arrays will be allocated from the memory pool 'pool', and added
  /// to the vector 'output'.
  ///
  /// \param[in] batch the record batch. schema should be the same as the one in 'Make'
  /// \param[in] pool memory pool used to allocate output arrays (if required).
  /// \param[out] output the vector of allocated/populated arrays.
  Status Evaluate(const arrow::RecordBatch& batch, arrow::MemoryPool* pool,
                  arrow::ArrayVector* output);

  /// Evaluate the specified record batch, and populate the output arrays. The output
  /// arrays of sufficient capacity must be allocated by the caller.
  ///
  /// \param[in] batch the record batch. schema should be the same as the one in 'Make'
  /// \param[in,out] output vector of arrays, the arrays are allocated by the caller and
  ///                populated by Evaluate.
  Status Evaluate(const arrow::RecordBatch& batch, const ArrayDataVector& output);

  /// Evaluate the specified record batch, and return the allocated and populated output
  /// arrays. The output arrays will be allocated from the memory pool 'pool', and added
  /// to the vector 'output'.
  ///
  /// \param[in] batch the record batch. schema should be the same as the one in 'Make'
  /// \param[in] selection_vector selection vector which has filtered row posisitons.
  /// \param[in] pool memory pool used to allocate output arrays (if required).
  /// \param[out] output the vector of allocated/populated arrays.
  Status Evaluate(const arrow::RecordBatch& batch,
                  const SelectionVector* selection_vector, arrow::MemoryPool* pool,
                  arrow::ArrayVector* output);

  /// Evaluate the specified record batch, and populate the output arrays at the filtered
  /// positions. The output arrays of sufficient capacity must be allocated by the caller.
  ///
  /// \param[in] batch the record batch. schema should be the same as the one in 'Make'
  /// \param[in] selection_vector selection vector which has the filtered row posisitons
  /// \param[in,out] output vector of arrays, the arrays are allocated by the caller and
  ///                 populated by Evaluate.
  Status Evaluate(const arrow::RecordBatch& batch,
                  const SelectionVector* selection_vector, const ArrayDataVector& output);

 private:
  Projector(std::unique_ptr<LLVMGenerator> llvm_generator, SchemaPtr schema,
            const FieldVector& output_fields, std::shared_ptr<Configuration>);

  /// Allocate an ArrowData of length 'length'.
  Status AllocArrayData(const DataTypePtr& type, int64_t length, arrow::MemoryPool* pool,
                        ArrayDataPtr* array_data);

  /// Validate that the ArrayData has sufficient capacity to accomodate 'num_records'.
  Status ValidateArrayDataCapacity(const arrow::ArrayData& array_data,
                                   const arrow::Field& field, int64_t num_records);

  /// Validate the common args for Evaluate() APIs.
  Status ValidateEvaluateArgsCommon(const arrow::RecordBatch& batch);

  const std::unique_ptr<LLVMGenerator> llvm_generator_;
  const SchemaPtr schema_;
  const FieldVector output_fields_;
  const std::shared_ptr<Configuration> configuration_;
};

}  // namespace gandiva
