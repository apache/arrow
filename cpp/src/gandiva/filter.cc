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

#include "gandiva/filter.h"

#include <memory>
#include <utility>
#include <vector>

#include "gandiva/bitmap_accumulator.h"
#include "gandiva/cache.h"
#include "gandiva/condition.h"
#include "gandiva/expr_validator.h"
#include "gandiva/filter_cache_key.h"
#include "gandiva/llvm_generator.h"
#include "gandiva/selection_vector_impl.h"

namespace gandiva {

Filter::Filter(std::unique_ptr<LLVMGenerator> llvm_generator, SchemaPtr schema,
               std::shared_ptr<Configuration> configuration)
    : llvm_generator_(std::move(llvm_generator)),
      schema_(schema),
      configuration_(configuration) {}

Filter::~Filter() {}

Status Filter::Make(SchemaPtr schema, ConditionPtr condition,
                    std::shared_ptr<Configuration> configuration,
                    std::shared_ptr<Filter>* filter) {
  ARROW_RETURN_IF(schema == nullptr, Status::Invalid("Schema cannot be null"));
  ARROW_RETURN_IF(condition == nullptr, Status::Invalid("Condition cannot be null"));
  ARROW_RETURN_IF(configuration == nullptr,
                  Status::Invalid("Configuration cannot be null"));

  static Cache<FilterCacheKey, std::shared_ptr<Filter>> cache;
  FilterCacheKey cache_key(schema, configuration, *(condition.get()));
  auto cachedFilter = cache.GetModule(cache_key);
  if (cachedFilter != nullptr) {
    *filter = cachedFilter;
    return Status::OK();
  }

  // Build LLVM generator, and generate code for the specified expression
  std::unique_ptr<LLVMGenerator> llvm_gen;
  ARROW_RETURN_NOT_OK(LLVMGenerator::Make(configuration, &llvm_gen));

  // Run the validation on the expression.
  // Return if the expression is invalid since we will not be able to process further.
  ExprValidator expr_validator(llvm_gen->types(), schema);
  ARROW_RETURN_NOT_OK(expr_validator.Validate(condition));
  ARROW_RETURN_NOT_OK(llvm_gen->Build({condition}, SelectionVector::Mode::MODE_NONE));

  // Instantiate the filter with the completely built llvm generator
  *filter = std::make_shared<Filter>(std::move(llvm_gen), schema, configuration);
  cache.PutModule(cache_key, *filter);

  return Status::OK();
}

Status Filter::Evaluate(const arrow::RecordBatch& batch,
                        std::shared_ptr<SelectionVector> out_selection) {
  const auto num_rows = batch.num_rows();
  ARROW_RETURN_IF(!batch.schema()->Equals(*schema_),
                  Status::Invalid("RecordBatch schema must expected filter schema"));
  ARROW_RETURN_IF(num_rows == 0, Status::Invalid("RecordBatch must be non-empty."));
  ARROW_RETURN_IF(out_selection == nullptr,
                  Status::Invalid("out_selection must be non-null."));
  ARROW_RETURN_IF(out_selection->GetMaxSlots() < num_rows,
                  Status::Invalid("Output selection vector capacity too small"));

  // Allocate three local_bitmaps (one for output, one for validity, one to compute the
  // intersection).
  LocalBitMapsHolder bitmaps(num_rows, 3 /*local_bitmaps*/);
  int64_t bitmap_size = bitmaps.GetLocalBitMapSize();

  auto validity = std::make_shared<arrow::Buffer>(bitmaps.GetLocalBitMap(0), bitmap_size);
  auto value = std::make_shared<arrow::Buffer>(bitmaps.GetLocalBitMap(1), bitmap_size);
  auto array_data = arrow::ArrayData::Make(arrow::boolean(), num_rows, {validity, value});

  // Execute the expression(s).
  ARROW_RETURN_NOT_OK(llvm_generator_->Execute(batch, {array_data}));

  // Compute the intersection of the value and validity.
  auto result = bitmaps.GetLocalBitMap(2);
  BitMapAccumulator::IntersectBitMaps(
      result, {bitmaps.GetLocalBitMap(0), bitmaps.GetLocalBitMap((1))}, {0, 0}, num_rows);

  return out_selection->PopulateFromBitMap(result, bitmap_size, num_rows - 1);
}

}  // namespace gandiva
