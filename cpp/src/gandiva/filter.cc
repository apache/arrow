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

#include "gandiva/filter.h"

#include <memory>
#include <utility>
#include <vector>

#include "codegen/bitmap_accumulator.h"
#include "codegen/cache.h"
#include "codegen/expr_validator.h"
#include "codegen/filter_cache_key.h"
#include "codegen/llvm_generator.h"
#include "codegen/selection_vector_impl.h"
#include "gandiva/condition.h"
#include "gandiva/status.h"

namespace gandiva {

Filter::Filter(std::unique_ptr<LLVMGenerator> llvm_generator, SchemaPtr schema,
               std::shared_ptr<Configuration> configuration)
    : llvm_generator_(std::move(llvm_generator)),
      schema_(schema),
      configuration_(configuration) {}

Status Filter::Make(SchemaPtr schema, ConditionPtr condition,
                    std::shared_ptr<Configuration> configuration,
                    std::shared_ptr<Filter> *filter) {
  GANDIVA_RETURN_FAILURE_IF_FALSE(schema != nullptr,
                                  Status::Invalid("schema cannot be null"));
  GANDIVA_RETURN_FAILURE_IF_FALSE(condition != nullptr,
                                  Status::Invalid("condition cannot be null"));
  GANDIVA_RETURN_FAILURE_IF_FALSE(configuration != nullptr,
                                  Status::Invalid("configuration cannot be null"));
  static Cache<FilterCacheKey, std::shared_ptr<Filter>> cache;
  FilterCacheKey cacheKey(schema, configuration, *(condition.get()));
  std::shared_ptr<Filter> cachedFilter = cache.GetModule(cacheKey);
  if (cachedFilter != nullptr) {
    *filter = cachedFilter;
    return Status::OK();
  }
  // Build LLVM generator, and generate code for the specified expression
  std::unique_ptr<LLVMGenerator> llvm_gen;
  Status status = LLVMGenerator::Make(configuration, &llvm_gen);
  GANDIVA_RETURN_NOT_OK(status);

  // Run the validation on the expression.
  // Return if the expression is invalid since we will not be able to process further.
  ExprValidator expr_validator(llvm_gen->types(), schema);
  status = expr_validator.Validate(condition);
  GANDIVA_RETURN_NOT_OK(status);

  status = llvm_gen->Build({condition});
  GANDIVA_RETURN_NOT_OK(status);

  // Instantiate the filter with the completely built llvm generator
  *filter = std::make_shared<Filter>(std::move(llvm_gen), schema, configuration);
  cache.PutModule(cacheKey, *filter);
  return Status::OK();
}

Status Filter::Evaluate(const arrow::RecordBatch &batch,
                        std::shared_ptr<SelectionVector> out_selection) {
  if (!batch.schema()->Equals(*schema_)) {
    return Status::Invalid("Schema in RecordBatch must match the schema in Make()");
  }
  if (batch.num_rows() == 0) {
    return Status::Invalid("RecordBatch must be non-empty.");
  }
  if (out_selection == nullptr) {
    return Status::Invalid("out_selection must be non-null.");
  }
  if (out_selection->GetMaxSlots() < batch.num_rows()) {
    std::stringstream ss;
    ss << "out_selection has " << out_selection->GetMaxSlots()
       << " slots, which is less than the batch size " << batch.num_rows();
    return Status::Invalid(ss.str());
  }

  // Allocate three local_bitmaps (one for output, one for validity, one to compute the
  // intersection).
  LocalBitMapsHolder bitmaps(batch.num_rows(), 3 /*local_bitmaps*/);
  int bitmap_size = bitmaps.GetLocalBitMapSize();

  auto validity = std::make_shared<arrow::Buffer>(bitmaps.GetLocalBitMap(0), bitmap_size);
  auto value = std::make_shared<arrow::Buffer>(bitmaps.GetLocalBitMap(1), bitmap_size);
  auto array_data =
      arrow::ArrayData::Make(arrow::boolean(), batch.num_rows(), {validity, value});

  // Execute the expression(s).
  auto status = llvm_generator_->Execute(batch, {array_data});
  GANDIVA_RETURN_NOT_OK(status);

  // Compute the intersection of the value and validity.
  auto result = bitmaps.GetLocalBitMap(2);
  BitMapAccumulator::IntersectBitMaps(
      result, {bitmaps.GetLocalBitMap(0), bitmaps.GetLocalBitMap((1))}, batch.num_rows());

  return out_selection->PopulateFromBitMap(result, bitmap_size, batch.num_rows() - 1);
}

}  // namespace gandiva
