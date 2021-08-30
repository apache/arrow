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
#include <thread>
#include <utility>
#include <vector>

#include "arrow/util/hash_util.h"

#include "gandiva/base_cache_key.h"
#include "gandiva/bitmap_accumulator.h"
#include "gandiva/cache.h"
#include "gandiva/condition.h"
#include "gandiva/expr_validator.h"
#include "gandiva/llvm_generator.h"
#include "gandiva/selection_vector_impl.h"

namespace gandiva {

FilterCacheKey::FilterCacheKey(SchemaPtr schema,
                               std::shared_ptr<Configuration> configuration,
                               Expression& expression)
    : schema_(schema), configuration_(configuration), uniqifier_(0) {
  static const int kSeedValue = 4;
  size_t result = kSeedValue;
  expression_as_string_ = expression.ToString();
  UpdateUniqifier(expression_as_string_);
  arrow::internal::hash_combine(result, expression_as_string_);
  arrow::internal::hash_combine(result, configuration->Hash());
  arrow::internal::hash_combine(result, schema_->ToString());
  arrow::internal::hash_combine(result, uniqifier_);
  hash_code_ = result;
}

bool FilterCacheKey::operator==(const FilterCacheKey& other) const {
  // arrow schema does not overload equality operators.
  if (!(schema_->Equals(*other.schema().get(), true))) {
    return false;
  }

  if (configuration_ != other.configuration_) {
    return false;
  }

  if (expression_as_string_ != other.expression_as_string_) {
    return false;
  }

  if (uniqifier_ != other.uniqifier_) {
    return false;
  }
  return true;
}

std::string FilterCacheKey::ToString() const {
  std::stringstream ss;
  // indent, window, indent_size, null_rep and skip new lines.
  arrow::PrettyPrintOptions options{0, 10, 2, "null", true};
  DCHECK_OK(PrettyPrint(*schema_.get(), options, &ss));

  ss << "Condition: [" << expression_as_string_ << "]";
  return ss.str();
}

void FilterCacheKey::UpdateUniqifier(const std::string& expr) {
  // caching of expressions with re2 patterns causes lock contention. So, use
  // multiple instances to reduce contention.
  if (expr.find(" like(") != std::string::npos) {
    uniqifier_ = std::hash<std::thread::id>()(std::this_thread::get_id()) % 16;
  }
}

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

  std::shared_ptr<Cache<BaseCacheKey, std::shared_ptr<llvm::MemoryBuffer>>> shared_cache =
      LLVMGenerator::GetCache();

  Condition conditionToKey = *(condition.get());

  FilterCacheKey filter_key(schema, configuration, conditionToKey);
  BaseCacheKey cache_key(filter_key, "filter");
  std::unique_ptr<BaseCacheKey> base_cache_key =
      std::make_unique<BaseCacheKey>(cache_key);
  std::shared_ptr<BaseCacheKey> shared_base_cache_key = std::move(base_cache_key);

  bool llvm_flag = false;

  std::shared_ptr<llvm::MemoryBuffer> prev_cached_obj;
  prev_cached_obj = shared_cache->GetObjectCode(*shared_base_cache_key);

  // Verify if previous filter obj code was cached
  if (prev_cached_obj != nullptr) {
    ARROW_LOG(DEBUG) << "[DEBUG][FILTER-CACHE-LOG]: Object code WAS already cached!";
    llvm_flag = true;
  }

  GandivaObjectCache<BaseCacheKey> obj_cache(shared_cache, shared_base_cache_key);

  // Build LLVM generator, and generate code for the specified expression
  std::unique_ptr<LLVMGenerator> llvm_gen;
  ARROW_RETURN_NOT_OK(LLVMGenerator::Make(configuration, &llvm_gen));

  // Run the validation on the expression.
  // Return if the expression is invalid since we will not be able to process further.
  ExprValidator expr_validator(llvm_gen->types(), schema);
  ARROW_RETURN_NOT_OK(expr_validator.Validate(condition));

  // Start measuring build time
  auto begin = std::chrono::high_resolution_clock::now();
  ARROW_RETURN_NOT_OK(llvm_gen->Build({condition}, SelectionVector::Mode::MODE_NONE));
  // Stop measuring time and calculate the elapsed time
  auto end = std::chrono::high_resolution_clock::now();
  auto elapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
//  ARROW_RETURN_NOT_OK(llvm_gen->Build({condition}, SelectionVector::Mode::MODE_NONE, obj_cache)); // to use when caching only the obj code

  // Instantiate the filter with the completely built llvm generator
  *filter = std::make_shared<Filter>(std::move(llvm_gen), schema, configuration);
  ValueCacheObject<std::shared_ptr<Filter>> value_cache(*filter, elapsed);
//  cache.PutModule(cache_key, value_cache);
//
//  filter->get()->SetCompiledFromCache(llvm_flag); // to use when caching only the obj code
//  used_cache_size_ = shared_cache->getCacheSize(); // track filter cache memory use
//
//  ARROW_LOG(DEBUG) << "[DEBUG][FILTER-CACHE-LOG] " + shared_cache->toString(); // to use when caching only the obj code

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

std::string Filter::DumpIR() { return llvm_generator_->DumpIR(); }

void Filter::SetCompiledFromCache(bool flag) { compiled_from_cache_ = flag; }

bool Filter::GetCompiledFromCache() { return compiled_from_cache_; }

size_t Filter::GetUsedCacheSize() { return used_cache_size_; }

size_t Filter::used_cache_size_ = 0;

}  // namespace gandiva
