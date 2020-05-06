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

#include "arrow/compute/api_eager.h"

#include <sstream>
#include <string>
#include <utility>

#include "arrow/compute/exec.h"

namespace arrow {
namespace compute {

#define SCALAR_EAGER_UNARY(NAME, REGISTRY_NAME)              \
  Result<Datum> NAME(const Datum& value, ExecContext* ctx) { \
    return ExecScalarFunction(ctx, REGISTRY_NAME, {value});  \
  }

#define SCALAR_EAGER_BINARY(NAME, REGISTRY_NAME)                                \
  Result<Datum> NAME(const Datum& left, const Datum& right, ExecContext* ctx) { \
    return ExecScalarFunction(ctx, REGISTRY_NAME, {left, right});               \
  }

// ----------------------------------------------------------------------
// Arithmetic

SCALAR_EAGER_BINARY(Add, "add")

// ----------------------------------------------------------------------
// Set-related operations

static Result<Datum> ExecSetLookup(const std::string& func_name, const Datum& data,
                                   std::shared_ptr<Array> value_set,
                                   bool add_nulls_to_hash_table, ExecContext* ctx) {
  if (value_set->length() > 0 && !data.type()->Equals(value_set->type())) {
    std::stringstream ss;
    ss << "Array type didn't match type of values set: " << data.type()->ToString()
       << " vs " << value_set->type()->ToString();
    return Status::Invalid(ss.str());
  }
  SetLookupOptions options(std::move(value_set), !add_nulls_to_hash_table);
  return ExecScalarFunction(ctx, func_name, {data}, &options);
}

Result<Datum> IsIn(const Datum& values, std::shared_ptr<Array> value_set,
                   ExecContext* ctx) {
  return ExecSetLookup("isin", values, std::move(value_set),
                       /*add_nulls_to_hash_table=*/false, ctx);
}

Result<Datum> Match(const Datum& values, std::shared_ptr<Array> value_set,
                    ExecContext* ctx) {
  return ExecSetLookup("match", values, std::move(value_set),
                       /*add_nulls_to_hash_table=*/true, ctx);
}

// ----------------------------------------------------------------------
// Boolean functions

SCALAR_EAGER_UNARY(Invert, "invert")
SCALAR_EAGER_BINARY(And, "and")
SCALAR_EAGER_BINARY(KleeneAnd, "and_kleene")
SCALAR_EAGER_BINARY(Or, "or")
SCALAR_EAGER_BINARY(KleeneOr, "or_kleene")
SCALAR_EAGER_BINARY(Xor, "xor")

// ----------------------------------------------------------------------

Result<Datum> Compare(const Datum& left, const Datum& right, CompareOptions options,
                      ExecContext* ctx) {
  std::string func_name;
  switch (options.op) {
    case CompareOperator::EQUAL:
      func_name = "==";
      break;
    case CompareOperator::NOT_EQUAL:
      func_name = "!=";
      break;
    case CompareOperator::GREATER:
      func_name = ">";
      break;
    case CompareOperator::GREATER_EQUAL:
      func_name = ">=";
      break;
    case CompareOperator::LESS:
      func_name = "<";
      break;
    case CompareOperator::LESS_EQUAL:
      func_name = "<=";
      break;
    default:
      DCHECK(false);
      break;
  }
  return ExecScalarFunction(ctx, func_name, {left, right}, &options);
}

// ----------------------------------------------------------------------
// Scalar aggregates

Result<Datum> Count(const Datum& value, CountOptions options, ExecContext* ctx) {
  return ExecScalarAggregateFunction(ctx, "count", {value}, &options);
}

Result<Datum> Mean(const Datum& value, ExecContext* ctx) {
  return ExecScalarAggregateFunction(ctx, "mean", {value});
}

Result<Datum> Sum(const Datum& value, ExecContext* ctx) {
  return ExecScalarAggregateFunction(ctx, "sum", {value});
}

// Result<Datum> MinMax(const Datum& value, const MinMaxOptions& options,
//                      ExecContext* ctx) {
//   return ExecScalarAggregateFunction(ctx, "minmax", {value});
// }

// ----------------------------------------------------------------------
// Vector functions

namespace {

// Status InvokeHash(FunctionContext* ctx, HashKernel* func, const Datum& value,
//                   std::vector<Datum>* kernel_outputs,
//                   std::shared_ptr<Array>* dictionary) {
//   RETURN_NOT_OK(detail::InvokeUnaryArrayKernel(ctx, func, value, kernel_outputs));
//   std::shared_ptr<ArrayData> dict_data;
//   RETURN_NOT_OK(func->GetDictionary(&dict_data));
//   *dictionary = MakeArray(dict_data);
//   return Status::OK();
// }

}  // namespace

Result<std::shared_ptr<Array>> Unique(const Datum& value, ExecContext* ctx) {
  // std::unique_ptr<HashKernel> func;
  // RETURN_NOT_OK(GetUniqueKernel(ctx, value.type(), &func));
  // std::vector<Datum> dummy_outputs;
  // return InvokeHash(ctx, func.get(), value, &dummy_outputs, out);
  return Status::NotImplemented("NYI");
}

Result<Datum> DictionaryEncode(const Datum& value, ExecContext* ctx) {
  // std::unique_ptr<HashKernel> func;
  // RETURN_NOT_OK(GetDictionaryEncodeKernel(ctx, value.type(), &func));
  // std::shared_ptr<Array> dict;
  // std::vector<Datum> indices_outputs;
  // RETURN_NOT_OK(InvokeHash(ctx, func.get(), value, &indices_outputs, &dict));
  // auto dict_type = dictionary(func->out_type(), dict->type());
  // // Wrap indices in dictionary arrays for result
  // std::vector<std::shared_ptr<Array>> dict_chunks;
  // for (const Datum& datum : indices_outputs) {
  //   dict_chunks.emplace_back(
  //       std::make_shared<DictionaryArray>(dict_type, datum.make_array(), dict));
  // }
  // *out = detail::WrapArraysLike(value, dict_type, dict_chunks);
  // return Status::OK();
  return Status::NotImplemented("NYI");
}

const char kValuesFieldName[] = "values";
const char kCountsFieldName[] = "counts";
const int32_t kValuesFieldIndex = 0;
const int32_t kCountsFieldIndex = 1;

Result<std::shared_ptr<Array>> ValueCounts(const Datum& value, ExecContext* ctx) {
  // std::unique_ptr<HashKernel> func;
  // RETURN_NOT_OK(GetValueCountsKernel(ctx, value.type(), &func));
  // // Calls return nothing for counts.
  // std::vector<Datum> unused_output;
  // std::shared_ptr<Array> uniques;
  // RETURN_NOT_OK(InvokeHash(ctx, func.get(), value, &unused_output, &uniques));
  // Datum value_counts;
  // RETURN_NOT_OK(func->FlushFinal(&value_counts));
  // auto data_type = std::make_shared<StructType>(std::vector<std::shared_ptr<Field>>{
  //     std::make_shared<Field>(kValuesFieldName, uniques->type()),
  //     std::make_shared<Field>(kCountsFieldName, int64())});
  // *counts = std::make_shared<StructArray>(
  //     data_type, uniques->length(),
  //     std::vector<std::shared_ptr<Array>>{uniques, MakeArray(value_counts.array())});
  // return Status::OK();
  return Status::NotImplemented("NYI");
}

Result<std::shared_ptr<Array>> PartitionIndices(const Array& values, int64_t n,
                                                ExecContext* ctx) {
  PartitionOptions options(/*pivot=*/n);
  ARROW_ASSIGN_OR_RAISE(Datum result, ExecVectorFunction(ctx, "partition_indices",
                                                         {Datum(values)}, &options));
  return result.make_array();
}

}  // namespace compute
}  // namespace arrow
