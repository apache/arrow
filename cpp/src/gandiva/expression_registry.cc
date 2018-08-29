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

#include "gandiva/expression_registry.h"

#include "boost/iterator/transform_iterator.hpp"

#include "codegen/function_registry.h"
#include "codegen/llvm_types.h"

namespace gandiva {

ExpressionRegistry::ExpressionRegistry() {
  function_registry_.reset(new FunctionRegistry());
}

ExpressionRegistry::~ExpressionRegistry() {}

const ExpressionRegistry::FunctionSignatureIterator
ExpressionRegistry::function_signature_begin() {
  return FunctionSignatureIterator(function_registry_->begin());
}

const ExpressionRegistry::FunctionSignatureIterator
ExpressionRegistry::function_signature_end() const {
  return FunctionSignatureIterator(function_registry_->end());
}

bool ExpressionRegistry::FunctionSignatureIterator::operator!=(
    const FunctionSignatureIterator &func_sign_it) {
  return func_sign_it.it_ != this->it_;
}

FunctionSignature ExpressionRegistry::FunctionSignatureIterator::operator*() {
  return (*it_).signature();
}

ExpressionRegistry::iterator ExpressionRegistry::FunctionSignatureIterator::operator++(
    int increment) {
  return it_++;
}

DataTypeVector ExpressionRegistry::supported_types_ =
    ExpressionRegistry::InitSupportedTypes();

DataTypeVector ExpressionRegistry::InitSupportedTypes() {
  DataTypeVector data_type_vector;
  llvm::LLVMContext llvm_context;
  LLVMTypes llvm_types(llvm_context);
  auto supported_arrow_types = llvm_types.GetSupportedArrowTypes();
  for (auto &type_id : supported_arrow_types) {
    AddArrowTypesToVector(type_id, data_type_vector);
  }
  return data_type_vector;
}

void ExpressionRegistry::AddArrowTypesToVector(arrow::Type::type &type,
                                               DataTypeVector &vector) {
  switch (type) {
    case arrow::Type::type::BOOL:
      vector.push_back(arrow::boolean());
      break;
    case arrow::Type::type::UINT8:
      vector.push_back(arrow::uint8());
      break;
    case arrow::Type::type::INT8:
      vector.push_back(arrow::int8());
      break;
    case arrow::Type::type::UINT16:
      vector.push_back(arrow::uint16());
      break;
    case arrow::Type::type::INT16:
      vector.push_back(arrow::int16());
      break;
    case arrow::Type::type::UINT32:
      vector.push_back(arrow::uint32());
      break;
    case arrow::Type::type::INT32:
      vector.push_back(arrow::int32());
      break;
    case arrow::Type::type::UINT64:
      vector.push_back(arrow::uint64());
      break;
    case arrow::Type::type::INT64:
      vector.push_back(arrow::int64());
      break;
    case arrow::Type::type::HALF_FLOAT:
      vector.push_back(arrow::float16());
      break;
    case arrow::Type::type::FLOAT:
      vector.push_back(arrow::float32());
      break;
    case arrow::Type::type::DOUBLE:
      vector.push_back(arrow::float64());
      break;
    case arrow::Type::type::STRING:
      vector.push_back(arrow::utf8());
      break;
    case arrow::Type::type::BINARY:
      vector.push_back(arrow::binary());
      break;
    case arrow::Type::type::DATE32:
      vector.push_back(arrow::date32());
      break;
    case arrow::Type::type::DATE64:
      vector.push_back(arrow::date64());
      break;
    case arrow::Type::type::TIMESTAMP:
      vector.push_back(arrow::timestamp(arrow::TimeUnit::SECOND));
      vector.push_back(arrow::timestamp(arrow::TimeUnit::MILLI));
      vector.push_back(arrow::timestamp(arrow::TimeUnit::NANO));
      vector.push_back(arrow::timestamp(arrow::TimeUnit::MICRO));
      break;
    case arrow::Type::type::TIME32:
      vector.push_back(arrow::time32(arrow::TimeUnit::SECOND));
      vector.push_back(arrow::time32(arrow::TimeUnit::MILLI));
      break;
    case arrow::Type::type::TIME64:
      vector.push_back(arrow::time64(arrow::TimeUnit::MICRO));
      vector.push_back(arrow::time64(arrow::TimeUnit::NANO));
      break;
    case arrow::Type::type::NA:
      vector.push_back(arrow::null());
      break;
    case arrow::Type::type::FIXED_SIZE_BINARY:
    case arrow::Type::type::MAP:
    case arrow::Type::type::INTERVAL:
    case arrow::Type::type::DECIMAL:
    case arrow::Type::type::LIST:
    case arrow::Type::type::STRUCT:
    case arrow::Type::type::UNION:
    case arrow::Type::type::DICTIONARY:
      // un-supported types. test ensures that
      // when one of these are added build breaks.
      DCHECK(false);
  }
}

}  // namespace gandiva
