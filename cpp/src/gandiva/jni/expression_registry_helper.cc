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
#include "jni/org_apache_arrow_gandiva_evaluator_ExpressionRegistryJniHelper.h"

#include <memory>

#include "Types.pb.h"
#include "gandiva/arrow.h"
#include "gandiva/expression_registry.h"

using gandiva::DataTypePtr;
using gandiva::ExpressionRegistry;

types::TimeUnit MapTimeUnit(arrow::TimeUnit::type &unit) {
  switch (unit) {
    case arrow::TimeUnit::MILLI:
      return types::TimeUnit::MILLISEC;
    case arrow::TimeUnit::SECOND:
      return types::TimeUnit::SEC;
    case arrow::TimeUnit::MICRO:
      return types::TimeUnit::MICROSEC;
    case arrow::TimeUnit::NANO:
      return types::TimeUnit::NANOSEC;
  }
  // satifsy gcc. should be unreachable.
  return types::TimeUnit::SEC;
}

void ArrowToProtobuf(DataTypePtr type, types::ExtGandivaType *gandiva_data_type) {
  switch (type->id()) {
    case arrow::Type::type::BOOL:
      gandiva_data_type->set_type(types::GandivaType::BOOL);
      break;
    case arrow::Type::type::UINT8:
      gandiva_data_type->set_type(types::GandivaType::UINT8);
      break;
    case arrow::Type::type::INT8:
      gandiva_data_type->set_type(types::GandivaType::INT8);
      break;
    case arrow::Type::type::UINT16:
      gandiva_data_type->set_type(types::GandivaType::UINT16);
      break;
    case arrow::Type::type::INT16:
      gandiva_data_type->set_type(types::GandivaType::INT16);
      break;
    case arrow::Type::type::UINT32:
      gandiva_data_type->set_type(types::GandivaType::UINT32);
      break;
    case arrow::Type::type::INT32:
      gandiva_data_type->set_type(types::GandivaType::INT32);
      break;
    case arrow::Type::type::UINT64:
      gandiva_data_type->set_type(types::GandivaType::UINT64);
      break;
    case arrow::Type::type::INT64:
      gandiva_data_type->set_type(types::GandivaType::INT64);
      break;
    case arrow::Type::type::HALF_FLOAT:
      gandiva_data_type->set_type(types::GandivaType::HALF_FLOAT);
      break;
    case arrow::Type::type::FLOAT:
      gandiva_data_type->set_type(types::GandivaType::FLOAT);
      break;
    case arrow::Type::type::DOUBLE:
      gandiva_data_type->set_type(types::GandivaType::DOUBLE);
      break;
    case arrow::Type::type::STRING:
      gandiva_data_type->set_type(types::GandivaType::UTF8);
      break;
    case arrow::Type::type::BINARY:
      gandiva_data_type->set_type(types::GandivaType::BINARY);
      break;
    case arrow::Type::type::DATE32:
      gandiva_data_type->set_type(types::GandivaType::DATE32);
      break;
    case arrow::Type::type::DATE64:
      gandiva_data_type->set_type(types::GandivaType::DATE64);
      break;
    case arrow::Type::type::TIMESTAMP: {
      gandiva_data_type->set_type(types::GandivaType::TIMESTAMP);
      std::shared_ptr<arrow::TimestampType> cast_time_stamp_type =
          std::dynamic_pointer_cast<arrow::TimestampType>(type);
      arrow::TimeUnit::type unit = cast_time_stamp_type->unit();
      types::TimeUnit time_unit = MapTimeUnit(unit);
      gandiva_data_type->set_timeunit(time_unit);
      break;
    }
    case arrow::Type::type::TIME32: {
      gandiva_data_type->set_type(types::GandivaType::TIME32);
      std::shared_ptr<arrow::Time32Type> cast_time_32_type =
          std::dynamic_pointer_cast<arrow::Time32Type>(type);
      arrow::TimeUnit::type unit = cast_time_32_type->unit();
      types::TimeUnit time_unit = MapTimeUnit(unit);
      gandiva_data_type->set_timeunit(time_unit);
      break;
    }
    case arrow::Type::type::TIME64: {
      gandiva_data_type->set_type(types::GandivaType::TIME32);
      std::shared_ptr<arrow::Time64Type> cast_time_64_type =
          std::dynamic_pointer_cast<arrow::Time64Type>(type);
      arrow::TimeUnit::type unit = cast_time_64_type->unit();
      types::TimeUnit time_unit = MapTimeUnit(unit);
      gandiva_data_type->set_timeunit(time_unit);
      break;
    }
    case arrow::Type::type::NA:
      gandiva_data_type->set_type(types::GandivaType::NONE);
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

JNIEXPORT jbyteArray JNICALL
Java_org_apache_arrow_gandiva_evaluator_ExpressionRegistryJniHelper_getGandivaSupportedDataTypes(
    JNIEnv *env, jobject types_helper) {
  types::GandivaDataTypes gandiva_data_types;
  auto supported_types = ExpressionRegistry::supported_types();
  for (auto const &type : supported_types) {
    types::ExtGandivaType *gandiva_data_type = gandiva_data_types.add_datatype();
    ArrowToProtobuf(type, gandiva_data_type);
  }
  size_t size = gandiva_data_types.ByteSizeLong();
  std::unique_ptr<jbyte[]> buffer{new jbyte[size]};
  gandiva_data_types.SerializeToArray((void *)buffer.get(), size);
  jbyteArray ret = env->NewByteArray(size);
  env->SetByteArrayRegion(ret, 0, size, buffer.get());
  return ret;
}

/*
 * Class:     org_apache_arrow_gandiva_types_ExpressionRegistryJniHelper
 * Method:    getGandivaSupportedFunctions
 * Signature: ()[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_apache_arrow_gandiva_evaluator_ExpressionRegistryJniHelper_getGandivaSupportedFunctions(
    JNIEnv *env, jobject types_helper) {
  ExpressionRegistry expr_registry;
  types::GandivaFunctions gandiva_functions;
  for (auto function = expr_registry.function_signature_begin();
       function != expr_registry.function_signature_end(); function++) {
    types::FunctionSignature *function_signature = gandiva_functions.add_function();
    function_signature->set_name((*function).base_name());
    types::ExtGandivaType *return_type = function_signature->mutable_returntype();
    ArrowToProtobuf((*function).ret_type(), return_type);
    for (auto &param_type : (*function).param_types()) {
      types::ExtGandivaType *proto_param_type = function_signature->add_paramtypes();
      ArrowToProtobuf(param_type, proto_param_type);
    }
  }
  size_t size = gandiva_functions.ByteSizeLong();
  std::unique_ptr<jbyte[]> buffer{new jbyte[size]};
  gandiva_functions.SerializeToArray((void *)buffer.get(), size);
  jbyteArray ret = env->NewByteArray(size);
  env->SetByteArrayRegion(ret, 0, size, buffer.get());
  return ret;
}
