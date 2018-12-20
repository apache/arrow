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

#include "arrow/dbi/hiveserver2/thrift-internal.h"

#include <map>
#include <sstream>

#include "arrow/dbi/hiveserver2/TCLIService_constants.h"
#include "arrow/dbi/hiveserver2/service.h"

#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace hs2 = apache::hive::service::cli::thrift;

namespace arrow {
namespace hiveserver2 {

namespace {

// Convert an "enum class" value to an integer equivalent, for outputting.
template <typename ENUM>
typename std::underlying_type<ENUM>::type EnumToInt(const ENUM& value) {
  return static_cast<typename std::underlying_type<ENUM>::type>(value);
}

}  // namespace

const std::string OperationStateToString(const Operation::State& state) {
  switch (state) {
    case Operation::State::INITIALIZED:
      return "INITIALIZED";
    case Operation::State::RUNNING:
      return "RUNNING";
    case Operation::State::FINISHED:
      return "FINISHED";
    case Operation::State::CANCELED:
      return "CANCELED";
    case Operation::State::CLOSED:
      return "CLOSED";
    case Operation::State::ERROR:
      return "ERROR";
    case Operation::State::UNKNOWN:
      return "UNKNOWN";
    case Operation::State::PENDING:
      return "PENDING";
    default:
      std::stringstream ss;
      ss << "Unknown Operation::State " << EnumToInt(state);
      return ss.str();
  }
}

const std::string TypeIdToString(const ColumnType::TypeId& type_id) {
  switch (type_id) {
    case ColumnType::TypeId::BOOLEAN:
      return "BOOLEAN";
    case ColumnType::TypeId::TINYINT:
      return "TINYINT";
    case ColumnType::TypeId::SMALLINT:
      return "SMALLINT";
    case ColumnType::TypeId::INT:
      return "INT";
    case ColumnType::TypeId::BIGINT:
      return "BIGINT";
    case ColumnType::TypeId::FLOAT:
      return "FLOAT";
    case ColumnType::TypeId::DOUBLE:
      return "DOUBLE";
    case ColumnType::TypeId::STRING:
      return "STRING";
    case ColumnType::TypeId::TIMESTAMP:
      return "TIMESTAMP";
    case ColumnType::TypeId::BINARY:
      return "BINARY";
    case ColumnType::TypeId::ARRAY:
      return "ARRAY";
    case ColumnType::TypeId::MAP:
      return "MAP";
    case ColumnType::TypeId::STRUCT:
      return "STRUCT";
    case ColumnType::TypeId::UNION:
      return "UNION";
    case ColumnType::TypeId::USER_DEFINED:
      return "USER_DEFINED";
    case ColumnType::TypeId::DECIMAL:
      return "DECIMAL";
    case ColumnType::TypeId::NULL_TYPE:
      return "NULL_TYPE";
    case ColumnType::TypeId::DATE:
      return "DATE";
    case ColumnType::TypeId::VARCHAR:
      return "VARCHAR";
    case ColumnType::TypeId::CHAR:
      return "CHAR";
    case ColumnType::TypeId::INVALID:
      return "INVALID";
    default: {
      std::stringstream ss;
      ss << "Unknown ColumnType::TypeId " << EnumToInt(type_id);
      return ss.str();
    }
  }
}

hs2::TFetchOrientation::type FetchOrientationToTFetchOrientation(
    FetchOrientation orientation) {
  switch (orientation) {
    case FetchOrientation::NEXT:
      return hs2::TFetchOrientation::FETCH_NEXT;
    case FetchOrientation::PRIOR:
      return hs2::TFetchOrientation::FETCH_PRIOR;
    case FetchOrientation::RELATIVE:
      return hs2::TFetchOrientation::FETCH_RELATIVE;
    case FetchOrientation::ABSOLUTE:
      return hs2::TFetchOrientation::FETCH_ABSOLUTE;
    case FetchOrientation::FIRST:
      return hs2::TFetchOrientation::FETCH_FIRST;
    case FetchOrientation::LAST:
      return hs2::TFetchOrientation::FETCH_LAST;
    default:
      DCHECK(false) << "Unknown FetchOrientation " << EnumToInt(orientation);
      return hs2::TFetchOrientation::FETCH_NEXT;
  }
}

hs2::TProtocolVersion::type ProtocolVersionToTProtocolVersion(ProtocolVersion protocol) {
  switch (protocol) {
    case ProtocolVersion::PROTOCOL_V1:
      return hs2::TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V1;
    case ProtocolVersion::PROTOCOL_V2:
      return hs2::TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V2;
    case ProtocolVersion::PROTOCOL_V3:
      return hs2::TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V3;
    case ProtocolVersion::PROTOCOL_V4:
      return hs2::TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V4;
    case ProtocolVersion::PROTOCOL_V5:
      return hs2::TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V5;
    case ProtocolVersion::PROTOCOL_V6:
      return hs2::TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V6;
    case ProtocolVersion::PROTOCOL_V7:
      return hs2::TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V7;
    default:
      DCHECK(false) << "Unknown ProtocolVersion " << EnumToInt(protocol);
      return hs2::TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V7;
  }
}

Operation::State TOperationStateToOperationState(
    const hs2::TOperationState::type& tstate) {
  switch (tstate) {
    case hs2::TOperationState::INITIALIZED_STATE:
      return Operation::State::INITIALIZED;
    case hs2::TOperationState::RUNNING_STATE:
      return Operation::State::RUNNING;
    case hs2::TOperationState::FINISHED_STATE:
      return Operation::State::FINISHED;
    case hs2::TOperationState::CANCELED_STATE:
      return Operation::State::CANCELED;
    case hs2::TOperationState::CLOSED_STATE:
      return Operation::State::CLOSED;
    case hs2::TOperationState::ERROR_STATE:
      return Operation::State::ERROR;
    case hs2::TOperationState::UKNOWN_STATE:
      return Operation::State::UNKNOWN;
    case hs2::TOperationState::PENDING_STATE:
      return Operation::State::PENDING;
    default:
      ARROW_LOG(WARNING) << "Unknown TOperationState " << tstate;
      return Operation::State::UNKNOWN;
  }
}

Status TStatusToStatus(const hs2::TStatus& tstatus) {
  switch (tstatus.statusCode) {
    case hs2::TStatusCode::SUCCESS_STATUS:
      return Status::OK();
    case hs2::TStatusCode::SUCCESS_WITH_INFO_STATUS: {
      std::stringstream ss;
      for (size_t i = 0; i < tstatus.infoMessages.size(); i++) {
        if (i != 0) ss << ",";
        ss << tstatus.infoMessages[i];
      }
      return Status::OK(ss.str());
    }
    case hs2::TStatusCode::STILL_EXECUTING_STATUS:
      return Status::StillExecuting();
    case hs2::TStatusCode::ERROR_STATUS:
      return Status::IOError(tstatus.errorMessage);
    case hs2::TStatusCode::INVALID_HANDLE_STATUS:
      return Status::Invalid("Invalid handle");
    default: { return Status::UnknownError("Unknown TStatusCode ", tstatus.statusCode); }
  }
}

std::unique_ptr<ColumnType> TTypeDescToColumnType(const hs2::TTypeDesc& ttype_desc) {
  if (ttype_desc.types.size() != 1 || !ttype_desc.types[0].__isset.primitiveEntry) {
    ARROW_LOG(WARNING) << "TTypeDescToColumnType only supports primitive types.";
    return std::unique_ptr<ColumnType>(new PrimitiveType(ColumnType::TypeId::INVALID));
  }

  ColumnType::TypeId type_id = TTypeIdToTypeId(ttype_desc.types[0].primitiveEntry.type);
  if (type_id == ColumnType::TypeId::CHAR || type_id == ColumnType::TypeId::VARCHAR) {
    const std::map<std::string, hs2::TTypeQualifierValue>& qualifiers =
        ttype_desc.types[0].primitiveEntry.typeQualifiers.qualifiers;
    DCHECK_EQ(qualifiers.count(hs2::g_TCLIService_constants.CHARACTER_MAXIMUM_LENGTH), 1);

    try {
      return std::unique_ptr<ColumnType>(new CharacterType(
          type_id,
          qualifiers.at(hs2::g_TCLIService_constants.CHARACTER_MAXIMUM_LENGTH).i32Value));
    } catch (std::out_of_range e) {
      ARROW_LOG(ERROR) << "Character type qualifiers invalid: " << e.what();
      return std::unique_ptr<ColumnType>(new PrimitiveType(ColumnType::TypeId::INVALID));
    }
  } else if (type_id == ColumnType::TypeId::DECIMAL) {
    const std::map<std::string, hs2::TTypeQualifierValue>& qualifiers =
        ttype_desc.types[0].primitiveEntry.typeQualifiers.qualifiers;
    DCHECK_EQ(qualifiers.count(hs2::g_TCLIService_constants.PRECISION), 1);
    DCHECK_EQ(qualifiers.count(hs2::g_TCLIService_constants.SCALE), 1);

    try {
      return std::unique_ptr<ColumnType>(new DecimalType(
          type_id, qualifiers.at(hs2::g_TCLIService_constants.PRECISION).i32Value,
          qualifiers.at(hs2::g_TCLIService_constants.SCALE).i32Value));
    } catch (std::out_of_range e) {
      ARROW_LOG(ERROR) << "Decimal type qualifiers invalid: " << e.what();
      return std::unique_ptr<ColumnType>(new PrimitiveType(ColumnType::TypeId::INVALID));
    }
  } else {
    return std::unique_ptr<ColumnType>(new PrimitiveType(type_id));
  }
}

ColumnType::TypeId TTypeIdToTypeId(const hs2::TTypeId::type& type_id) {
  switch (type_id) {
    case hs2::TTypeId::BOOLEAN_TYPE:
      return ColumnType::TypeId::BOOLEAN;
    case hs2::TTypeId::TINYINT_TYPE:
      return ColumnType::TypeId::TINYINT;
    case hs2::TTypeId::SMALLINT_TYPE:
      return ColumnType::TypeId::SMALLINT;
    case hs2::TTypeId::INT_TYPE:
      return ColumnType::TypeId::INT;
    case hs2::TTypeId::BIGINT_TYPE:
      return ColumnType::TypeId::BIGINT;
    case hs2::TTypeId::FLOAT_TYPE:
      return ColumnType::TypeId::FLOAT;
    case hs2::TTypeId::DOUBLE_TYPE:
      return ColumnType::TypeId::DOUBLE;
    case hs2::TTypeId::STRING_TYPE:
      return ColumnType::TypeId::STRING;
    case hs2::TTypeId::TIMESTAMP_TYPE:
      return ColumnType::TypeId::TIMESTAMP;
    case hs2::TTypeId::BINARY_TYPE:
      return ColumnType::TypeId::BINARY;
    case hs2::TTypeId::ARRAY_TYPE:
      return ColumnType::TypeId::ARRAY;
    case hs2::TTypeId::MAP_TYPE:
      return ColumnType::TypeId::MAP;
    case hs2::TTypeId::STRUCT_TYPE:
      return ColumnType::TypeId::STRUCT;
    case hs2::TTypeId::UNION_TYPE:
      return ColumnType::TypeId::UNION;
    case hs2::TTypeId::USER_DEFINED_TYPE:
      return ColumnType::TypeId::USER_DEFINED;
    case hs2::TTypeId::DECIMAL_TYPE:
      return ColumnType::TypeId::DECIMAL;
    case hs2::TTypeId::NULL_TYPE:
      return ColumnType::TypeId::NULL_TYPE;
    case hs2::TTypeId::DATE_TYPE:
      return ColumnType::TypeId::DATE;
    case hs2::TTypeId::VARCHAR_TYPE:
      return ColumnType::TypeId::VARCHAR;
    case hs2::TTypeId::CHAR_TYPE:
      return ColumnType::TypeId::CHAR;
    default:
      ARROW_LOG(WARNING) << "Unknown TTypeId " << type_id;
      return ColumnType::TypeId::INVALID;
  }
}

}  // namespace hiveserver2
}  // namespace arrow
