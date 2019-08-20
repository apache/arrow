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

#include "arrow/dbi/hiveserver2/columnar_row_set.h"
#include "arrow/dbi/hiveserver2/operation.h"
#include "arrow/dbi/hiveserver2/service.h"
#include "arrow/dbi/hiveserver2/types.h"

#include "arrow/dbi/hiveserver2/ImpalaHiveServer2Service.h"
#include "arrow/dbi/hiveserver2/TCLIService.h"

namespace arrow {
namespace hiveserver2 {

// PIMPL structs.
struct ColumnarRowSet::ColumnarRowSetImpl {
  apache::hive::service::cli::thrift::TFetchResultsResp resp;
};

struct Operation::OperationImpl {
  apache::hive::service::cli::thrift::TOperationHandle handle;
  apache::hive::service::cli::thrift::TSessionHandle session_handle;
};

struct ThriftRPC {
  std::unique_ptr<impala::ImpalaHiveServer2ServiceClient> client;
};

const std::string OperationStateToString(const Operation::State& state);

const std::string TypeIdToString(const ColumnType::TypeId& type_id);

// Functions for converting Thrift object to hs2client objects and vice-versa.
apache::hive::service::cli::thrift::TFetchOrientation::type
FetchOrientationToTFetchOrientation(FetchOrientation orientation);

apache::hive::service::cli::thrift::TProtocolVersion::type
ProtocolVersionToTProtocolVersion(ProtocolVersion protocol);

Operation::State TOperationStateToOperationState(
    const apache::hive::service::cli::thrift::TOperationState::type& tstate);

Status TStatusToStatus(const apache::hive::service::cli::thrift::TStatus& tstatus);

// Converts a TTypeDesc to a ColumnType. Currently only primitive types are supported.
// The converted type is returned as a pointer to allow for polymorphism with ColumnType
// and its subclasses.
std::unique_ptr<ColumnType> TTypeDescToColumnType(
    const apache::hive::service::cli::thrift::TTypeDesc& ttype_desc);

ColumnType::TypeId TTypeIdToTypeId(
    const apache::hive::service::cli::thrift::TTypeId::type& type_id);

}  // namespace hiveserver2
}  // namespace arrow

#define TRY_RPC_OR_RETURN(rpc)                  \
  do {                                          \
    try {                                       \
      (rpc);                                    \
    } catch (apache::thrift::TException & tx) { \
      return Status::IOError(tx.what());        \
    }                                           \
  } while (0)

#define THRIFT_RETURN_NOT_OK(tstatus)                                       \
  do {                                                                      \
    if (tstatus.statusCode != hs2::TStatusCode::SUCCESS_STATUS &&           \
        tstatus.statusCode != hs2::TStatusCode::SUCCESS_WITH_INFO_STATUS) { \
      return TStatusToStatus(tstatus);                                      \
    }                                                                       \
  } while (0)
