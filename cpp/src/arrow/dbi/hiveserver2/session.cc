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

#include "arrow/dbi/hiveserver2/session.h"

#include "arrow/dbi/hiveserver2/TCLIService.h"
#include "arrow/dbi/hiveserver2/thrift-internal.h"

#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace hs2 = apache::hive::service::cli::thrift;
using apache::thrift::TException;
using std::string;
using std::unique_ptr;

namespace arrow {
namespace hiveserver2 {

struct Session::SessionImpl {
  hs2::TSessionHandle handle;
};

Session::Session(const std::shared_ptr<ThriftRPC>& rpc)
    : impl_(new SessionImpl()), rpc_(rpc), open_(false) {}

Session::~Session() { DCHECK(!open_); }

Status Session::Close() {
  if (!open_) return Status::OK();

  hs2::TCloseSessionReq req;
  req.__set_sessionHandle(impl_->handle);
  hs2::TCloseSessionResp resp;
  TRY_RPC_OR_RETURN(rpc_->client->CloseSession(resp, req));
  THRIFT_RETURN_NOT_OK(resp.status);

  open_ = false;
  return TStatusToStatus(resp.status);
}

Status Session::Open(const HS2ClientConfig& config, const string& user) {
  hs2::TOpenSessionReq req;
  req.__set_configuration(config.GetConfig());
  req.__set_username(user);
  hs2::TOpenSessionResp resp;
  TRY_RPC_OR_RETURN(rpc_->client->OpenSession(resp, req));
  THRIFT_RETURN_NOT_OK(resp.status);

  impl_->handle = resp.sessionHandle;
  open_ = true;
  return TStatusToStatus(resp.status);
}

class ExecuteStatementOperation : public Operation {
 public:
  explicit ExecuteStatementOperation(const std::shared_ptr<ThriftRPC>& rpc)
      : Operation(rpc) {}

  Status Open(hs2::TSessionHandle session_handle, const string& statement,
              const HS2ClientConfig& config) {
    hs2::TExecuteStatementReq req;
    req.__set_sessionHandle(session_handle);
    req.__set_statement(statement);
    req.__set_confOverlay(config.GetConfig());
    hs2::TExecuteStatementResp resp;
    TRY_RPC_OR_RETURN(rpc_->client->ExecuteStatement(resp, req));
    THRIFT_RETURN_NOT_OK(resp.status);

    impl_->handle = resp.operationHandle;
    impl_->session_handle = session_handle;
    open_ = true;
    return TStatusToStatus(resp.status);
  }
};

Status Session::ExecuteStatement(const string& statement,
                                 unique_ptr<Operation>* operation) const {
  return ExecuteStatement(statement, HS2ClientConfig(), operation);
}

Status Session::ExecuteStatement(const string& statement,
                                 const HS2ClientConfig& conf_overlay,
                                 unique_ptr<Operation>* operation) const {
  ExecuteStatementOperation* op = new ExecuteStatementOperation(rpc_);
  operation->reset(op);
  return op->Open(impl_->handle, statement, conf_overlay);
}

}  // namespace hiveserver2
}  // namespace arrow
