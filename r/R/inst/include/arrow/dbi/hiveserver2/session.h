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

#include "arrow/dbi/hiveserver2/operation.h"
#include "arrow/dbi/hiveserver2/service.h"

#include "arrow/util/visibility.h"

namespace arrow {

class Status;

namespace hiveserver2 {

struct ThriftRPC;

// Manages a single HiveServer2 session - stores the session handle returned by
// the OpenSession RPC and uses it to create and return operations.
//
// Sessions are created with Service::OpenSession(). They must have Close
// called on them before they can be deleted.
//
// Executing RPCs with an Operation corresponding to a particular Session after
// that Session has been closed or deleted is undefined.
//
// This class is not thread-safe.
class ARROW_EXPORT Session {
 public:
  ~Session();

  // Closes the session. Must be called before the session is deleted. May be safely
  // called on an invalid or already closed session - will only return an error if the
  // session is open but the close rpc fails.
  Status Close();

  Status ExecuteStatement(const std::string& statement,
                          std::unique_ptr<Operation>* operation) const;
  Status ExecuteStatement(const std::string& statement,
                          const HS2ClientConfig& conf_overlay,
                          std::unique_ptr<Operation>* operation) const;

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(Session);

  // Hides Thrift objects from the header.
  struct SessionImpl;

  // For access to the c'tor.
  friend class Service;

  explicit Session(const std::shared_ptr<ThriftRPC>& rpc);

  // Performs the RPC that initiates the session and stores the returned handle.
  // Must be called before operations can be executed.
  Status Open(const HS2ClientConfig& config, const std::string& user);

  std::unique_ptr<SessionImpl> impl_;
  std::shared_ptr<ThriftRPC> rpc_;

  // True if Open has been called and Close has not.
  bool open_;
};

}  // namespace hiveserver2
}  // namespace arrow
