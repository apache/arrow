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

#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace arrow::internal {

class StatusConstant {
 public:
  StatusConstant(StatusCode code, std::string msg,
                 std::shared_ptr<StatusDetail> detail = nullptr)
      : state_{code, /*is_constant=*/true, std::move(msg), std::move(detail)} {
    ARROW_CHECK_NE(code, StatusCode::OK)
        << "StatusConstant is not intended for use with OK status codes";
  }

  operator Status() {  // NOLINT(runtime/explicit)
    Status st;
    st.state_ = &state_;
    return st;
  }

 private:
  Status::State state_;
};

}  // namespace arrow::internal
