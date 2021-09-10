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

#include <arrow/status.h>

#include <mex.h>

namespace arrow {
namespace matlab {
namespace util {

void HandleStatus(const Status& status) {
  const char* arrow_error_message = "Arrow error: %s";
  switch (status.code()) {
    case StatusCode::OK: {
      break;
    }
    case StatusCode::OutOfMemory: {
      mexErrMsgIdAndTxt("MATLAB:arrow:status:OutOfMemory", arrow_error_message,
                        status.ToString().c_str());
      break;
    }
    case StatusCode::KeyError: {
      mexErrMsgIdAndTxt("MATLAB:arrow:status:KeyError", arrow_error_message,
                        status.ToString().c_str());
      break;
    }
    case StatusCode::TypeError: {
      mexErrMsgIdAndTxt("MATLAB:arrow:status:TypeError", arrow_error_message,
                        status.ToString().c_str());
      break;
    }
    case StatusCode::Invalid: {
      mexErrMsgIdAndTxt("MATLAB:arrow:status:Invalid", arrow_error_message,
                        status.ToString().c_str());
      break;
    }
    case StatusCode::IOError: {
      mexErrMsgIdAndTxt("MATLAB:arrow:status:IOError", arrow_error_message,
                        status.ToString().c_str());
      break;
    }
    case StatusCode::CapacityError: {
      mexErrMsgIdAndTxt("MATLAB:arrow:status:CapacityError", arrow_error_message,
                        status.ToString().c_str());
      break;
    }
    case StatusCode::IndexError: {
      mexErrMsgIdAndTxt("MATLAB:arrow:status:IndexError", arrow_error_message,
                        status.ToString().c_str());
      break;
    }
    case StatusCode::UnknownError: {
      mexErrMsgIdAndTxt("MATLAB:arrow:status:UnknownError", arrow_error_message,
                        status.ToString().c_str());
      break;
    }
    case StatusCode::NotImplemented: {
      mexErrMsgIdAndTxt("MATLAB:arrow:status:NotImplemented", arrow_error_message,
                        status.ToString().c_str());
      break;
    }
    case StatusCode::SerializationError: {
      mexErrMsgIdAndTxt("MATLAB:arrow:status:SerializationError", arrow_error_message,
                        status.ToString().c_str());
      break;
    }
    default: {
      mexErrMsgIdAndTxt("MATLAB:arrow:status:UnknownStatus", arrow_error_message,
                        "Unknown status");
      break;
    }
  }
}
}  // namespace util
}  // namespace matlab
}  // namespace arrow
