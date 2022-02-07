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

#include "mex_util.h"

namespace arrow {
namespace matlab {
namespace mex {

void checkNumArgs(int nrhs) {
  if (nrhs < 1) {
    mexErrMsgIdAndTxt("MATLAB:arrow:minrhs",
                      "'mexfcn' requires at least one input argument, which must be the "
                      "name of the C++ MEX to invoke.");
  }
}

std::string get_function_name(const mxArray* input) {
  std::string opname;
  if (!mxIsChar(input)) {
    mexErrMsgIdAndTxt("MATLAB:arrow:FunctionNameDataType",
                      "The first input argument to 'mexfcn' must be a character vector.");
  }
  const char* c_str = mxArrayToUTF8String(input);
  return std::string{c_str};
}

mex_fcn_t lookup_function(const std::string& function_name) {
  auto kv_pair = FUNCTION_MAP.find(function_name);
  if (kv_pair == FUNCTION_MAP.end()) {
    mexErrMsgIdAndTxt("MATLAB:arrow:UnknownMEXFunction", "Unrecognized MEX function '%s'",
                      function_name.c_str());
  }
  return kv_pair->second;
}
    
} // namespace mex
} // namespace matlab
} // namespace arrow