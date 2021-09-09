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

#include <string>
#include <unordered_map>

#include <mex.h>

#include "featherreadmex.h"
#include "featherwritemex.h"

enum class OperationCode
{
    FEATHER_READ,
    FEATHER_WRITE,
    INVALID_OPERATION
};

static const std::unordered_map<std::string, OperationCode> OPERATION_CODE_MAP =
{
    {u8"featherread", OperationCode::FEATHER_READ},
    {u8"featherwrite", OperationCode::FEATHER_WRITE}
};


OperationCode opname_to_opcode(const std::string& opname)
{
    auto kv_pair = OPERATION_CODE_MAP.find(opname);
    if (kv_pair == OPERATION_CODE_MAP.end()) {
        return OperationCode::INVALID_OPERATION;
    }
    return kv_pair->second;
}

std::string get_opname(const mxArray* input)
{
    std::string opname;
    if (!mxIsChar(input)) {
        mexErrMsgIdAndTxt("MATLAB:arrow:OperationNameDataType",
                          "The first input to 'mexDispatcher' must be a char vector.");
    }
    const char* c_str = mxArrayToUTF8String(input);
    if (!c_str) {
        mexErrMsgIdAndTxt("MATLAB:arrow:NullOperationName",
                          "The function name passed to ''mexDispatcher' cannot be null.");
    }
    return std::string{c_str};
}

void checkNumArgs(int nrhs)
{
    if (nrhs < 1) {
        mexErrMsgIdAndTxt("MATLAB:arrow:minrhs",
                          "'mexDispatcher' requires at least one input argument.");
    }
}

// MEX gateway function.
void mexFunction(int nlhs, mxArray* plhs[], int nrhs, const mxArray* prhs[])
{
    checkNumArgs(nrhs);
    const std::string opname = get_opname(prhs[0]);
    OperationCode opcode = opname_to_opcode(opname);
    switch (opcode) {
        case OperationCode::FEATHER_READ:
            featherreadmex(nlhs, plhs, nrhs - 1, ++prhs);
            break;
        case OperationCode::FEATHER_WRITE:
            featherwrite(nlhs, plhs, nrhs - 1, ++prhs);
            break;
        case OperationCode::INVALID_OPERATION:
            break;
    }
}
