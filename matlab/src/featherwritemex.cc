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

#include <mex.h>

#include "feather_writer.h"
#include "util/handle_status.h"

// MEX gateway function. This is the entry point for featherwritemex.cc.
void mexFunction(int nlhs, mxArray* plhs[], int nrhs, const mxArray* prhs[]) {
  const std::string filename{mxArrayToUTF8String(prhs[0])};

  // Open a Feather file at the provided file path for writing.
  std::shared_ptr<arrow::matlab::FeatherWriter> feather_writer{nullptr};
  arrow::matlab::util::HandleStatus(
      arrow::matlab::FeatherWriter::Open(filename, &feather_writer));

  // Write the Feather file table variables and table metadata from MATLAB.
  feather_writer->WriteMetadata(prhs[2]);
  arrow::matlab::util::HandleStatus(feather_writer->WriteVariables(prhs[1]));
}
