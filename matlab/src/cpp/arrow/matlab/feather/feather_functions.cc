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

#include "feather_functions.h"

#include <string>

#include "feather_reader.h"
#include "feather_writer.h"
#include "util/handle_status.h"

namespace arrow {
namespace matlab {
namespace feather {

void featherwrite(int nlhs, mxArray* plhs[], int nrhs, const mxArray* prhs[]) {
  const std::string filename{mxArrayToUTF8String(prhs[0])};

  // Open a Feather file at the provided file path for writing.
  std::shared_ptr<FeatherWriter> feather_writer{nullptr};
  util::HandleStatus(FeatherWriter::Open(filename, &feather_writer));

  // Write the Feather file table variables and table metadata from MATLAB.
  util::HandleStatus(feather_writer->WriteVariables(prhs[1], prhs[2]));
}

void featherread(int nlhs, mxArray* plhs[], int nrhs, const mxArray* prhs[]) {
  const std::string filename{mxArrayToUTF8String(prhs[0])};

  // Read the given Feather file into memory.
  std::shared_ptr<FeatherReader> feather_reader{nullptr};
  util::HandleStatus(FeatherReader::Open(filename, &feather_reader));

  // Return the Feather file table variables and table metadata to MATLAB.
  plhs[0] = feather_reader->ReadVariables();
  plhs[1] = feather_reader->ReadMetadata();
}

}  // namespace feather
}  // namespace matlab
}  // namespace arrow