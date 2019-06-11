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

#ifndef ARROW_MATLAB_UTIL_UNICODE_CONVERSION_H
#define ARROW_MATLAB_UTIL_UNICODE_CONVERSION_H

#include <string>
#include <mex.h>

namespace arrow {
namespace matlab {
namespace util {
// Converts a UTF-8 encoded std::string to a heap-allocated UTF-16 encoded
// mxCharArray.
mxArray* ConvertUTF8StringToUTF16CharMatrix(const std::string& utf8_string);
}  // namespace util
}  // namespace matlab
}  // namespace arrow

#endif /* ARROW_MATLAB_UTIL_UNICODE_CONVERSION_H */
