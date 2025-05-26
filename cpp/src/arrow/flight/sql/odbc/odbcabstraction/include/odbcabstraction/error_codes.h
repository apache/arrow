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

#include <cstdint>

namespace driver {
namespace odbcabstraction {

enum ODBCErrorCodes : int32_t {
  ODBCErrorCodes_GENERAL_ERROR = 100,
  ODBCErrorCodes_AUTH = 200,
  ODBCErrorCodes_TLS = 300,
  ODBCErrorCodes_FRACTIONAL_TRUNCATION_ERROR = 400,
  ODBCErrorCodes_COMMUNICATION = 500,
  ODBCErrorCodes_GENERAL_WARNING = 1000000,
  ODBCErrorCodes_TRUNCATION_WARNING = 1000100,
  ODBCErrorCodes_FRACTIONAL_TRUNCATION_WARNING = 1000100,
  ODBCErrorCodes_INDICATOR_NEEDED = 1000200
};
}  // namespace odbcabstraction
}  // namespace driver
