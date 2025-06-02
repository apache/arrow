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

#include <arrow/type.h>

namespace driver {
namespace flight_sql {

void ReportSystemFunction(const std::string& function, uint32_t& current_sys_functions,
                          uint32_t& current_convert_functions);
void ReportNumericFunction(const std::string& function, uint32_t& current_functions);
void ReportStringFunction(const std::string& function, uint32_t& current_functions);
void ReportDatetimeFunction(const std::string& function, uint32_t& current_functions);

}  // namespace flight_sql
}  // namespace driver
