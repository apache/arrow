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

#include "arrow-adbc/adbc.h"
#include "arrow-adbc/driver_base.h"

namespace arrow {
namespace engine {

namespace internal {

class AceroDatabase : public adbc::common::DatabaseObjectBase {};

class AceroConnection : public adbc::common::ConnectionObjectBase {};

class AceroStatement : public adbc::common::StatementObjectBase {};

using AceroDriver = adbc::common::Driver<AceroDatabase, AceroConnection, AceroStatement>;

}  // namespace internal

uint8_t AceroDriverInitFunc(int version, void* raw_driver, void* error) {
  return internal::AceroDriver::Init(version, raw_driver,
                                     reinterpret_cast<AdbcError*>(error));
}

}  // namespace engine
}  // namespace arrow
