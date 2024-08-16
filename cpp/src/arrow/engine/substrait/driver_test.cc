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

#include <gtest/gtest.h>

#include "arrow-adbc/adbc.h"
#include "arrow/engine/substrait/driver.h"

// Self-contained version of the Handle
static inline void clean_up(AdbcDriver* ptr) { ptr->release(ptr, nullptr); }

// static inline void clean_up(AdbcDatabase* ptr) {
//   ptr->private_driver->DatabaseRelease(ptr, nullptr);
// }

// static inline void clean_up(AdbcConnection* ptr) {
//   ptr->private_driver->ConnectionRelease(ptr, nullptr);
// }

// static inline void clean_up(AdbcStatement* ptr) {
//   ptr->private_driver->StatementRelease(ptr, nullptr);
// }

// static inline void clean_up(AdbcError* ptr) {
//   if (ptr->release != nullptr) {
//     ptr->release(ptr);
//   }
// }

template <typename T>
class Handle {
 public:
  explicit Handle(T* value) : value_(value) {}

  ~Handle() { clean_up(value_); }

 private:
  T* value_;
};

TEST(AdbcDriver, AdbcDriverInit) {
  // Test the get/set option implementation in the base driver
  struct AdbcDriver driver;
  memset(&driver, 0, sizeof(driver));
  ASSERT_EQ(arrow::engine::AceroDriverInitFunc(ADBC_VERSION_1_1_0, &driver, nullptr),
            ADBC_STATUS_OK);
  Handle<AdbcDriver> driver_handle(&driver);
}
