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

#include "arrow/python/visibility.h"
#include "arrow/python/wrap_macros.h"

// Work around ARROW-2317 (C linkage warning from Cython)
extern "C++" {

namespace arrow {

// Forward declarations. Actual wrappers/unwrappers are in pyarrow.{h,cc}
namespace acero {
struct Declaration;
class ExecNodeOptions;
}  // namespace acero

namespace py {

ARROW_PYTHON_EXPORT int import_pyarrow_acero();

DECLARE_WRAP_FUNCTIONS(declaration, acero::Declaration)
DECLARE_WRAP_FUNCTIONS(exec_node_options, std::shared_ptr<acero::ExecNodeOptions>)

// If status is ok, return 0.
// If status is not ok, set Python error indicator and return -1.
ARROW_PYTHON_EXPORT int check_status(const Status& status);
}  // namespace py
}  // namespace arrow

}  // extern "C++"
