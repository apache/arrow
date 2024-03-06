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
#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"

#include "arrow/python/lib_acero_api.h"
#include "arrow/python/pyarrow_acero.h"
#include "arrow/python/wrap_macros.h"

namespace arrow {
namespace py {

int import_pyarrow_acero() { return ::import_pyarrow__lib_acero(); }

DEFINE_WRAP_FUNCTIONS(exec_node_options, std::shared_ptr<acero::ExecNodeOptions>, out)
DEFINE_WRAP_FUNCTIONS(declaration, acero::Declaration, out.IsValid());

}  // namespace py
}  // namespace arrow
