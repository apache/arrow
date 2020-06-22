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

// IWYU pragma: begin_exports

#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array/data.h"
#include "arrow/buffer.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/function.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/datum.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/string_view.h"

// IWYU pragma: end_exports

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

}  // namespace arrow
