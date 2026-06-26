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

#include "arrow/array/data.h"

namespace arrow {
namespace dict_util {

int64_t LogicalNullCount(const ArraySpan& span);
void SetLogicalNullBits(const ArraySpan& span, uint8_t* out_bitmap, int64_t out_offset,
                        bool set_on_null);

}  // namespace dict_util
}  // namespace arrow
