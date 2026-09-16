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

// The baseline leg of the interleaved kernel dispatch: this translation unit
// carries no instruction-set flags of its own, so it compiles at whatever the
// build's own flags name. It is the fallback DynamicDispatch resolves to when no
// vector leg is available, and it is the only leg that always exists.

#include "arrow/util/fastlanes/interleaved_dispatch_internal.h"
#include "arrow/util/fastlanes/interleaved_kernel_table_internal.h"

namespace arrow {
namespace util {
namespace fastlanes {

void InterleavedPackBlockBaseline(uint8_t bit_width, const uint32_t* in, uint32_t* out) {
  return PackLeg<>(bit_width, in, out);
}

void InterleavedUnpackBlockBaseline(uint8_t bit_width, const uint32_t* packed,
                                    uint32_t* out, uint32_t bias) {
  return UnpackLeg<>(bit_width, packed, out, bias);
}

}  // namespace fastlanes
}  // namespace util
}  // namespace arrow
