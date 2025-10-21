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

#include "arrow/util/visibility.h"

#include <cstdint>

namespace arrow::internal {

template <typename Uint>
ARROW_EXPORT int unpack_scalar(const uint8_t* in, Uint* out, int batch_size,
                               int num_bits);

extern template ARROW_TEMPLATE_EXPORT int unpack_scalar<bool>(const uint8_t* in,
                                                              bool* out, int batch_size,
                                                              int num_bits);

extern template ARROW_TEMPLATE_EXPORT int unpack_scalar<uint8_t>(const uint8_t* in,
                                                                 uint8_t* out,
                                                                 int batch_size,
                                                                 int num_bits);

extern template ARROW_TEMPLATE_EXPORT int unpack_scalar<uint16_t>(const uint8_t* in,
                                                                  uint16_t* out,
                                                                  int batch_size,
                                                                  int num_bits);

extern template ARROW_TEMPLATE_EXPORT int unpack_scalar<uint32_t>(const uint8_t* in,
                                                                  uint32_t* out,
                                                                  int batch_size,
                                                                  int num_bits);

extern template ARROW_TEMPLATE_EXPORT int unpack_scalar<uint64_t>(const uint8_t* in,
                                                                  uint64_t* out,
                                                                  int batch_size,
                                                                  int num_bits);

}  // namespace arrow::internal
