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

#ifndef GANDIVA_LITERAL_HOLDER
#define GANDIVA_LITERAL_HOLDER

#include <string>

#include <arrow/util/variant.h>

#include <arrow/type.h>
#include "gandiva/decimal_scalar.h"

namespace gandiva {

using LiteralHolder =
    arrow::util::variant<bool, float, double, int8_t, int16_t, int32_t, int64_t, uint8_t,
                         uint16_t, uint32_t, uint64_t, std::string, DecimalScalar128>;
}  // namespace gandiva

#endif  // GANDIVA_LITERAL_HOLDER
