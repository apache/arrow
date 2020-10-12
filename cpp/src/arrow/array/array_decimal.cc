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

#include "arrow/array/array_decimal.h"

#include <cstdint>
#include <memory>
#include <string>

#include "arrow/array/array_binary.h"
#include "arrow/array/data.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;


template<uint32_t width>
BaseDecimalArray<width>::BaseDecimalArray(const std::shared_ptr<ArrayData>& data)
    : FixedSizeBinaryArray(data) {
  ARROW_CHECK_EQ(data->type->id(), DecimalTypeTraits<width>::Id);
}

template<uint32_t width>
std::string BaseDecimalArray<width>::FormatValue(int64_t i) const {
  const auto& type_ = checked_cast<const typename DecimalTypeTraits<width>::TypeClass&>(*type());
  const typename DecimalTypeTraits<width>::ValueType value(GetValue(i));
  return value.ToString(type_.scale());
}

template class BaseDecimalArray<128>;
template class BaseDecimalArray<256>;

}  // namespace arrow
