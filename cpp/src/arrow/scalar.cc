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

#include "arrow/scalar.h"

#include <memory>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

namespace arrow {

FixedSizeBinaryScalar::FixedSizeBinaryScalar(const std::shared_ptr<Buffer>& value,
                                             const std::shared_ptr<DataType>& type,
                                             bool is_valid)
    : BinaryScalar(value, type, is_valid) {
  DCHECK_EQ(checked_cast<const FixedSizeBinaryType&>(*type).byte_width(),
            value->size());
}

DecimalScalar::DecimalScalar(const ValueType& value, const std::shared_ptr<DataType> type,
                             bool is_valid)
    : Scalar(is_valid, type), value_(value) {}

ListScalar::ListScalar(const std::shared_ptr<Array>& value,
                       const std::shared_ptr<DataType>& type,
                       bool is_valid)
    : Scalar(value, type), value(value) {}

}  // namespace arrow
