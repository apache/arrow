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

#include "gandiva/decimal_type_util.h"
#include "gandiva/logging.h"

namespace gandiva {

constexpr int32_t DecimalTypeUtil::kMinAdjustedScale;

#define DCHECK_TYPE(type)                        \
  {                                              \
    DCHECK_GE(type->scale(), 0);                 \
    DCHECK_LE(type->precision(), kMaxPrecision); \
  }

// Implementation of decimal rules.
Status DecimalTypeUtil::GetResultType(Op op, const Decimal128TypeVector& in_types,
                                      Decimal128TypePtr* out_type) {
  DCHECK_EQ(in_types.size(), 2);

  *out_type = nullptr;
  auto t1 = in_types[0];
  auto t2 = in_types[1];
  DCHECK_TYPE(t1);
  DCHECK_TYPE(t2);

  int32_t s1 = t1->scale();
  int32_t s2 = t2->scale();
  int32_t p1 = t1->precision();
  int32_t p2 = t2->precision();
  int32_t result_scale = 0;
  int32_t result_precision = 0;

  switch (op) {
    case kOpAdd:
    case kOpSubtract:
      result_scale = std::max(s1, s2);
      result_precision = std::max(p1 - s1, p2 - s2) + result_scale + 1;
      break;

    case kOpMultiply:
      result_scale = s1 + s2;
      result_precision = p1 + p2 + 1;
      break;

    case kOpDivide:
      result_scale = std::max(kMinAdjustedScale, s1 + p2 + 1);
      result_precision = p1 - s1 + s2 + result_scale;
      break;

    case kOpMod:
      result_scale = std::max(s1, s2);
      result_precision = std::min(p1 - s1, p2 - s2) + result_scale;
      break;
  }
  *out_type = MakeAdjustedType(result_precision, result_scale);
  return Status::OK();
}

}  // namespace gandiva
