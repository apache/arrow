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

// Private header, not to be exported

#pragma once

#include "arrow/array_all.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

#define ARRAY_VISIT_INLINE(TYPE_CLASS)                                                   \
  case TYPE_CLASS##Type::type_id:                                                        \
    return visitor->Visit(                                                               \
        internal::checked_cast<const typename TypeTraits<TYPE_CLASS##Type>::ArrayType&>( \
            array));

template <typename VISITOR>
inline Status VisitArrayInline(const Array& array, VISITOR* visitor) {
  switch (array.type_id()) {
    ARROW_GENERATE_FOR_ALL_TYPES(ARRAY_VISIT_INLINE);
    case Type::INTERVAL: {
      const auto& interval_type = dynamic_cast<const IntervalType&>(*array.type());
      if (interval_type.interval_type() == IntervalType::MONTHS) {
        return visitor->Visit(internal::checked_cast<const MonthIntervalArray&>(array));
      }
      if (interval_type.interval_type() == IntervalType::DAY_TIME) {
        return visitor->Visit(internal::checked_cast<const DayTimeIntervalArray&>(array));
      }
      break;
    }

    default:
      break;
  }
  return Status::NotImplemented("Type not implemented");
}

#undef ARRAY_VISIT_INLINE

}  // namespace arrow
