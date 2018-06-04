/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <vector>
#include <utility>
#include "arrow/test-util.h"
#include "gandiva/arrow.h"

#ifndef GANDIVA_TEST_UTIL_H
#define GANDIVA_TEST_UTIL_H

namespace gandiva {

/*
 * Helper function to create an arrow-array of type ARROWTYPE
 * from primitive vectors of data & validity.
 *
 * arrow/test-util.h has good utility classes for this purpose.
 * Using those
 */
template<typename TYPE, typename C_TYPE>
static ArrayPtr MakeArrowArray(std::vector<C_TYPE> values,
                               std::vector<bool> validity) {
  ArrayPtr out;
  arrow::ArrayFromVector<TYPE, C_TYPE>(validity, values, &out);
  return out;
}
#define MakeArrowArrayInt32 MakeArrowArray<arrow::Int32Type, int32_t>
#define MakeArrowArrayFloat32 MakeArrowArray<arrow::FloatType, float_t>
#define MakeArrowArrayBool MakeArrowArray<arrow::BooleanType, bool>

} // namespace gandiva

#endif // GANDIVA_TEST_UTIL_H
