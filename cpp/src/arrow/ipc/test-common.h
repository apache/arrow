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

#ifndef ARROW_IPC_TEST_COMMON_H
#define ARROW_IPC_TEST_COMMON_H

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/test-util.h"
#include "arrow/types/list.h"
#include "arrow/types/primitive.h"
#include "arrow/util/buffer.h"
#include "arrow/util/memory-pool.h"

namespace arrow {
namespace ipc {

Status MakeRandomInt32Array(
    int32_t length, bool include_nulls, MemoryPool* pool, std::shared_ptr<Array>* array) {
  std::shared_ptr<PoolBuffer> data;
  test::MakeRandomInt32PoolBuffer(length, pool, &data);
  const auto INT32 = std::make_shared<Int32Type>();
  Int32Builder builder(pool, INT32);
  if (include_nulls) {
    std::shared_ptr<PoolBuffer> valid_bytes;
    test::MakeRandomBytePoolBuffer(length, pool, &valid_bytes);
    RETURN_NOT_OK(builder.Append(
        reinterpret_cast<const int32_t*>(data->data()), length, valid_bytes->data()));
    *array = builder.Finish();
    return Status::OK();
  }
  RETURN_NOT_OK(builder.Append(reinterpret_cast<const int32_t*>(data->data()), length));
  *array = builder.Finish();
  return Status::OK();
}

Status MakeRandomListArray(const std::shared_ptr<Array>& child_array, int num_lists,
    bool include_nulls, MemoryPool* pool, std::shared_ptr<Array>* array) {
  // Create the null list values
  std::vector<uint8_t> valid_lists(num_lists);
  const double null_percent = include_nulls ? 0.1 : 0;
  test::random_null_bytes(num_lists, null_percent, valid_lists.data());

  // Create list offsets
  const int max_list_size = 10;

  std::vector<int32_t> list_sizes(num_lists, 0);
  std::vector<int32_t> offsets(
      num_lists + 1, 0);  // +1 so we can shift for nulls. See partial sum below.
  const int seed = child_array->length();
  if (num_lists > 0) {
    test::rand_uniform_int(num_lists, seed, 0, max_list_size, list_sizes.data());
    // make sure sizes are consistent with null
    std::transform(list_sizes.begin(), list_sizes.end(), valid_lists.begin(),
        list_sizes.begin(),
        [](int32_t size, int32_t valid) { return valid == 0 ? 0 : size; });
    std::partial_sum(list_sizes.begin(), list_sizes.end(), ++offsets.begin());

    // Force invariants
    const int child_length = child_array->length();
    offsets[0] = 0;
    std::replace_if(offsets.begin(), offsets.end(),
        [child_length](int32_t offset) { return offset > child_length; }, child_length);
  }
  ListBuilder builder(pool, child_array);
  RETURN_NOT_OK(builder.Append(offsets.data(), num_lists, valid_lists.data()));
  *array = builder.Finish();
  return (*array)->Validate();
}

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_TEST_COMMON_H
