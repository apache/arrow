/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <arrow-glib/tensor.hpp>

namespace garrow {
  template <typename T>
  GArrowTensor *numeric_tensor_new(GArrowBuffer *data,
                                   gint64 *shape,
                                   gsize n_dimensions,
                                   gint64 *strides,
                                   gsize n_strides,
                                   gchar **dimention_names,
                                   gsize n_dimention_names) {
    auto arrow_data = garrow_buffer_get_raw(data);
    std::vector<int64_t> arrow_shape;
    for (gsize i = 0; i < n_dimensions; ++i) {
      arrow_shape.push_back(shape[i]);
    }
    std::vector<int64_t> arrow_strides;
    for (gsize i = 0; i < n_strides; ++i) {
      arrow_strides.push_back(strides[i]);
    }
    std::vector<std::string> arrow_dimention_names;
    for (gsize i = 0; i < n_dimention_names; ++i) {
      arrow_dimention_names.push_back(dimention_names[i]);
    }
    auto arrow_numeric_tensor =
      std::make_shared<T>(arrow_data,
                          arrow_shape,
                          arrow_strides,
                          arrow_dimention_names);
    std::shared_ptr<arrow::Tensor> arrow_tensor = arrow_numeric_tensor;
    auto tensor = garrow_tensor_new_raw(&arrow_tensor);
    return tensor;
  }

  template <typename T, typename value_type>
  const value_type *numeric_tensor_get_raw_data(GArrowTensor *tensor,
                                                gint64 *n_data) {
    auto arrow_tensor = garrow_tensor_get_raw(tensor);
    auto arrow_numeric_tensor = static_cast<const T *>(arrow_tensor.get());
    *n_data = arrow_numeric_tensor->size();
    return arrow_numeric_tensor->raw_data();
  }
}
