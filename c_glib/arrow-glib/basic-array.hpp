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

#include <arrow/api.h>

#include <arrow-glib/basic-array.h>

arrow::EqualOptions *
garrow_equal_options_get_raw(GArrowEqualOptions *equal_options);

GArrowArray *
garrow_array_new_raw(std::shared_ptr<arrow::Array> *arrow_array);
GArrowArray *
garrow_array_new_raw(std::shared_ptr<arrow::Array> *arrow_array,
                     const gchar *first_property_name,
                     ...);
GArrowArray *
garrow_array_new_raw_valist(std::shared_ptr<arrow::Array> *arrow_array,
                            const gchar *first_property_name,
                            va_list args);
GArrowExtensionArray *
garrow_extension_array_new_raw(std::shared_ptr<arrow::Array> *arrow_array,
                               GArrowArray *storage);
std::shared_ptr<arrow::Array>
garrow_array_get_raw(GArrowArray *array);

template <typename DataType>
inline std::shared_ptr<typename arrow::TypeTraits<DataType>::ArrayType>
garrow_array_get_raw(GArrowArray *array) {
  auto arrow_array = garrow_array_get_raw(array);
  return std::static_pointer_cast<typename arrow::TypeTraits<DataType>::ArrayType>(arrow_array);
}
