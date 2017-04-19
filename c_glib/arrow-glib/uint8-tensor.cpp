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

#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include <arrow-glib/buffer.hpp>
#include <arrow-glib/uint8-tensor.h>
#include <arrow-glib/numeric-tensor.hpp>

G_BEGIN_DECLS

/**
 * SECTION: uint8-tensor
 * @short_description: 8-bit unsigned integer tensor class
 *
 * #GArrowUint8Tensor is a class for 8-bit unsigned integer tensor. It
 * can store zero or more 8-bit integer data.
 */

G_DEFINE_TYPE(GArrowUInt8Tensor,               \
              garrow_uint8_tensor,             \
              GARROW_TYPE_TENSOR)

static void
garrow_uint8_tensor_init(GArrowUInt8Tensor *object)
{
}

static void
garrow_uint8_tensor_class_init(GArrowUInt8TensorClass *klass)
{
}

/**
 * garrow_uint8_tensor_new:
 * @data: A #GArrowBuffer that contains tensor data.
 * @shape: (array length=n_dimensions): A list of dimension sizes.
 * @n_dimensions: The number of dimensions.
 * @strides: (array length=n_strides) (nullable): A list of the number of
 *   bytes in each dimension.
 * @n_strides: The number of strides.
 * @dimention_names: (array length=n_dimention_names) (nullable): A list of
 *   dimension names.
 * @n_dimention_names: The number of dimension names
 *
 * Returns: The newly created #GArrowUInt8Tensor.
 *
 * Since: 0.3.0
 */
GArrowUInt8Tensor *
garrow_uint8_tensor_new(GArrowBuffer *data,
                       gint64 *shape,
                       gsize n_dimensions,
                       gint64 *strides,
                       gsize n_strides,
                       gchar **dimension_names,
                       gsize n_dimension_names)
{
  auto tensor =
    garrow::numeric_tensor_new<arrow::UInt8Tensor>(data,
                                                  shape,
                                                  n_dimensions,
                                                  strides,
                                                  n_strides,
                                                  dimension_names,
                                                  n_dimension_names);
  return GARROW_UINT8_TENSOR(tensor);
}

/**
 * garrow_uint8_tensor_get_raw_data:
 * @tensor: A #GArrowUInt8Tensor.
 * @n_data: (out): The number of data.
 *
 * Returns: (array length=n_data): The raw data in the tensor.
 *
 * Since: 0.3.0
 */
const guint8 *
garrow_uint8_tensor_get_raw_data(GArrowUInt8Tensor *tensor,
                                 gint64 *n_data)
{
  return garrow::numeric_tensor_get_raw_data<arrow::UInt8Tensor, uint8_t>(GARROW_TENSOR(tensor),
                                                                          n_data);
}

G_END_DECLS
