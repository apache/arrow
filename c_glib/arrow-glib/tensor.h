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

#include <arrow-glib/buffer.h>
#include <arrow-glib/data-type.h>

G_BEGIN_DECLS

#define GARROW_TYPE_TENSOR (garrow_tensor_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowTensor, garrow_tensor, GARROW, TENSOR, GObject)
struct _GArrowTensorClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowTensor *
garrow_tensor_new(GArrowDataType *data_type,
                  GArrowBuffer *data,
                  gint64 *shape,
                  gsize n_dimensions,
                  gint64 *strides,
                  gsize n_strides,
                  gchar **dimension_names,
                  gsize n_dimension_names);
GARROW_AVAILABLE_IN_ALL
gboolean
garrow_tensor_equal(GArrowTensor *tensor, GArrowTensor *other_tensor);

GARROW_AVAILABLE_IN_ALL
GArrowDataType *
garrow_tensor_get_value_data_type(GArrowTensor *tensor);

GARROW_AVAILABLE_IN_ALL
GArrowType
garrow_tensor_get_value_type(GArrowTensor *tensor);

GARROW_AVAILABLE_IN_ALL
GArrowBuffer *
garrow_tensor_get_buffer(GArrowTensor *tensor);

GARROW_AVAILABLE_IN_ALL
gint64 *
garrow_tensor_get_shape(GArrowTensor *tensor, gint *n_dimensions);

GARROW_AVAILABLE_IN_ALL
gint64 *
garrow_tensor_get_strides(GArrowTensor *tensor, gint *n_strides);

GARROW_AVAILABLE_IN_ALL
gint
garrow_tensor_get_n_dimensions(GArrowTensor *tensor);

GARROW_AVAILABLE_IN_ALL
const gchar *
garrow_tensor_get_dimension_name(GArrowTensor *tensor, gint i);

GARROW_AVAILABLE_IN_ALL
gint64
garrow_tensor_get_size(GArrowTensor *tensor);

GARROW_AVAILABLE_IN_ALL
gboolean
garrow_tensor_is_mutable(GArrowTensor *tensor);

GARROW_AVAILABLE_IN_ALL
gboolean
garrow_tensor_is_contiguous(GArrowTensor *tensor);

GARROW_AVAILABLE_IN_ALL
gboolean
garrow_tensor_is_row_major(GArrowTensor *tensor);

GARROW_AVAILABLE_IN_ALL
gboolean
garrow_tensor_is_column_major(GArrowTensor *tensor);

G_END_DECLS
