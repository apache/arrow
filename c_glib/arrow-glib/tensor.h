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

#define GARROW_TYPE_TENSOR \
  (garrow_tensor_get_type())
#define GARROW_TENSOR(obj) \
  (G_TYPE_CHECK_INSTANCE_CAST((obj), GARROW_TYPE_TENSOR, GArrowTensor))
#define GARROW_TENSOR_CLASS(klass) \
  (G_TYPE_CHECK_CLASS_CAST((klass), GARROW_TYPE_TENSOR, GArrowTensorClass))
#define GARROW_IS_TENSOR(obj) \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj), GARROW_TYPE_TENSOR))
#define GARROW_IS_TENSOR_CLASS(klass) \
  (G_TYPE_CHECK_CLASS_TYPE((klass), GARROW_TYPE_TENSOR))
#define GARROW_TENSOR_GET_CLASS(obj) \
  (G_TYPE_INSTANCE_GET_CLASS((obj), GARROW_TYPE_TENSOR, GArrowTensorClass))

typedef struct _GArrowTensor         GArrowTensor;
typedef struct _GArrowTensorClass    GArrowTensorClass;

/**
 * GArrowTensor:
 *
 * It wraps `arrow::Tensor`.
 */
struct _GArrowTensor
{
  /*< private >*/
  GObject parent_instance;
};

struct _GArrowTensorClass
{
  GObjectClass parent_class;
};

GType           garrow_tensor_get_type           (void) G_GNUC_CONST;

GArrowDataType *garrow_tensor_get_value_data_type(GArrowTensor *tensor);
GArrowType      garrow_tensor_get_value_type     (GArrowTensor *tensor);
GArrowBuffer   *garrow_tensor_get_buffer         (GArrowTensor *tensor);
gint64         *garrow_tensor_get_shape          (GArrowTensor *tensor,
                                                  gint *n_dimensions);
gint64         *garrow_tensor_get_strides        (GArrowTensor *tensor,
                                                  gint *n_strides);
gint            garrow_tensor_get_n_dimensions   (GArrowTensor *tensor);
const gchar    *garrow_tensor_get_dimension_name (GArrowTensor *tensor,
                                                  gint i);
gint64          garrow_tensor_get_size           (GArrowTensor *tensor);
gboolean        garrow_tensor_is_mutable         (GArrowTensor *tensor);
gboolean        garrow_tensor_is_contiguous      (GArrowTensor *tensor);
gboolean        garrow_tensor_is_row_major       (GArrowTensor *tensor);
gboolean        garrow_tensor_is_column_major    (GArrowTensor *tensor);

G_END_DECLS
