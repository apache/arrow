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

#include <arrow-glib/tensor.h>

G_BEGIN_DECLS

#define GARROW_TYPE_INT8_TENSOR                 \
  (garrow_int8_tensor_get_type())
#define GARROW_INT8_TENSOR(obj)                         \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_INT8_TENSOR,  \
                              GArrowInt8Tensor))
#define GARROW_INT8_TENSOR_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_INT8_TENSOR,     \
                           GArrowInt8TensorClass))
#define GARROW_IS_INT8_TENSOR(obj)                      \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                    \
                              GARROW_TYPE_INT8_TENSOR))
#define GARROW_IS_INT8_TENSOR_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_INT8_TENSOR))
#define GARROW_INT8_TENSOR_GET_CLASS(obj)               \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_INT8_TENSOR,   \
                             GArrowInt8TensorClass))

typedef struct _GArrowInt8Tensor         GArrowInt8Tensor;
typedef struct _GArrowInt8TensorClass    GArrowInt8TensorClass;

/**
 * GArrowInt8Tensor:
 *
 * It wraps `arrow::Int8Tensor`.
 */
struct _GArrowInt8Tensor
{
  /*< private >*/
  GArrowTensor parent_instance;
};

struct _GArrowInt8TensorClass
{
  GArrowTensorClass parent_class;
};

GType garrow_int8_tensor_get_type(void) G_GNUC_CONST;

GArrowInt8Tensor *garrow_int8_tensor_new(GArrowBuffer *data,
                                         gint64 *shape,
                                         gsize n_dimensions,
                                         gint64 *strides,
                                         gsize n_strides,
                                         gchar **dimention_names,
                                         gsize n_dimention_names);

const gint8 *garrow_int8_tensor_get_raw_data(GArrowInt8Tensor *tensor,
                                             gint64 *n_data);

G_END_DECLS
