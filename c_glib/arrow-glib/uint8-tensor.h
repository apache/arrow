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

#define GARROW_TYPE_UINT8_TENSOR                \
  (garrow_uint8_tensor_get_type())
#define GARROW_UINT8_TENSOR(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_UINT8_TENSOR, \
                              GArrowUInt8Tensor))
#define GARROW_UINT8_TENSOR_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_UINT8_TENSOR,    \
                           GArrowUInt8TensorClass))
#define GARROW_IS_UINT8_TENSOR(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_UINT8_TENSOR))
#define GARROW_IS_UINT8_TENSOR_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_UINT8_TENSOR))
#define GARROW_UINT8_TENSOR_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_UINT8_TENSOR,  \
                             GArrowUInt8TensorClass))

typedef struct _GArrowUInt8Tensor         GArrowUInt8Tensor;
typedef struct _GArrowUInt8TensorClass    GArrowUInt8TensorClass;

/**
 * GArrowUInt8Tensor:
 *
 * It wraps `arrow::UInt8Tensor`.
 */
struct _GArrowUInt8Tensor
{
  /*< private >*/
  GArrowTensor parent_instance;
};

struct _GArrowUInt8TensorClass
{
  GArrowTensorClass parent_class;
};

GType garrow_uint8_tensor_get_type(void) G_GNUC_CONST;

GArrowUInt8Tensor *garrow_uint8_tensor_new(GArrowBuffer *data,
                                           gint64 *shape,
                                           gsize n_dimensions,
                                           gint64 *strides,
                                           gsize n_strides,
                                           gchar **dimention_names,
                                           gsize n_dimention_names);

const guint8 *garrow_uint8_tensor_get_raw_data(GArrowUInt8Tensor *tensor,
                                               gint64 *n_data);

G_END_DECLS
