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

#include <arrow-glib/array-builder.h>

G_BEGIN_DECLS

#define GARROW_TYPE_UINT32_ARRAY_BUILDER         \
  (garrow_uint32_array_builder_get_type())
#define GARROW_UINT32_ARRAY_BUILDER(obj)                         \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_UINT32_ARRAY_BUILDER,  \
                              GArrowUInt32ArrayBuilder))
#define GARROW_UINT32_ARRAY_BUILDER_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_UINT32_ARRAY_BUILDER,     \
                           GArrowUInt32ArrayBuilderClass))
#define GARROW_IS_UINT32_ARRAY_BUILDER(obj)                      \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_UINT32_ARRAY_BUILDER))
#define GARROW_IS_UINT32_ARRAY_BUILDER_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_UINT32_ARRAY_BUILDER))
#define GARROW_UINT32_ARRAY_BUILDER_GET_CLASS(obj)               \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_UINT32_ARRAY_BUILDER,   \
                             GArrowUInt32ArrayBuilderClass))

typedef struct _GArrowUInt32ArrayBuilder         GArrowUInt32ArrayBuilder;
typedef struct _GArrowUInt32ArrayBuilderClass    GArrowUInt32ArrayBuilderClass;

/**
 * GArrowUInt32ArrayBuilder:
 *
 * It wraps `arrow::UInt32Builder`.
 */
struct _GArrowUInt32ArrayBuilder
{
  /*< private >*/
  GArrowArrayBuilder parent_instance;
};

struct _GArrowUInt32ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GType garrow_uint32_array_builder_get_type(void) G_GNUC_CONST;

GArrowUInt32ArrayBuilder *garrow_uint32_array_builder_new(void);

gboolean garrow_uint32_array_builder_append(GArrowUInt32ArrayBuilder *builder,
                                           guint32 value,
                                           GError **error);
gboolean garrow_uint32_array_builder_append_null(GArrowUInt32ArrayBuilder *builder,
                                                GError **error);

G_END_DECLS
