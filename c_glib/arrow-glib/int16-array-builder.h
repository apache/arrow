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

#define GARROW_TYPE_INT16_ARRAY_BUILDER          \
  (garrow_int16_array_builder_get_type())
#define GARROW_INT16_ARRAY_BUILDER(obj)                          \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_INT16_ARRAY_BUILDER,   \
                              GArrowInt16ArrayBuilder))
#define GARROW_INT16_ARRAY_BUILDER_CLASS(klass)                  \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_INT16_ARRAY_BUILDER,      \
                           GArrowInt16ArrayBuilderClass))
#define GARROW_IS_INT16_ARRAY_BUILDER(obj)                       \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_INT16_ARRAY_BUILDER))
#define GARROW_IS_INT16_ARRAY_BUILDER_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_INT16_ARRAY_BUILDER))
#define GARROW_INT16_ARRAY_BUILDER_GET_CLASS(obj)                \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_INT16_ARRAY_BUILDER,    \
                             GArrowInt16ArrayBuilderClass))

typedef struct _GArrowInt16ArrayBuilder         GArrowInt16ArrayBuilder;
typedef struct _GArrowInt16ArrayBuilderClass    GArrowInt16ArrayBuilderClass;

/**
 * GArrowInt16ArrayBuilder:
 *
 * It wraps `arrow::Int16Builder`.
 */
struct _GArrowInt16ArrayBuilder
{
  /*< private >*/
  GArrowArrayBuilder parent_instance;
};

struct _GArrowInt16ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GType garrow_int16_array_builder_get_type(void) G_GNUC_CONST;

GArrowInt16ArrayBuilder *garrow_int16_array_builder_new(void);

gboolean garrow_int16_array_builder_append(GArrowInt16ArrayBuilder *builder,
                                          gint16 value,
                                          GError **error);
gboolean garrow_int16_array_builder_append_null(GArrowInt16ArrayBuilder *builder,
                                               GError **error);

G_END_DECLS
