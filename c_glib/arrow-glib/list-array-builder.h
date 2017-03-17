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

#define GARROW_TYPE_LIST_ARRAY_BUILDER          \
  (garrow_list_array_builder_get_type())
#define GARROW_LIST_ARRAY_BUILDER(obj)                          \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_LIST_ARRAY_BUILDER,   \
                              GArrowListArrayBuilder))
#define GARROW_LIST_ARRAY_BUILDER_CLASS(klass)                  \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_LIST_ARRAY_BUILDER,      \
                           GArrowListArrayBuilderClass))
#define GARROW_IS_LIST_ARRAY_BUILDER(obj)                       \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_LIST_ARRAY_BUILDER))
#define GARROW_IS_LIST_ARRAY_BUILDER_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_LIST_ARRAY_BUILDER))
#define GARROW_LIST_ARRAY_BUILDER_GET_CLASS(obj)                \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_LIST_ARRAY_BUILDER,    \
                             GArrowListArrayBuilderClass))

typedef struct _GArrowListArrayBuilder         GArrowListArrayBuilder;
typedef struct _GArrowListArrayBuilderClass    GArrowListArrayBuilderClass;

/**
 * GArrowListArrayBuilder:
 *
 * It wraps `arrow::ListBuilder`.
 */
struct _GArrowListArrayBuilder
{
  /*< private >*/
  GArrowArrayBuilder parent_instance;
};

struct _GArrowListArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GType garrow_list_array_builder_get_type(void) G_GNUC_CONST;

GArrowListArrayBuilder *garrow_list_array_builder_new(GArrowArrayBuilder *value_builder);

gboolean garrow_list_array_builder_append(GArrowListArrayBuilder *builder,
                                          GError **error);
gboolean garrow_list_array_builder_append_null(GArrowListArrayBuilder *builder,
                                               GError **error);

GArrowArrayBuilder *garrow_list_array_builder_get_value_builder(GArrowListArrayBuilder *builder);

G_END_DECLS
