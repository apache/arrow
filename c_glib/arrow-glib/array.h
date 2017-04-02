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

#include <arrow-glib/data-type.h>

G_BEGIN_DECLS

#define GARROW_TYPE_ARRAY \
  (garrow_array_get_type())
#define GARROW_ARRAY(obj) \
  (G_TYPE_CHECK_INSTANCE_CAST((obj), GARROW_TYPE_ARRAY, GArrowArray))
#define GARROW_ARRAY_CLASS(klass) \
  (G_TYPE_CHECK_CLASS_CAST((klass), GARROW_TYPE_ARRAY, GArrowArrayClass))
#define GARROW_IS_ARRAY(obj) \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj), GARROW_TYPE_ARRAY))
#define GARROW_IS_ARRAY_CLASS(klass) \
  (G_TYPE_CHECK_CLASS_TYPE((klass), GARROW_TYPE_ARRAY))
#define GARROW_ARRAY_GET_CLASS(obj) \
  (G_TYPE_INSTANCE_GET_CLASS((obj), GARROW_TYPE_ARRAY, GArrowArrayClass))

typedef struct _GArrowArray         GArrowArray;
typedef struct _GArrowArrayClass    GArrowArrayClass;

/**
 * GArrowArray:
 *
 * It wraps `arrow::Array`.
 */
struct _GArrowArray
{
  /*< private >*/
  GObject parent_instance;
};

struct _GArrowArrayClass
{
  GObjectClass parent_class;
};

GType          garrow_array_get_type    (void) G_GNUC_CONST;

gboolean       garrow_array_is_null     (GArrowArray *array,
                                         gint64 i);
gint64         garrow_array_get_length  (GArrowArray *array);
gint64         garrow_array_get_offset  (GArrowArray *array);
gint64         garrow_array_get_n_nulls (GArrowArray *array);
GArrowDataType *garrow_array_get_data_type(GArrowArray *array);
GArrowArray   *garrow_array_slice       (GArrowArray *array,
                                         gint64 offset,
                                         gint64 length);

G_END_DECLS
