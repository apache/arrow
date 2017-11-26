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

#include <arrow-glib/basic-array.h>
#include <arrow-glib/data-type.h>

G_BEGIN_DECLS

#define GARROW_TYPE_LIST_ARRAY                  \
  (garrow_list_array_get_type())
#define GARROW_LIST_ARRAY(obj)                          \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_LIST_ARRAY,   \
                              GArrowListArray))
#define GARROW_LIST_ARRAY_CLASS(klass)                  \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_LIST_ARRAY,      \
                           GArrowListArrayClass))
#define GARROW_IS_LIST_ARRAY(obj)                       \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                    \
                              GARROW_TYPE_LIST_ARRAY))
#define GARROW_IS_LIST_ARRAY_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_LIST_ARRAY))
#define GARROW_LIST_ARRAY_GET_CLASS(obj)                \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_LIST_ARRAY,    \
                             GArrowListArrayClass))

typedef struct _GArrowListArray         GArrowListArray;
typedef struct _GArrowListArrayClass    GArrowListArrayClass;

/**
 * GArrowListArray:
 *
 * It wraps `arrow::ListArray`.
 */
struct _GArrowListArray
{
  /*< private >*/
  GArrowArray parent_instance;
};

struct _GArrowListArrayClass
{
  GArrowArrayClass parent_class;
};

GType garrow_list_array_get_type(void) G_GNUC_CONST;

GArrowListArray *garrow_list_array_new(gint64 length,
                                       GArrowBuffer *value_offsets,
                                       GArrowArray *values,
                                       GArrowBuffer *null_bitmap,
                                       gint64 n_nulls);

GArrowDataType *garrow_list_array_get_value_type(GArrowListArray *array);
GArrowArray *garrow_list_array_get_value(GArrowListArray *array,
                                         gint64 i);


#define GARROW_TYPE_STRUCT_ARRAY                \
  (garrow_struct_array_get_type())
#define GARROW_STRUCT_ARRAY(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_STRUCT_ARRAY, \
                              GArrowStructArray))
#define GARROW_STRUCT_ARRAY_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_STRUCT_ARRAY,    \
                           GArrowStructArrayClass))
#define GARROW_IS_STRUCT_ARRAY(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_STRUCT_ARRAY))
#define GARROW_IS_STRUCT_ARRAY_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_STRUCT_ARRAY))
#define GARROW_STRUCT_ARRAY_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_STRUCT_ARRAY,  \
                             GArrowStructArrayClass))

typedef struct _GArrowStructArray         GArrowStructArray;
typedef struct _GArrowStructArrayClass    GArrowStructArrayClass;

/**
 * GArrowStructArray:
 *
 * It wraps `arrow::StructArray`.
 */
struct _GArrowStructArray
{
  /*< private >*/
  GArrowArray parent_instance;
};

struct _GArrowStructArrayClass
{
  GArrowArrayClass parent_class;
};

GType garrow_struct_array_get_type(void) G_GNUC_CONST;

GArrowStructArray *garrow_struct_array_new(GArrowDataType *data_type,
                                           gint64 length,
                                           GList *children,
                                           GArrowBuffer *null_bitmap,
                                           gint64 n_nulls);

GArrowArray *garrow_struct_array_get_field(GArrowStructArray *array,
                                           gint i);
GList *garrow_struct_array_get_fields(GArrowStructArray *array);

G_END_DECLS
