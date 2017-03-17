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

#include <arrow-glib/array.h>
#include <arrow-glib/chunked-array.h>
#include <arrow-glib/field.h>

G_BEGIN_DECLS

#define GARROW_TYPE_COLUMN                      \
  (garrow_column_get_type())
#define GARROW_COLUMN(obj)                              \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_COLUMN,       \
                              GArrowColumn))
#define GARROW_COLUMN_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_CAST((klass),             \
                           GARROW_TYPE_COLUMN,  \
                           GArrowColumnClass))
#define GARROW_IS_COLUMN(obj)                           \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                    \
                              GARROW_TYPE_COLUMN))
#define GARROW_IS_COLUMN_CLASS(klass)           \
  (G_TYPE_CHECK_CLASS_TYPE((klass),             \
                           GARROW_TYPE_COLUMN))
#define GARROW_COLUMN_GET_CLASS(obj)                    \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_COLUMN,        \
                             GArrowColumnClass))

typedef struct _GArrowColumn         GArrowColumn;
typedef struct _GArrowColumnClass    GArrowColumnClass;

/**
 * GArrowColumn:
 *
 * It wraps `arrow::Column`.
 */
struct _GArrowColumn
{
  /*< private >*/
  GObject parent_instance;
};

struct _GArrowColumnClass
{
  GObjectClass parent_class;
};

GType               garrow_column_get_type      (void) G_GNUC_CONST;

GArrowColumn *garrow_column_new_array(GArrowField *field,
                                      GArrowArray *array);
GArrowColumn *garrow_column_new_chunked_array(GArrowField *field,
                                              GArrowChunkedArray *chunked_array);

guint64             garrow_column_get_length    (GArrowColumn *column);
guint64             garrow_column_get_n_nulls   (GArrowColumn *column);
GArrowField        *garrow_column_get_field     (GArrowColumn *column);
const gchar        *garrow_column_get_name      (GArrowColumn *column);
GArrowDataType     *garrow_column_get_data_type (GArrowColumn *column);
GArrowChunkedArray *garrow_column_get_data      (GArrowColumn *column);

G_END_DECLS
