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

#define GARROW_TYPE_COLUMN (garrow_column_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowColumn,
                         garrow_column,
                         GARROW,
                         COLUMN,
                         GObject)
struct _GArrowColumnClass
{
  GObjectClass parent_class;
};

GArrowColumn *garrow_column_new_array(GArrowField *field,
                                      GArrowArray *array);
GArrowColumn *garrow_column_new_chunked_array(GArrowField *field,
                                              GArrowChunkedArray *chunked_array);
GArrowColumn *garrow_column_slice(GArrowColumn *column,
                                  guint64 offset,
                                  guint64 length);

gboolean            garrow_column_equal         (GArrowColumn *column,
                                                 GArrowColumn *other_column);

guint64             garrow_column_get_length    (GArrowColumn *column);
guint64             garrow_column_get_n_nulls   (GArrowColumn *column);
GArrowField        *garrow_column_get_field     (GArrowColumn *column);
const gchar        *garrow_column_get_name      (GArrowColumn *column);
GArrowDataType     *garrow_column_get_data_type (GArrowColumn *column);
GArrowChunkedArray *garrow_column_get_data      (GArrowColumn *column);
gchar              *garrow_column_to_string     (GArrowColumn *column,
                                                 GError **error);

G_END_DECLS
