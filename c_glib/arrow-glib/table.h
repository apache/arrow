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

#include <arrow-glib/column.h>
#include <arrow-glib/schema.h>

G_BEGIN_DECLS

#define GARROW_TYPE_TABLE (garrow_table_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowTable,
                         garrow_table,
                         GARROW,
                         TABLE,
                         GObject)
struct _GArrowTableClass
{
  GObjectClass parent_class;
};

GArrowTable    *garrow_table_new           (GArrowSchema *schema,
                                            GList *columns);

gboolean        garrow_table_equal         (GArrowTable *table,
                                            GArrowTable *other_table);

GArrowSchema   *garrow_table_get_schema    (GArrowTable *table);
GArrowColumn   *garrow_table_get_column    (GArrowTable *table,
                                            guint i);
guint           garrow_table_get_n_columns (GArrowTable *table);
guint64         garrow_table_get_n_rows    (GArrowTable *table);

GArrowTable    *garrow_table_add_column    (GArrowTable *table,
                                            guint i,
                                            GArrowColumn *column,
                                            GError **error);
GArrowTable    *garrow_table_remove_column (GArrowTable *table,
                                            guint i,
                                            GError **error);
GArrowTable    *garrow_table_replace_column(GArrowTable *table,
                                            guint i,
                                            GArrowColumn *column,
                                            GError **error);

G_END_DECLS
