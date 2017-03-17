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

#define GARROW_TYPE_TABLE                       \
  (garrow_table_get_type())
#define GARROW_TABLE(obj)                               \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_TABLE,        \
                              GArrowTable))
#define GARROW_TABLE_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_CAST((klass),             \
                           GARROW_TYPE_TABLE,   \
                           GArrowTableClass))
#define GARROW_IS_TABLE(obj)                            \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                    \
                              GARROW_TYPE_TABLE))
#define GARROW_IS_TABLE_CLASS(klass)            \
  (G_TYPE_CHECK_CLASS_TYPE((klass),             \
                           GARROW_TYPE_TABLE))
#define GARROW_TABLE_GET_CLASS(obj)             \
  (G_TYPE_INSTANCE_GET_CLASS((obj),             \
                             GARROW_TYPE_TABLE, \
                             GArrowTableClass))

typedef struct _GArrowTable         GArrowTable;
typedef struct _GArrowTableClass    GArrowTableClass;

/**
 * GArrowTable:
 *
 * It wraps `arrow::Table`.
 */
struct _GArrowTable
{
  /*< private >*/
  GObject parent_instance;
};

struct _GArrowTableClass
{
  GObjectClass parent_class;
};

GType           garrow_table_get_type      (void) G_GNUC_CONST;

GArrowTable    *garrow_table_new           (const gchar *name,
                                            GArrowSchema *schema,
                                            GList *columns);

const gchar    *garrow_table_get_name      (GArrowTable *table);
GArrowSchema   *garrow_table_get_schema    (GArrowTable *table);
GArrowColumn   *garrow_table_get_column    (GArrowTable *table,
                                            guint i);
guint           garrow_table_get_n_columns (GArrowTable *table);
guint64         garrow_table_get_n_rows    (GArrowTable *table);

G_END_DECLS
