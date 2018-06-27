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

#include <arrow-glib/field.h>

G_BEGIN_DECLS

#define GARROW_TYPE_SCHEMA (garrow_schema_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowSchema,
                         garrow_schema,
                         GARROW,
                         SCHEMA,
                         GObject)
struct _GArrowSchemaClass
{
  GObjectClass parent_class;
};

GArrowSchema    *garrow_schema_new              (GList *fields);

gboolean         garrow_schema_equal            (GArrowSchema *schema,
                                                 GArrowSchema *other_schema);
GArrowField     *garrow_schema_get_field        (GArrowSchema *schema,
                                                 guint i);
GArrowField     *garrow_schema_get_field_by_name(GArrowSchema *schema,
                                                 const gchar *name);

guint            garrow_schema_n_fields         (GArrowSchema *schema);
GList           *garrow_schema_get_fields       (GArrowSchema *schema);

gchar           *garrow_schema_to_string        (GArrowSchema *schema);

GArrowSchema    *garrow_schema_add_field        (GArrowSchema *schema,
                                                 guint i,
                                                 GArrowField *field,
                                                 GError **error);
GArrowSchema    *garrow_schema_remove_field     (GArrowSchema *schema,
                                                 guint i,
                                                 GError **error);
GArrowSchema    *garrow_schema_replace_field    (GArrowSchema *schema,
                                                 guint i,
                                                 GArrowField *field,
                                                 GError **error);

G_END_DECLS
