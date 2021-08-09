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

GARROW_AVAILABLE_IN_6_0
GArrowSchema *
garrow_schema_import(gpointer c_abi_schema,
                     GError **error);

GArrowSchema    *garrow_schema_new              (GList *fields);

GARROW_AVAILABLE_IN_6_0
gpointer
garrow_schema_export(GArrowSchema *schema,
                     GError **error);

gboolean         garrow_schema_equal            (GArrowSchema *schema,
                                                 GArrowSchema *other_schema);
GArrowField     *garrow_schema_get_field        (GArrowSchema *schema,
                                                 guint i);
GArrowField     *garrow_schema_get_field_by_name(GArrowSchema *schema,
                                                 const gchar *name);
GARROW_AVAILABLE_IN_0_15
gint             garrow_schema_get_field_index  (GArrowSchema *schema,
                                                 const gchar *name);

guint            garrow_schema_n_fields         (GArrowSchema *schema);
GList           *garrow_schema_get_fields       (GArrowSchema *schema);

gchar *garrow_schema_to_string(GArrowSchema *schema);
GARROW_AVAILABLE_IN_0_17
gchar *garrow_schema_to_string_metadata(GArrowSchema *schema,
                                        gboolean show_metadata);

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

GARROW_AVAILABLE_IN_3_0
gboolean
garrow_schema_has_metadata(GArrowSchema *schema);
GARROW_AVAILABLE_IN_0_17
GHashTable *
garrow_schema_get_metadata(GArrowSchema *schema);
GARROW_AVAILABLE_IN_0_17
GArrowSchema *
garrow_schema_with_metadata(GArrowSchema *schema,
                            GHashTable *metadata);

G_END_DECLS
