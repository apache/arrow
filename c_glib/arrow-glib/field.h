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

#include <arrow-glib/basic-data-type.h>

G_BEGIN_DECLS

#define GARROW_TYPE_FIELD (garrow_field_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowField,
                         garrow_field,
                         GARROW,
                         FIELD,
                         GObject)
struct _GArrowFieldClass
{
  GObjectClass parent_class;
};

GArrowField    *garrow_field_new           (const gchar *name,
                                            GArrowDataType *data_type);
GArrowField    *garrow_field_new_full      (const gchar *name,
                                            GArrowDataType *data_type,
                                            gboolean nullable);

const gchar    *garrow_field_get_name      (GArrowField *field);
GArrowDataType *garrow_field_get_data_type (GArrowField *field);
gboolean        garrow_field_is_nullable   (GArrowField *field);

gboolean        garrow_field_equal         (GArrowField *field,
                                            GArrowField *other_field);

gchar          *garrow_field_to_string     (GArrowField *field);

G_END_DECLS
