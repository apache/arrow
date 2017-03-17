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

#define GARROW_TYPE_SCHEMA                      \
  (garrow_schema_get_type())
#define GARROW_SCHEMA(obj)                              \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_SCHEMA,       \
                              GArrowSchema))
#define GARROW_SCHEMA_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_CAST((klass),             \
                           GARROW_TYPE_SCHEMA,  \
                           GArrowSchemaClass))
#define GARROW_IS_SCHEMA(obj)                           \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                    \
                              GARROW_TYPE_SCHEMA))
#define GARROW_IS_SCHEMA_CLASS(klass)           \
  (G_TYPE_CHECK_CLASS_TYPE((klass),             \
                           GARROW_TYPE_SCHEMA))
#define GARROW_SCHEMA_GET_CLASS(obj)                    \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_SCHEMA,        \
                             GArrowSchemaClass))

typedef struct _GArrowSchema         GArrowSchema;
typedef struct _GArrowSchemaClass    GArrowSchemaClass;

/**
 * GArrowSchema:
 *
 * It wraps `arrow::Schema`.
 */
struct _GArrowSchema
{
  /*< private >*/
  GObject parent_instance;
};

struct _GArrowSchemaClass
{
  GObjectClass parent_class;
};

GType            garrow_schema_get_type         (void) G_GNUC_CONST;

GArrowSchema    *garrow_schema_new              (GList *fields);

GArrowField     *garrow_schema_get_field        (GArrowSchema *schema,
                                                 guint i);
GArrowField     *garrow_schema_get_field_by_name(GArrowSchema *schema,
                                                 const gchar *name);

guint            garrow_schema_n_fields         (GArrowSchema *schema);
GList           *garrow_schema_get_fields       (GArrowSchema *schema);

gchar           *garrow_schema_to_string        (GArrowSchema *schema);

G_END_DECLS
