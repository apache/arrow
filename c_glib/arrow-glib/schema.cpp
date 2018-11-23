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

#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include <arrow-glib/error.hpp>
#include <arrow-glib/field.hpp>
#include <arrow-glib/schema.hpp>

G_BEGIN_DECLS

/**
 * SECTION: schema
 * @short_description: Schema class
 *
 * #GArrowSchema is a class for schema. Schema is metadata of a
 * table. It has zero or more #GArrowFields.
 */

typedef struct GArrowSchemaPrivate_ {
  std::shared_ptr<arrow::Schema> schema;
} GArrowSchemaPrivate;

enum {
  PROP_0,
  PROP_SCHEMA
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowSchema,
                           garrow_schema,
                           G_TYPE_OBJECT)

#define GARROW_SCHEMA_GET_PRIVATE(obj)         \
  static_cast<GArrowSchemaPrivate *>(          \
     garrow_schema_get_instance_private(       \
       GARROW_SCHEMA(obj)))

static void
garrow_schema_finalize(GObject *object)
{
  GArrowSchemaPrivate *priv;

  priv = GARROW_SCHEMA_GET_PRIVATE(object);

  priv->schema = nullptr;

  G_OBJECT_CLASS(garrow_schema_parent_class)->finalize(object);
}

static void
garrow_schema_set_property(GObject *object,
                           guint prop_id,
                           const GValue *value,
                           GParamSpec *pspec)
{
  GArrowSchemaPrivate *priv;

  priv = GARROW_SCHEMA_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_SCHEMA:
    priv->schema =
      *static_cast<std::shared_ptr<arrow::Schema> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_schema_get_property(GObject *object,
                           guint prop_id,
                           GValue *value,
                           GParamSpec *pspec)
{
  switch (prop_id) {
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_schema_init(GArrowSchema *object)
{
}

static void
garrow_schema_class_init(GArrowSchemaClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_schema_finalize;
  gobject_class->set_property = garrow_schema_set_property;
  gobject_class->get_property = garrow_schema_get_property;

  spec = g_param_spec_pointer("schema",
                              "Schema",
                              "The raw std::shared<arrow::Schema> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_SCHEMA, spec);
}

/**
 * garrow_schema_new:
 * @fields: (element-type GArrowField): The fields of the schema.
 *
 * Returns: A newly created #GArrowSchema.
 */
GArrowSchema *
garrow_schema_new(GList *fields)
{
  std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
  for (GList *node = fields; node; node = node->next) {
    GArrowField *field = GARROW_FIELD(node->data);
    arrow_fields.push_back(garrow_field_get_raw(field));
  }

  auto arrow_schema = std::make_shared<arrow::Schema>(arrow_fields);
  return garrow_schema_new_raw(&arrow_schema);
}

/**
 * garrow_schema_equal:
 * @schema: A #GArrowSchema.
 * @other_schema: A #GArrowSchema to be compared.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 *
 * Since: 0.4.0
 */
gboolean
garrow_schema_equal(GArrowSchema *schema, GArrowSchema *other_schema)
{
  const auto arrow_schema = garrow_schema_get_raw(schema);
  const auto arrow_other_schema = garrow_schema_get_raw(other_schema);
  return arrow_schema->Equals(*arrow_other_schema);
}

/**
 * garrow_schema_get_field:
 * @schema: A #GArrowSchema.
 * @i: The index of the target field.
 *
 * Returns: (transfer full): The i-th field of the schema.
 */
GArrowField *
garrow_schema_get_field(GArrowSchema *schema, guint i)
{
  const auto arrow_schema = garrow_schema_get_raw(schema);
  auto arrow_field = arrow_schema->field(i);
  return garrow_field_new_raw(&arrow_field);
}

/**
 * garrow_schema_get_field_by_name:
 * @schema: A #GArrowSchema.
 * @name: The name of the field to be found.
 *
 * Returns: (transfer full): The found field or %NULL.
 */
GArrowField *
garrow_schema_get_field_by_name(GArrowSchema *schema,
                                const gchar *name)
{
  const auto arrow_schema = garrow_schema_get_raw(schema);
  auto arrow_field = arrow_schema->GetFieldByName(std::string(name));
  if (arrow_field == nullptr) {
    return NULL;
  } else {
    return garrow_field_new_raw(&arrow_field);
  }
}

/**
 * garrow_schema_n_fields:
 * @schema: A #GArrowSchema.
 *
 * Returns: The number of fields of the schema.
 */
guint
garrow_schema_n_fields(GArrowSchema *schema)
{
  const auto arrow_schema = garrow_schema_get_raw(schema);
  return arrow_schema->num_fields();
}

/**
 * garrow_schema_get_fields:
 * @schema: A #GArrowSchema.
 *
 * Returns: (element-type GArrowField) (transfer full):
 *   The fields of the schema.
 */
GList *
garrow_schema_get_fields(GArrowSchema *schema)
{
  const auto arrow_schema = garrow_schema_get_raw(schema);

  GList *fields = NULL;
  for (auto arrow_field : arrow_schema->fields()) {
    GArrowField *field = garrow_field_new_raw(&arrow_field);
    fields = g_list_prepend(fields, field);
  }

  return g_list_reverse(fields);
}

/**
 * garrow_schema_to_string:
 * @schema: A #GArrowSchema.
 *
 * Returns: The string representation of the schema.
 */
gchar *
garrow_schema_to_string(GArrowSchema *schema)
{
  const auto arrow_schema = garrow_schema_get_raw(schema);
  return g_strdup(arrow_schema->ToString().c_str());
}

/**
 * garrow_schema_add_field:
 * @schema: A #GArrowSchema.
 * @i: The index of the new field.
 * @field: The field to be added.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The newly allocated
 * #GArrowSchema that has a new field or %NULL on error.
 *
 * Since: 0.10.0
 */
GArrowSchema *
garrow_schema_add_field(GArrowSchema *schema,
                        guint i,
                        GArrowField *field,
                        GError **error)
{
  const auto arrow_schema = garrow_schema_get_raw(schema);
  const auto arrow_field = garrow_field_get_raw(field);
  std::shared_ptr<arrow::Schema> arrow_new_schema;
  auto status = arrow_schema->AddField(i, arrow_field, &arrow_new_schema);
  if (garrow_error_check(error, status, "[schema][add-field]")) {
    return garrow_schema_new_raw(&arrow_new_schema);
  } else {
    return NULL;
  }
}

/**
 * garrow_schema_remove_field:
 * @schema: A #GArrowSchema.
 * @i: The index of the field to be removed.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The newly allocated
 * #GArrowSchema that doesn't have the field or %NULL on error.
 *
 * Since: 0.10.0
 */
GArrowSchema *
garrow_schema_remove_field(GArrowSchema *schema,
                           guint i,
                           GError **error)
{
  const auto arrow_schema = garrow_schema_get_raw(schema);
  std::shared_ptr<arrow::Schema> arrow_new_schema;
  auto status = arrow_schema->RemoveField(i, &arrow_new_schema);
  if (garrow_error_check(error, status, "[schema][remove-field]")) {
    return garrow_schema_new_raw(&arrow_new_schema);
  } else {
    return NULL;
  }
}

/**
 * garrow_schema_replace_field:
 * @schema: A #GArrowSchema.
 * @i: The index of the field to be replaced.
 * @field: The newly added #GArrowField.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The newly allocated
 * #GArrowSchema that has @field as the @i-th field or %NULL on error.
 *
 * Since: 0.10.0
 */
GArrowSchema *
garrow_schema_replace_field(GArrowSchema *schema,
                            guint i,
                            GArrowField *field,
                            GError **error)
{
  const auto arrow_schema = garrow_schema_get_raw(schema);
  const auto arrow_field = garrow_field_get_raw(field);
  std::shared_ptr<arrow::Schema> arrow_new_schema;
  auto status = arrow_schema->SetField(i, arrow_field, &arrow_new_schema);
  if (garrow_error_check(error, status, "[schema][replace-field]")) {
    return garrow_schema_new_raw(&arrow_new_schema);
  } else {
    return NULL;
  }
}

G_END_DECLS

GArrowSchema *
garrow_schema_new_raw(std::shared_ptr<arrow::Schema> *arrow_schema)
{
  auto schema = GARROW_SCHEMA(g_object_new(GARROW_TYPE_SCHEMA,
                                           "schema", arrow_schema,
                                           NULL));
  return schema;
}

std::shared_ptr<arrow::Schema>
garrow_schema_get_raw(GArrowSchema *schema)
{
  GArrowSchemaPrivate *priv;

  priv = GARROW_SCHEMA_GET_PRIVATE(schema);
  return priv->schema;
}
