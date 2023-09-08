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

#include <arrow-glib/basic-data-type.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/field.hpp>
#include <arrow-glib/internal-hash-table.hpp>
#include <arrow-glib/schema.hpp>

#include <arrow/c/bridge.h>

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
  auto priv = GARROW_SCHEMA_GET_PRIVATE(object);

  priv->schema.~shared_ptr();

  G_OBJECT_CLASS(garrow_schema_parent_class)->finalize(object);
}

static void
garrow_schema_set_property(GObject *object,
                           guint prop_id,
                           const GValue *value,
                           GParamSpec *pspec)
{
  auto priv = GARROW_SCHEMA_GET_PRIVATE(object);

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
  auto priv = GARROW_SCHEMA_GET_PRIVATE(object);
  new(&priv->schema) std::shared_ptr<arrow::Schema>;
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
 * garrow_schema_import:
 * @c_abi_schema: (not nullable): A `struct ArrowSchema *`.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable): An imported #GArrowSchema on success,
 *   %NULL on error.
 *
 *   You don't need to release the passed `struct ArrowSchema *`,
 *   even if this function reports an error.
 *
 * Since: 6.0.0
 */
GArrowSchema *
garrow_schema_import(gpointer c_abi_schema, GError **error)
{
  auto arrow_schema_result =
    arrow::ImportSchema(static_cast<ArrowSchema *>(c_abi_schema));
  if (garrow::check(error, arrow_schema_result, "[schema][import]")) {
    return garrow_schema_new_raw(&(*arrow_schema_result));
  } else {
    return NULL;
  }
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
 * garrow_schema_export:
 * @schema: A #GArrowSchema.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable): An exported #GArrowSchema as
 *   `struct ArrowStruct *` on success, %NULL on error.
 *
 *   It should be freed with the `ArrowSchema::release` callback then
 *   g_free() when no longer needed.
 *
 * Since: 6.0.0
 */
gpointer
garrow_schema_export(GArrowSchema *schema, GError **error)
{
  const auto arrow_schema = garrow_schema_get_raw(schema);
  auto c_abi_schema = g_new(ArrowSchema, 1);
  auto status = arrow::ExportSchema(*arrow_schema, c_abi_schema);
  if (garrow::check(error, status, "[schema][export]")) {
    return c_abi_schema;
  } else {
    g_free(c_abi_schema);
    return NULL;
  }
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
  return garrow_field_new_raw(&arrow_field, nullptr);
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
    auto arrow_data_type = arrow_field->type();
    return garrow_field_new_raw(&arrow_field, nullptr);
  }
}

/**
 * garrow_schema_get_field_index:
 * @schema: A #GArrowSchema.
 * @name: The name of the field to be found.
 *
 * Returns: The index of the found field, -1 on not found.
 *
 * Since: 0.15.0
 */
gint
garrow_schema_get_field_index(GArrowSchema *schema,
                              const gchar *name)
{
  const auto &arrow_schema = garrow_schema_get_raw(schema);
  return arrow_schema->GetFieldIndex(std::string(name));
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
    auto field = garrow_field_new_raw(&arrow_field, nullptr);
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
  const auto string = arrow_schema->ToString();
  return g_strdup(string.c_str());
}

/**
 * garrow_schema_to_string_metadata:
 * @schema: A #GArrowSchema.
 * @show_metadata: Whether include metadata or not.
 *
 * Returns: The string representation of the schema.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 0.17.0
 */
gchar *
garrow_schema_to_string_metadata(GArrowSchema *schema, gboolean show_metadata)
{
  const auto arrow_schema = garrow_schema_get_raw(schema);
  const auto string = arrow_schema->ToString(show_metadata);
  return g_strdup(string.c_str());
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
  auto maybe_new_schema = arrow_schema->AddField(i, arrow_field);
  if (garrow::check(error, maybe_new_schema, "[schema][add-field]")) {
    return garrow_schema_new_raw(&(*maybe_new_schema));
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
  auto maybe_new_schema = arrow_schema->RemoveField(i);
  if (garrow::check(error, maybe_new_schema, "[schema][remove-field]")) {
    return garrow_schema_new_raw(&(*maybe_new_schema));
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
  auto maybe_new_schema = arrow_schema->SetField(i, arrow_field);
  if (garrow::check(error, maybe_new_schema, "[schema][replace-field]")) {
    return garrow_schema_new_raw(&(*maybe_new_schema));
  } else {
    return NULL;
  }
}

/**
 * garrow_schema_has_metadata:
 * @schema: A #GArrowSchema.
 *
 * Returns: %TRUE if the schema has metadata, %FALSE otherwise.
 *
 * Since: 3.0.0
 */
gboolean
garrow_schema_has_metadata(GArrowSchema *schema)
{
  const auto arrow_schema = garrow_schema_get_raw(schema);
  return arrow_schema->HasMetadata();
}

/**
 * garrow_schema_get_metadata:
 * @schema: A #GArrowSchema.
 *
 * Returns: (element-type utf8 utf8) (nullable) (transfer full): The
 *   metadata in the schema.
 *
 *   It should be freed with g_hash_table_unref() when no longer needed.
 *
 * Since: 0.17.0
 */
GHashTable *
garrow_schema_get_metadata(GArrowSchema *schema)
{
  const auto arrow_schema = garrow_schema_get_raw(schema);
  if (!arrow_schema->HasMetadata()) {
    return NULL;
  }

  auto arrow_metadata = arrow_schema->metadata();
  auto metadata = g_hash_table_new(g_str_hash, g_str_equal);
  const auto n = arrow_metadata->size();
  for (int64_t i = 0; i < n; ++i) {
    g_hash_table_insert(metadata,
                        const_cast<gchar *>(arrow_metadata->key(i).c_str()),
                        const_cast<gchar *>(arrow_metadata->value(i).c_str()));
  }
  return metadata;
}

/**
 * garrow_schema_with_metadata:
 * @schema: A #GArrowSchema.
 * @metadata: (element-type utf8 utf8): A new associated metadata.
 *
 * Returns: (transfer full): The new schema with the given metadata.
 *
 * Since: 0.17.0
 */
GArrowSchema *
garrow_schema_with_metadata(GArrowSchema *schema,
                            GHashTable *metadata)
{
  const auto arrow_schema = garrow_schema_get_raw(schema);
  auto arrow_metadata = garrow_internal_hash_table_to_metadata(metadata);
  auto arrow_new_schema = arrow_schema->WithMetadata(arrow_metadata);
  return garrow_schema_new_raw(&arrow_new_schema);
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
  auto priv = GARROW_SCHEMA_GET_PRIVATE(schema);
  return priv->schema;
}
