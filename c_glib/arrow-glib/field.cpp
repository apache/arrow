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

#include <arrow-glib/data-type.hpp>
#include <arrow-glib/field.hpp>

G_BEGIN_DECLS

/**
 * SECTION: field
 * @short_description: Field class
 *
 * #GArrowField is a class for field. Field is metadata of a
 * column. It has name, data type (#GArrowDataType) and nullable
 * information of the column.
 */

typedef struct GArrowFieldPrivate_ {
  std::shared_ptr<arrow::Field> field;
  GArrowDataType *data_type;
} GArrowFieldPrivate;

enum {
  PROP_FIELD = 1,
  PROP_DATA_TYPE
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowField,
                           garrow_field,
                           G_TYPE_OBJECT)

#define GARROW_FIELD_GET_PRIVATE(obj)         \
  static_cast<GArrowFieldPrivate *>(          \
     garrow_field_get_instance_private(       \
       GARROW_FIELD(obj)))

static void
garrow_field_dispose(GObject *object)
{
  auto priv = GARROW_FIELD_GET_PRIVATE(object);

  if (priv->data_type) {
    g_object_unref(priv->data_type);
    priv->data_type = nullptr;
  }

  G_OBJECT_CLASS(garrow_field_parent_class)->dispose(object);
}

static void
garrow_field_finalize(GObject *object)
{
  auto priv = GARROW_FIELD_GET_PRIVATE(object);

  priv->field = nullptr;

  G_OBJECT_CLASS(garrow_field_parent_class)->finalize(object);
}

static void
garrow_field_set_property(GObject *object,
                          guint prop_id,
                          const GValue *value,
                          GParamSpec *pspec)
{
  GArrowFieldPrivate *priv;

  priv = GARROW_FIELD_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FIELD:
    priv->field =
      *static_cast<std::shared_ptr<arrow::Field> *>(g_value_get_pointer(value));
    break;
  case PROP_DATA_TYPE:
    priv->data_type = GARROW_DATA_TYPE(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_field_init(GArrowField *object)
{
}

static void
garrow_field_class_init(GArrowFieldClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_field_dispose;
  gobject_class->finalize     = garrow_field_finalize;
  gobject_class->set_property = garrow_field_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("field",
                              "Field",
                              "The raw std::shared<arrow::Field> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FIELD, spec);

  spec = g_param_spec_object("data-type",
                             "Data type",
                             "The data type",
                             GARROW_TYPE_DATA_TYPE,
                             static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_DATA_TYPE, spec);
}

/**
 * garrow_field_new:
 * @name: The name of the field.
 * @data_type: The data type of the field.
 *
 * Returns: A newly created #GArrowField.
 */
GArrowField *
garrow_field_new(const gchar *name,
                 GArrowDataType *data_type)
{
  auto arrow_data_type = garrow_data_type_get_raw(data_type);
  auto arrow_field = std::make_shared<arrow::Field>(name, arrow_data_type);
  return garrow_field_new_raw(&arrow_field, data_type);
}

/**
 * garrow_field_new_full:
 * @name: The name of the field.
 * @data_type: The data type of the field.
 * @nullable: Whether null may be included or not.
 *
 * Returns: A newly created #GArrowField.
 */
GArrowField *
garrow_field_new_full(const gchar *name,
                      GArrowDataType *data_type,
                      gboolean nullable)
{
  auto arrow_field =
    std::make_shared<arrow::Field>(name,
                                   garrow_data_type_get_raw(data_type),
                                   nullable);
  return garrow_field_new_raw(&arrow_field, data_type);
}

/**
 * garrow_field_get_name:
 * @field: A #GArrowField.
 *
 * Returns: The name of the field.
 */
const gchar *
garrow_field_get_name(GArrowField *field)
{
  const auto arrow_field = garrow_field_get_raw(field);
  return arrow_field->name().c_str();
}

/**
 * garrow_field_get_data_type:
 * @field: A #GArrowField.
 *
 * Returns: (transfer none): The data type of the field.
 */
GArrowDataType *
garrow_field_get_data_type(GArrowField *field)
{
  auto priv = GARROW_FIELD_GET_PRIVATE(field);
  return priv->data_type;
}

/**
 * garrow_field_is_nullable:
 * @field: A #GArrowField.
 *
 * Returns: Whether the filed may include null or not.
 */
gboolean
garrow_field_is_nullable(GArrowField *field)
{
  const auto arrow_field = garrow_field_get_raw(field);
  return arrow_field->nullable();
}

/**
 * garrow_field_equal:
 * @field: A #GArrowField.
 * @other_field: A #GArrowField to be compared.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 */
gboolean
garrow_field_equal(GArrowField *field,
                   GArrowField *other_field)
{
  const auto arrow_field = garrow_field_get_raw(field);
  const auto arrow_other_field = garrow_field_get_raw(other_field);
  return arrow_field->Equals(arrow_other_field);
}

/**
 * garrow_field_to_string:
 * @field: A #GArrowField.
 *
 * Returns: The string representation of the field.
 */
gchar *
garrow_field_to_string(GArrowField *field)
{
  const auto arrow_field = garrow_field_get_raw(field);
  return g_strdup(arrow_field->ToString().c_str());
}

G_END_DECLS

GArrowField *
garrow_field_new_raw(std::shared_ptr<arrow::Field> *arrow_field,
                     GArrowDataType *data_type)
{
  bool data_type_need_unref = false;
  if (!data_type) {
    auto arrow_data_type = (*arrow_field)->type();
    data_type = garrow_data_type_new_raw(&arrow_data_type);
    data_type_need_unref = true;
  }
  auto field = GARROW_FIELD(g_object_new(GARROW_TYPE_FIELD,
                                         "field", arrow_field,
                                         "data-type", data_type,
                                         NULL));
  if (data_type_need_unref) {
    g_object_unref(data_type);
  }
  return field;
}

std::shared_ptr<arrow::Field>
garrow_field_get_raw(GArrowField *field)
{
  GArrowFieldPrivate *priv;

  priv = GARROW_FIELD_GET_PRIVATE(field);
  return priv->field;
}
