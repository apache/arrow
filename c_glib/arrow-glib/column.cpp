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

#include <arrow-glib/array.hpp>
#include <arrow-glib/chunked-array.hpp>
#include <arrow-glib/column.hpp>
#include <arrow-glib/data-type.hpp>
#include <arrow-glib/field.hpp>

G_BEGIN_DECLS

/**
 * SECTION: column
 * @short_description: Column class
 *
 * #GArrowColumn is a class for column. Column has a #GArrowField and
 * zero or more values. Values are #GArrowChunkedArray.
 */

typedef struct GArrowColumnPrivate_ {
  std::shared_ptr<arrow::Column> column;
  GArrowField *field;
  GArrowArray *array;
  GArrowChunkedArray *chunked_array;
} GArrowColumnPrivate;

enum {
  PROP_0,
  PROP_COLUMN,
  PROP_FIELD,
  PROP_ARRAY,
  PROP_CHUNKED_ARRAY
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowColumn,
                           garrow_column,
                           G_TYPE_OBJECT)

#define GARROW_COLUMN_GET_PRIVATE(object)          \
  static_cast<GArrowColumnPrivate *>(              \
    garrow_column_get_instance_private(            \
      GARROW_COLUMN(object)))

static void
garrow_column_dispose(GObject *object)
{
  auto priv = GARROW_COLUMN_GET_PRIVATE(object);

  if (priv->field) {
    g_object_unref(priv->field);
    priv->field = nullptr;
  }

  if (priv->array) {
    g_object_unref(priv->array);
    priv->array = nullptr;
  }

  if (priv->chunked_array) {
    g_object_unref(priv->chunked_array);
    priv->chunked_array = nullptr;
  }

  G_OBJECT_CLASS(garrow_column_parent_class)->dispose(object);
}

static void
garrow_column_finalize(GObject *object)
{
  auto priv = GARROW_COLUMN_GET_PRIVATE(object);

  priv->column = nullptr;

  G_OBJECT_CLASS(garrow_column_parent_class)->finalize(object);
}

static void
garrow_column_set_property(GObject *object,
                           guint prop_id,
                           const GValue *value,
                           GParamSpec *pspec)
{
  auto priv = GARROW_COLUMN_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_COLUMN:
    priv->column =
      *static_cast<std::shared_ptr<arrow::Column> *>(g_value_get_pointer(value));
    break;
  case PROP_FIELD:
    priv->field = static_cast<GArrowField *>(g_value_dup_object(value));
    break;
  case PROP_ARRAY:
    priv->array = static_cast<GArrowArray *>(g_value_dup_object(value));
    break;
  case PROP_CHUNKED_ARRAY:
    priv->chunked_array =
      static_cast<GArrowChunkedArray *>(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_column_get_property(GObject *object,
                           guint prop_id,
                           GValue *value,
                           GParamSpec *pspec)
{
  auto priv = GARROW_COLUMN_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FIELD:
    g_value_set_object(value, priv->field);
    break;
  case PROP_ARRAY:
    g_value_set_object(value, priv->array);
    break;
  case PROP_CHUNKED_ARRAY:
    g_value_set_object(value, priv->chunked_array);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_column_init(GArrowColumn *object)
{
}

static void
garrow_column_class_init(GArrowColumnClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_column_dispose;
  gobject_class->finalize     = garrow_column_finalize;
  gobject_class->set_property = garrow_column_set_property;
  gobject_class->get_property = garrow_column_get_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("column",
                              "Column",
                              "The raw std::shared<arrow::Column> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_COLUMN, spec);

  spec = g_param_spec_object("field",
                             "Field",
                             "The field of the column",
                             GARROW_TYPE_FIELD,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FIELD, spec);

  spec = g_param_spec_object("array",
                             "Array",
                             "The array of the column",
                             GARROW_TYPE_ARRAY,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_ARRAY, spec);

  spec = g_param_spec_object("chunked-array",
                             "Chunked array",
                             "The chunked array of the column",
                             GARROW_TYPE_CHUNKED_ARRAY,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_CHUNKED_ARRAY, spec);
}

/**
 * garrow_column_new_array:
 * @field: The metadata of the column.
 * @array: The data of the column.
 *
 * Returns: A newly created #GArrowColumn.
 */
GArrowColumn *
garrow_column_new_array(GArrowField *field,
                        GArrowArray *array)
{
  auto arrow_column =
    std::make_shared<arrow::Column>(garrow_field_get_raw(field),
                                    garrow_array_get_raw(array));
  auto column = GARROW_COLUMN(g_object_new(GARROW_TYPE_COLUMN,
                                           "column", &arrow_column,
                                           "field", field,
                                           "array", array,
                                           NULL));
  return column;
}

/**
 * garrow_column_new_chunked_array:
 * @field: The metadata of the column.
 * @chunked_array: The data of the column.
 *
 * Returns: A newly created #GArrowColumn.
 */
GArrowColumn *
garrow_column_new_chunked_array(GArrowField *field,
                                GArrowChunkedArray *chunked_array)
{
  auto arrow_column =
    std::make_shared<arrow::Column>(garrow_field_get_raw(field),
                                    garrow_chunked_array_get_raw(chunked_array));
  auto column = GARROW_COLUMN(g_object_new(GARROW_TYPE_COLUMN,
                                           "column", &arrow_column,
                                           "field", field,
                                           "chunked-array", chunked_array,
                                           NULL));
  return column;
}

/**
 * garrow_column_slice:
 * @column: A #GArrowColumn.
 * @offset: The offset of sub #GArrowColumn.
 * @length: The length of sub #GArrowColumn.
 *
 * Returns: (transfer full): The sub #GArrowColumn. It covers only from
 *   `offset` to `offset + length` range. The sub #GArrowColumn shares
 *   values with the base #GArrowColumn.
 */
GArrowColumn *
garrow_column_slice(GArrowColumn *column,
                    guint64 offset,
                    guint64 length)
{
  const auto arrow_column = garrow_column_get_raw(column);
  auto arrow_sub_column = arrow_column->Slice(offset, length);
  return garrow_column_new_raw(&arrow_sub_column);
}

/**
 * garrow_column_equal:
 * @column: A #GArrowColumn.
 * @other_column: A #GArrowColumn to be compared.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 *
 * Since: 0.4.0
 */
gboolean
garrow_column_equal(GArrowColumn *column, GArrowColumn *other_column)
{
  const auto arrow_column = garrow_column_get_raw(column);
  const auto arrow_other_column = garrow_column_get_raw(other_column);
  return arrow_column->Equals(arrow_other_column);
}

/**
 * garrow_column_get_length:
 * @column: A #GArrowColumn.
 *
 * Returns: The number of data of the column.
 */
guint64
garrow_column_get_length(GArrowColumn *column)
{
  const auto arrow_column = garrow_column_get_raw(column);
  return arrow_column->length();
}

/**
 * garrow_column_get_n_nulls:
 * @column: A #GArrowColumn.
 *
 * Returns: The number of nulls of the column.
 */
guint64
garrow_column_get_n_nulls(GArrowColumn *column)
{
  const auto arrow_column = garrow_column_get_raw(column);
  return arrow_column->null_count();
}

/**
 * garrow_column_get_field:
 * @column: A #GArrowColumn.
 *
 * Returns: (transfer full): The metadata of the column.
 */
GArrowField *
garrow_column_get_field(GArrowColumn *column)
{
  auto priv = GARROW_COLUMN_GET_PRIVATE(column);
  if (priv->field) {
    g_object_ref(priv->field);
    return priv->field;
  } else {
    const auto arrow_column = garrow_column_get_raw(column);
    auto arrow_field = arrow_column->field();
    return garrow_field_new_raw(&arrow_field);
  }
}

/**
 * garrow_column_get_name:
 * @column: A #GArrowColumn.
 *
 * Returns: The name of the column.
 */
const gchar *
garrow_column_get_name(GArrowColumn *column)
{
  const auto arrow_column = garrow_column_get_raw(column);
  return arrow_column->name().c_str();
}

/**
 * garrow_column_get_data_type:
 * @column: A #GArrowColumn.
 *
 * Returns: (transfer full): The data type of the column.
 */
GArrowDataType *
garrow_column_get_data_type(GArrowColumn *column)
{
  const auto arrow_column = garrow_column_get_raw(column);
  auto arrow_data_type = arrow_column->type();
  return garrow_data_type_new_raw(&arrow_data_type);
}

/**
 * garrow_column_get_data:
 * @column: A #GArrowColumn.
 *
 * Returns: (transfer full): The data of the column.
 */
GArrowChunkedArray *
garrow_column_get_data(GArrowColumn *column)
{
  const auto arrow_column = garrow_column_get_raw(column);
  auto arrow_chunked_array = arrow_column->data();
  return garrow_chunked_array_new_raw(&arrow_chunked_array);
}

G_END_DECLS

GArrowColumn *
garrow_column_new_raw(std::shared_ptr<arrow::Column> *arrow_column)
{
  auto column = GARROW_COLUMN(g_object_new(GARROW_TYPE_COLUMN,
                                           "column", arrow_column,
                                           NULL));
  return column;
}

std::shared_ptr<arrow::Column>
garrow_column_get_raw(GArrowColumn *column)
{
  auto priv = GARROW_COLUMN_GET_PRIVATE(column);
  return priv->column;
}
