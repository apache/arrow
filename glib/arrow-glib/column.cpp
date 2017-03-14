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
} GArrowColumnPrivate;

enum {
  PROP_0,
  PROP_COLUMN
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowColumn,
                           garrow_column,
                           G_TYPE_OBJECT)

#define GARROW_COLUMN_GET_PRIVATE(obj)                  \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                   \
                               GARROW_TYPE_COLUMN,      \
                               GArrowColumnPrivate))

static void
garrow_column_dispose(GObject *object)
{
  GArrowColumnPrivate *priv;

  priv = GARROW_COLUMN_GET_PRIVATE(object);

  priv->column = nullptr;

  G_OBJECT_CLASS(garrow_column_parent_class)->dispose(object);
}

static void
garrow_column_set_property(GObject *object,
                           guint prop_id,
                           const GValue *value,
                           GParamSpec *pspec)
{
  GArrowColumnPrivate *priv;

  priv = GARROW_COLUMN_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_COLUMN:
    priv->column =
      *static_cast<std::shared_ptr<arrow::Column> *>(g_value_get_pointer(value));
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
  switch (prop_id) {
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
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_column_dispose;
  gobject_class->set_property = garrow_column_set_property;
  gobject_class->get_property = garrow_column_get_property;

  spec = g_param_spec_pointer("column",
                              "Column",
                              "The raw std::shared<arrow::Column> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_COLUMN, spec);
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
  return garrow_column_new_raw(&arrow_column);
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
  return garrow_column_new_raw(&arrow_column);
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
  const auto arrow_column = garrow_column_get_raw(column);
  auto arrow_field = arrow_column->field();
  return garrow_field_new_raw(&arrow_field);
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
  GArrowColumnPrivate *priv;

  priv = GARROW_COLUMN_GET_PRIVATE(column);
  return priv->column;
}
