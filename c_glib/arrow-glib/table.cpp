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

#include <arrow-glib/column.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/schema.hpp>
#include <arrow-glib/table.hpp>

G_BEGIN_DECLS

/**
 * SECTION: table
 * @short_description: Table class
 *
 * #GArrowTable is a class for table. Table has zero or more
 * #GArrowColumns and zero or more records.
 */

typedef struct GArrowTablePrivate_ {
  std::shared_ptr<arrow::Table> table;
} GArrowTablePrivate;

enum {
  PROP_0,
  PROP_TABLE
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowTable,
                           garrow_table,
                           G_TYPE_OBJECT)

#define GARROW_TABLE_GET_PRIVATE(obj)               \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),               \
                               GARROW_TYPE_TABLE,   \
                               GArrowTablePrivate))

static void
garrow_table_dispose(GObject *object)
{
  GArrowTablePrivate *priv;

  priv = GARROW_TABLE_GET_PRIVATE(object);

  priv->table = nullptr;

  G_OBJECT_CLASS(garrow_table_parent_class)->dispose(object);
}

static void
garrow_table_set_property(GObject *object,
                          guint prop_id,
                          const GValue *value,
                          GParamSpec *pspec)
{
  GArrowTablePrivate *priv;

  priv = GARROW_TABLE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_TABLE:
    priv->table =
      *static_cast<std::shared_ptr<arrow::Table> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_table_get_property(GObject *object,
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
garrow_table_init(GArrowTable *object)
{
}

static void
garrow_table_class_init(GArrowTableClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_table_dispose;
  gobject_class->set_property = garrow_table_set_property;
  gobject_class->get_property = garrow_table_get_property;

  spec = g_param_spec_pointer("table",
                              "Table",
                              "The raw std::shared<arrow::Table> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_TABLE, spec);
}

/**
 * garrow_table_new:
 * @schema: The schema of the table.
 * @columns: (element-type GArrowColumn): The columns of the table.
 *
 * Returns: A newly created #GArrowTable.
 */
GArrowTable *
garrow_table_new(GArrowSchema *schema,
                 GList *columns)
{
  std::vector<std::shared_ptr<arrow::Column>> arrow_columns;
  for (GList *node = columns; node; node = node->next) {
    GArrowColumn *column = GARROW_COLUMN(node->data);
    arrow_columns.push_back(garrow_column_get_raw(column));
  }

  auto arrow_table =
    arrow::Table::Make(garrow_schema_get_raw(schema), arrow_columns);
  return garrow_table_new_raw(&arrow_table);
}

/**
 * garrow_table_equal:
 * @table: A #GArrowTable.
 * @other_table: A #GArrowTable to be compared.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 *
 * Since: 0.4.0
 */
gboolean
garrow_table_equal(GArrowTable *table, GArrowTable *other_table)
{
  const auto arrow_table = garrow_table_get_raw(table);
  const auto arrow_other_table = garrow_table_get_raw(other_table);
  return arrow_table->Equals(*arrow_other_table);
}

/**
 * garrow_table_get_schema:
 * @table: A #GArrowTable.
 *
 * Returns: (transfer full): The schema of the table.
 */
GArrowSchema *
garrow_table_get_schema(GArrowTable *table)
{
  const auto arrow_table = garrow_table_get_raw(table);
  auto arrow_schema = arrow_table->schema();
  return garrow_schema_new_raw(&arrow_schema);
}

/**
 * garrow_table_get_column:
 * @table: A #GArrowTable.
 * @i: The index of the target column.
 *
 * Returns: (transfer full): The i-th column in the table.
 */
GArrowColumn *
garrow_table_get_column(GArrowTable *table,
                        guint i)
{
  const auto arrow_table = garrow_table_get_raw(table);
  auto arrow_column = arrow_table->column(i);
  return garrow_column_new_raw(&arrow_column);
}

/**
 * garrow_table_get_n_columns:
 * @table: A #GArrowTable.
 *
 * Returns: The number of columns in the table.
 */
guint
garrow_table_get_n_columns(GArrowTable *table)
{
  const auto arrow_table = garrow_table_get_raw(table);
  return arrow_table->num_columns();
}

/**
 * garrow_table_get_n_rows:
 * @table: A #GArrowTable.
 *
 * Returns: The number of rows in the table.
 */
guint64
garrow_table_get_n_rows(GArrowTable *table)
{
  const auto arrow_table = garrow_table_get_raw(table);
  return arrow_table->num_rows();
}

/**
 * garrow_table_add_column:
 * @table: A #GArrowTable.
 * @i: The index of the new column.
 * @column: The column to be added.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The newly allocated
 *   #GArrowTable that has a new column or %NULL on error.
 *
 * Since: 0.3.0
 */
GArrowTable *
garrow_table_add_column(GArrowTable *table,
                        guint i,
                        GArrowColumn *column,
                        GError **error)
{
  const auto arrow_table = garrow_table_get_raw(table);
  const auto arrow_column = garrow_column_get_raw(column);
  std::shared_ptr<arrow::Table> arrow_new_table;
  auto status = arrow_table->AddColumn(i, arrow_column, &arrow_new_table);
  if (garrow_error_check(error, status, "[table][add-column]")) {
    return garrow_table_new_raw(&arrow_new_table);
  } else {
    return NULL;
  }
}

/**
 * garrow_table_remove_column:
 * @table: A #GArrowTable.
 * @i: The index of the column to be removed.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The newly allocated
 *   #GArrowTable that doesn't have the column or %NULL on error.
 *
 * Since: 0.3.0
 */
GArrowTable *
garrow_table_remove_column(GArrowTable *table,
                           guint i,
                           GError **error)
{
  const auto arrow_table = garrow_table_get_raw(table);
  std::shared_ptr<arrow::Table> arrow_new_table;
  auto status = arrow_table->RemoveColumn(i, &arrow_new_table);
  if (garrow_error_check(error, status, "[table][remove-column]")) {
    return garrow_table_new_raw(&arrow_new_table);
  } else {
    return NULL;
  }
}

/**
 * garrow_table_replace_column:
 * @table: A #GArrowTable.
 * @i: The index of the column to be replaced.
 * @column: The newly added #GArrowColumn.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The newly allocated
 * #GArrowTable that has @column as the @i-th column or %NULL on
 * error.
 *
 * Since: 0.10.0
 */
GArrowTable *
garrow_table_replace_column(GArrowTable *table,
                            guint i,
                            GArrowColumn *column,
                            GError **error)
{
  const auto arrow_table = garrow_table_get_raw(table);
  const auto arrow_column = garrow_column_get_raw(column);
  std::shared_ptr<arrow::Table> arrow_new_table;
  auto status = arrow_table->SetColumn(i, arrow_column, &arrow_new_table);
  if (garrow_error_check(error, status, "[table][replace-column]")) {
    return garrow_table_new_raw(&arrow_new_table);
  } else {
    return NULL;
  }
}

G_END_DECLS

GArrowTable *
garrow_table_new_raw(std::shared_ptr<arrow::Table> *arrow_table)
{
  auto table = GARROW_TABLE(g_object_new(GARROW_TYPE_TABLE,
                                         "table", arrow_table,
                                         NULL));
  return table;
}

std::shared_ptr<arrow::Table>
garrow_table_get_raw(GArrowTable *table)
{
  GArrowTablePrivate *priv;

  priv = GARROW_TABLE_GET_PRIVATE(table);
  return priv->table;
}
