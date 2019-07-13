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
#include <arrow-glib/error.hpp>
#include <arrow-glib/field.hpp>
#include <arrow-glib/internal-index.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/schema.hpp>
#include <arrow-glib/table.hpp>

#include <sstream>

G_BEGIN_DECLS

/**
 * SECTION: table
 * @short_description: Table class
 *
 * #GArrowTable is a class for table. Table has zero or more
 * #GArrowChunkedArrays and zero or more records.
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

#define GARROW_TABLE_GET_PRIVATE(obj)         \
  static_cast<GArrowTablePrivate *>(          \
     garrow_table_get_instance_private(       \
       GARROW_TABLE(obj)))

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
 * garrow_table_new_values: (skip)
 * @schema: The schema of the table.
 * @values: The values of the table. All values must be instance of
 *   the same class. Available classes are #GArrowChunkedArray,
 *   #GArrowArray and #GArrowRecordBatch.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowTable or %NULL on error.
 *
 * Since: 0.12.0
 */
GArrowTable *
garrow_table_new_values(GArrowSchema *schema,
                        GList *values,
                        GError **error)
{
  const auto context = "[table][new][values]";
  auto arrow_schema = garrow_schema_get_raw(schema);
  std::vector<std::shared_ptr<arrow::ChunkedArray>> arrow_chunked_arrays;
  std::vector<std::shared_ptr<arrow::Array>> arrow_arrays;
  std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_record_batches;
  for (GList *node = values; node; node = node->next) {
    if (GARROW_IS_CHUNKED_ARRAY(node->data)) {
      auto chunked_array = GARROW_CHUNKED_ARRAY(node->data);
      arrow_chunked_arrays.push_back(garrow_chunked_array_get_raw(chunked_array));
    } else if (GARROW_IS_ARRAY(node->data)) {
      auto array = GARROW_ARRAY(node->data);
      arrow_arrays.push_back(garrow_array_get_raw(array));
    } else if (GARROW_IS_RECORD_BATCH(node->data)) {
      auto record_batch = GARROW_RECORD_BATCH(node->data);
      arrow_record_batches.push_back(garrow_record_batch_get_raw(record_batch));
    } else {
      g_set_error(error,
                  GARROW_ERROR,
                  GARROW_ERROR_INVALID,
                  "%s: %s",
                  context,
                  "value must be one of "
                  "GArrowChunkedArray, GArrowArray and GArrowRecordBatch");
      return NULL;
    }
  }

  size_t n_types = 0;
  if (!arrow_chunked_arrays.empty()) {
    ++n_types;
  }
  if (!arrow_arrays.empty()) {
    ++n_types;
  }
  if (!arrow_record_batches.empty()) {
    ++n_types;
  }
  if (n_types > 1) {
    g_set_error(error,
                GARROW_ERROR,
                GARROW_ERROR_INVALID,
                "%s: %s",
                context,
                "all values must be the same objects of "
                "GArrowChunkedArray, GArrowArray or GArrowRecordBatch");
    return NULL;
  }

  if (!arrow_chunked_arrays.empty()) {
    auto arrow_table = arrow::Table::Make(arrow_schema, arrow_chunked_arrays);
    auto status = arrow_table->Validate();
    if (garrow_error_check(error, status, context)) {
      return garrow_table_new_raw(&arrow_table);
    } else {
      return NULL;
    }
  } else if (!arrow_arrays.empty()) {
    auto arrow_table = arrow::Table::Make(arrow_schema, arrow_arrays);
    auto status = arrow_table->Validate();
    if (garrow_error_check(error, status, context)) {
      return garrow_table_new_raw(&arrow_table);
    } else {
      return NULL;
    }
  } else {
    std::shared_ptr<arrow::Table> arrow_table;
    auto status = arrow::Table::FromRecordBatches(arrow_schema,
                                                  arrow_record_batches,
                                                  &arrow_table);
    if (garrow_error_check(error, status, context)) {
      return garrow_table_new_raw(&arrow_table);
    } else {
      return NULL;
    }
  }
}

/**
 * garrow_table_new_chunked_arrays:
 * @schema: The schema of the table.
 * @chunked_arrays: (array length=n_chunked_arrays): The chunked arrays of
 *   the table.
 * @n_chunked_arrays: The number of chunked arrays.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowTable or %NULL on error.
 *
 * Since: 1.0.0
 */
GArrowTable *
garrow_table_new_chunked_arrays(GArrowSchema *schema,
                                GArrowChunkedArray **chunked_arrays,
                                gsize n_chunked_arrays,
                                GError **error)
{
  auto arrow_schema = garrow_schema_get_raw(schema);
  std::vector<std::shared_ptr<arrow::ChunkedArray>> arrow_chunked_arrays;
  for (gsize i = 0; i < n_chunked_arrays; ++i) {
    auto arrow_chunked_array = garrow_chunked_array_get_raw(chunked_arrays[i]);
    arrow_chunked_arrays.push_back(arrow_chunked_array);
  }

  auto arrow_table = arrow::Table::Make(arrow_schema, arrow_chunked_arrays);
  auto status = arrow_table->Validate();
  if (garrow_error_check(error, status, "[table][new][chunked-arrays]")) {
    return garrow_table_new_raw(&arrow_table);
  } else {
    return NULL;
  }
}

/**
 * garrow_table_new_arrays:
 * @schema: The schema of the table.
 * @arrays: (array length=n_arrays): The arrays of the table.
 * @n_arrays: The number of arrays.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowTable or %NULL on error.
 *
 * Since: 0.12.0
 */
GArrowTable *
garrow_table_new_arrays(GArrowSchema *schema,
                        GArrowArray **arrays,
                        gsize n_arrays,
                        GError **error)
{
  auto arrow_schema = garrow_schema_get_raw(schema);
  std::vector<std::shared_ptr<arrow::Array>> arrow_arrays;
  for (gsize i = 0; i < n_arrays; ++i) {
    arrow_arrays.push_back(garrow_array_get_raw(arrays[i]));
  }

  auto arrow_table = arrow::Table::Make(arrow_schema, arrow_arrays);
  auto status = arrow_table->Validate();
  if (garrow_error_check(error, status, "[table][new][arrays]")) {
    return garrow_table_new_raw(&arrow_table);
  } else {
    return NULL;
  }
}

/**
 * garrow_table_new_record_batches:
 * @schema: The schema of the table.
 * @record_batches: (array length=n_record_batches): The record batches
 *   that have data for the table.
 * @n_record_batches: The number of record batches.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowTable or %NULL on error.
 *
 * Since: 0.12.0
 */
GArrowTable *
garrow_table_new_record_batches(GArrowSchema *schema,
                                GArrowRecordBatch **record_batches,
                                gsize n_record_batches,
                                GError **error)
{
  auto arrow_schema = garrow_schema_get_raw(schema);
  std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_record_batches;
  for (gsize i = 0; i < n_record_batches; ++i) {
    auto arrow_record_batch = garrow_record_batch_get_raw(record_batches[i]);
    arrow_record_batches.push_back(arrow_record_batch);
  }

  std::shared_ptr<arrow::Table> arrow_table;
  auto status = arrow::Table::FromRecordBatches(arrow_schema,
                                                arrow_record_batches,
                                                &arrow_table);
  if (garrow_error_check(error, status, "[table][new][record-batches]")) {
    return garrow_table_new_raw(&arrow_table);
  } else {
    return NULL;
  }
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
 * garrow_table_get_column_data:
 * @table: A #GArrowTable.
 * @i: The index of the target column. If it's negative, index is
 *   counted backward from the end of the columns. `-1` means the last
 *   column.
 *
 * Returns: (nullable) (transfer full): The i-th column's data in the table.
 *
 * Since: 1.0.0
 */
GArrowChunkedArray *
garrow_table_get_column_data(GArrowTable *table,
                             gint i)
{
  const auto &arrow_table = garrow_table_get_raw(table);
  if (!garrow_internal_index_adjust(i, arrow_table->num_columns())) {
    return NULL;
  }
  auto arrow_column = arrow_table->column(i);
  return garrow_chunked_array_new_raw(&arrow_column);
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
 * @field: The field for the column to be added.
 * @chunked_array: The column data to be added.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The newly allocated
 *   #GArrowTable that has a new column or %NULL on error.
 *
 * Since: 1.0.0
 */
GArrowTable *
garrow_table_add_column(GArrowTable *table,
                        guint i,
                        GArrowField *field,
                        GArrowChunkedArray *chunked_array,
                        GError **error)
{
  const auto arrow_table = garrow_table_get_raw(table);
  const auto arrow_field = garrow_field_get_raw(field);
  const auto arrow_chunked_array = garrow_chunked_array_get_raw(chunked_array);
  std::shared_ptr<arrow::Table> arrow_new_table;
  auto status = arrow_table->AddColumn(i,
                                       arrow_field,
                                       arrow_chunked_array,
                                       &arrow_new_table);
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
 * @field: The field for the new column.
 * @chunked_array: The newly added column data.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The newly allocated
 * #GArrowTable that has @column as the @i-th column or %NULL on
 * error.
 *
 * Since: 1.0.0
 */
GArrowTable *
garrow_table_replace_column(GArrowTable *table,
                            guint i,
                            GArrowField *field,
                            GArrowChunkedArray *chunked_array,
                            GError **error)
{
  const auto arrow_table = garrow_table_get_raw(table);
  const auto arrow_field = garrow_field_get_raw(field);
  const auto arrow_chunked_array = garrow_chunked_array_get_raw(chunked_array);
  std::shared_ptr<arrow::Table> arrow_new_table;
  auto status = arrow_table->SetColumn(i,
                                       arrow_field,
                                       arrow_chunked_array,
                                       &arrow_new_table);
  if (garrow_error_check(error, status, "[table][replace-column]")) {
    return garrow_table_new_raw(&arrow_new_table);
  } else {
    return NULL;
  }
}

/**
 * garrow_table_to_string:
 * @table: A #GArrowTable.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   The formatted table content or %NULL on error.
 *
 *   The returned string should be freed when with g_free() when no
 *   longer needed.
 *
 * Since: 0.12.0
 */
gchar *
garrow_table_to_string(GArrowTable *table, GError **error)
{
  const auto arrow_table = garrow_table_get_raw(table);
  std::stringstream sink;
  auto status = arrow::PrettyPrint(*arrow_table, 0, &sink);
  if (garrow_error_check(error, status, "[table][to-string]")) {
    return g_strdup(sink.str().c_str());
  } else {
    return NULL;
  }
}

/**
 * garrow_table_concatenate:
 * @table: A #GArrowTable.
 * @other_tables: (element-type GArrowTable): The tables to be concatenated.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The table concatenated vertically.
 *
 * Since: 0.14.0
 */
GArrowTable *
garrow_table_concatenate(GArrowTable *table,
                         GList *other_tables,
                         GError **error)
{
  auto arrow_table = garrow_table_get_raw(table);
  std::vector<std::shared_ptr<arrow::Table>> arrow_tables = { arrow_table };
  for (auto node = other_tables; node; node = g_list_next(node)) {
    auto arrow_other_table = garrow_table_get_raw(GARROW_TABLE(node->data));
    arrow_tables.push_back(arrow_other_table);
  }
  std::shared_ptr<arrow::Table> arrow_concatenated_table;
  auto status = arrow::ConcatenateTables(arrow_tables, &arrow_concatenated_table);
  if (garrow_error_check(error, status, "[table][concatenate]")) {
    return garrow_table_new_raw(&arrow_concatenated_table);
  } else {
    return NULL;
  }
}

/**
 * garrow_table_slice:
 * @table: A #GArrowTable.
 * @offset: The offset of sub #GArrowTable. If the offset is negative,
 *   the offset is counted from the last.
 * @length: The length of sub #GArrowTable.
 *
 * Returns: (transfer full): The sub #GArrowTable. It covers
 *   only from `offset` to `offset + length` range. The sub
 *   #GArrowTable shares values with the base
 *   #GArrowTable.
 *
 * Since: 0.14.0
 */
GArrowTable *
garrow_table_slice(GArrowTable *table,
                   gint64 offset,
                   gint64 length)
{
  const auto arrow_table = garrow_table_get_raw(table);
  if (offset < 0) {
    offset += arrow_table->num_rows();
  }
  auto arrow_sub_table = arrow_table->Slice(offset, length);
  return garrow_table_new_raw(&arrow_sub_table);
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
