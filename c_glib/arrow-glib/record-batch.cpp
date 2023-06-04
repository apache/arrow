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

#include <arrow-glib/array.hpp>
#include <arrow-glib/buffer.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/field.hpp>
#include <arrow-glib/internal-index.hpp>
#include <arrow-glib/ipc-options.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/schema.hpp>

#include <arrow/c/bridge.h>
#include <arrow/util/iterator.h>

#include <sstream>

G_BEGIN_DECLS

/**
 * SECTION: record-batch
 * @section_id: record-batch
 * @title: Record batch related classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowRecordBatch is a class for record batch. Record batch is
 * similar to #GArrowTable. Record batch also has also zero or more
 * columns and zero or more records.
 *
 * Record batch is used for shared memory IPC.
 *
 * #GArrowRecordBatchIterator is a class for iterating record
 * batches.
 */

typedef struct GArrowRecordBatchPrivate_ {
  std::shared_ptr<arrow::RecordBatch> record_batch;
} GArrowRecordBatchPrivate;

enum {
  PROP_RECORD_BATCH = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowRecordBatch,
                           garrow_record_batch,
                           G_TYPE_OBJECT)

#define GARROW_RECORD_BATCH_GET_PRIVATE(obj)         \
  static_cast<GArrowRecordBatchPrivate *>(           \
     garrow_record_batch_get_instance_private(       \
       GARROW_RECORD_BATCH(obj)))

static void
garrow_record_batch_finalize(GObject *object)
{
  auto priv = GARROW_RECORD_BATCH_GET_PRIVATE(object);

  priv->record_batch.~shared_ptr();

  G_OBJECT_CLASS(garrow_record_batch_parent_class)->finalize(object);
}

static void
garrow_record_batch_set_property(GObject *object,
                                 guint prop_id,
                                 const GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GARROW_RECORD_BATCH_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_RECORD_BATCH:
    priv->record_batch =
      *static_cast<std::shared_ptr<arrow::RecordBatch> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_record_batch_get_property(GObject *object,
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
garrow_record_batch_init(GArrowRecordBatch *object)
{
  auto priv = GARROW_RECORD_BATCH_GET_PRIVATE(object);
  new(&priv->record_batch) std::shared_ptr<arrow::RecordBatch>;
}

static void
garrow_record_batch_class_init(GArrowRecordBatchClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_record_batch_finalize;
  gobject_class->set_property = garrow_record_batch_set_property;
  gobject_class->get_property = garrow_record_batch_get_property;

  spec = g_param_spec_pointer("record-batch",
                              "RecordBatch",
                              "The raw std::shared<arrow::RecordBatch> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_RECORD_BATCH, spec);
}

/**
 * garrow_record_batch_import:
 * @c_abi_array: (not nullable): A `struct ArrowArray *`.
 * @schema: A #GArrowSchema of the C ABI array.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable): An imported #GArrowRecordBatch
 *   on success, %NULL on error.
 *
 *   You don't need to release the passed `struct ArrowArray *`,
 *   even if this function reports an error.
 *
 * Since: 6.0.0
 */
GArrowRecordBatch *
garrow_record_batch_import(gpointer c_abi_array,
                           GArrowSchema *schema,
                           GError **error)
{
  auto arrow_schema = garrow_schema_get_raw(schema);
  auto arrow_record_batch_result =
    arrow::ImportRecordBatch(static_cast<ArrowArray *>(c_abi_array),
                             arrow_schema);
  if (garrow::check(error,
                    arrow_record_batch_result,
                    "[record-batch][import]")) {
    return garrow_record_batch_new_raw(&(*arrow_record_batch_result));
  } else {
    return NULL;
  }
}

/**
 * garrow_record_batch_new:
 * @schema: The schema of the record batch.
 * @n_rows: The number of the rows in the record batch.
 * @columns: (element-type GArrowArray): The columns in the record batch.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowRecordBatch or %NULL on error.
 */
GArrowRecordBatch *
garrow_record_batch_new(GArrowSchema *schema,
                        guint32 n_rows,
                        GList *columns,
                        GError **error)
{
  const gchar *tag = "[record-batch][new]";

  std::vector<std::shared_ptr<arrow::Array>> arrow_columns;
  for (GList *node = columns; node; node = node->next) {
    GArrowArray *column = GARROW_ARRAY(node->data);
    arrow_columns.push_back(garrow_array_get_raw(column));
  }

  const auto &arrow_schema = garrow_schema_get_raw(schema);
  if (arrow_schema->num_fields() != static_cast<int>(arrow_columns.size())) {
    auto status =
      arrow::Status::Invalid("Number of columns did not match schema");
    garrow_error_check(error, status, tag);
    return NULL;
  }

  auto arrow_record_batch =
    arrow::RecordBatch::Make(arrow_schema, n_rows, arrow_columns);
  auto status = arrow_record_batch->Validate();
  if (garrow_error_check(error, status, tag)) {
    return garrow_record_batch_new_raw(&arrow_record_batch);
  } else {
    return NULL;
  }
}

/**
 * garrow_record_batch_export:
 * @record_batch: A #GArrowRecordBatch.
 * @c_abi_array: (out): Return location for a `struct ArrowArray *`.
 *   It should be freed with the `ArrowArray::release` callback then
 *   g_free() when no longer needed.
 * @c_abi_schema: (out) (nullable): Return location for a
 *   `struct ArrowSchema *` or %NULL.
 *   It should be freed with the `ArrowSchema::release` callback then
 *   g_free() when no longer needed.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 6.0.0
 */
gboolean
garrow_record_batch_export(GArrowRecordBatch *record_batch,
                           gpointer *c_abi_array,
                           gpointer *c_abi_schema,
                           GError **error)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  *c_abi_array = g_new(ArrowArray, 1);
  arrow::Status status;
  if (c_abi_schema) {
    *c_abi_schema = g_new(ArrowSchema, 1);
    status = arrow::ExportRecordBatch(*arrow_record_batch,
                                      static_cast<ArrowArray *>(*c_abi_array),
                                      static_cast<ArrowSchema *>(*c_abi_schema));
  } else {
    status = arrow::ExportRecordBatch(*arrow_record_batch,
                                      static_cast<ArrowArray *>(*c_abi_array));
  }
  if (garrow::check(error, status, "[record-batch][export]")) {
    return true;
  } else {
    g_free(*c_abi_array);
    *c_abi_array = nullptr;
    if (c_abi_schema) {
      g_free(*c_abi_schema);
      *c_abi_schema = nullptr;
    }
    return false;
  }
}

/**
 * garrow_record_batch_equal:
 * @record_batch: A #GArrowRecordBatch.
 * @other_record_batch: A #GArrowRecordBatch to be compared.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 *
 * Since: 0.4.0
 */
gboolean
garrow_record_batch_equal(GArrowRecordBatch *record_batch,
                          GArrowRecordBatch *other_record_batch)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  const auto arrow_other_record_batch =
    garrow_record_batch_get_raw(other_record_batch);
  return arrow_record_batch->Equals(*arrow_other_record_batch);
}

/**
 * garrow_record_batch_equal_metadata:
 * @record_batch: A #GArrowRecordBatch.
 * @other_record_batch: A #GArrowRecordBatch to be compared.
 * @check_metadata: Whether to compare metadata.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 *
 * Since: 0.17.0
 */
gboolean
garrow_record_batch_equal_metadata(GArrowRecordBatch *record_batch,
                                   GArrowRecordBatch *other_record_batch,
                                   gboolean check_metadata)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  const auto arrow_other_record_batch = garrow_record_batch_get_raw(other_record_batch);
  return arrow_record_batch->Equals(*arrow_other_record_batch, check_metadata);
}

/**
 * garrow_record_batch_get_schema:
 * @record_batch: A #GArrowRecordBatch.
 *
 * Returns: (transfer full): The schema of the record batch.
 */
GArrowSchema *
garrow_record_batch_get_schema(GArrowRecordBatch *record_batch)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  auto arrow_schema = arrow_record_batch->schema();
  return garrow_schema_new_raw(&arrow_schema);
}

/**
 * garrow_record_batch_get_column_data:
 * @record_batch: A #GArrowRecordBatch.
 * @i: The index of the target column. If it's negative, index is
 *   counted backward from the end of the columns. `-1` means the last
 *   column.
 *
 * Returns: (transfer full) (nullable): The i-th column in the record batch
 *   on success, %NULL on out of index.
 *
 * Since: 0.15.0
 */
GArrowArray *
garrow_record_batch_get_column_data(GArrowRecordBatch *record_batch,
                                    gint i)
{
  const auto &arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  if (!garrow_internal_index_adjust(i, arrow_record_batch->num_columns())) {
    return NULL;
  }
  auto arrow_column = arrow_record_batch->column(i);
  return garrow_array_new_raw(&arrow_column);
}

/**
 * garrow_record_batch_get_column_name:
 * @record_batch: A #GArrowRecordBatch.
 * @i: The index of the target column. If it's negative, index is
 *   counted backward from the end of the columns. `-1` means the last
 *   column.
 *
 * Returns: (nullable): The name of the i-th column in the record batch
 *   on success, %NULL on out of index
 */
const gchar *
garrow_record_batch_get_column_name(GArrowRecordBatch *record_batch,
                                    gint i)
{
  const auto &arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  if (!garrow_internal_index_adjust(i, arrow_record_batch->num_columns())) {
    return NULL;
  }
  return arrow_record_batch->column_name(i).c_str();
}

/**
 * garrow_record_batch_get_n_columns:
 * @record_batch: A #GArrowRecordBatch.
 *
 * Returns: The number of columns in the record batch.
 */
guint
garrow_record_batch_get_n_columns(GArrowRecordBatch *record_batch)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  return arrow_record_batch->num_columns();
}

/**
 * garrow_record_batch_get_n_rows:
 * @record_batch: A #GArrowRecordBatch.
 *
 * Returns: The number of rows in the record batch.
 */
gint64
garrow_record_batch_get_n_rows(GArrowRecordBatch *record_batch)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  return arrow_record_batch->num_rows();
}

/**
 * garrow_record_batch_slice:
 * @record_batch: A #GArrowRecordBatch.
 * @offset: The offset of sub #GArrowRecordBatch.
 * @length: The length of sub #GArrowRecordBatch.
 *
 * Returns: (transfer full): The sub #GArrowRecordBatch. It covers
 *   only from `offset` to `offset + length` range. The sub
 *   #GArrowRecordBatch shares values with the base
 *   #GArrowRecordBatch.
 */
GArrowRecordBatch *
garrow_record_batch_slice(GArrowRecordBatch *record_batch,
                          gint64 offset,
                          gint64 length)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  auto arrow_sub_record_batch = arrow_record_batch->Slice(offset, length);
  return garrow_record_batch_new_raw(&arrow_sub_record_batch);
}

/**
 * garrow_record_batch_to_string:
 * @record_batch: A #GArrowRecordBatch.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable):
 *   The formatted record batch content or %NULL on error.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 0.4.0
 */
gchar *
garrow_record_batch_to_string(GArrowRecordBatch *record_batch, GError **error)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  const auto string = arrow_record_batch->ToString();
  return g_strdup(string.c_str());
}

/**
 * garrow_record_batch_add_column:
 * @record_batch: A #GArrowRecordBatch.
 * @i: The index of the new column.
 * @field: The field to be added.
 * @column: The column to be added.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The newly allocated
 *   #GArrowRecordBatch that has a new column or %NULL on error.
 *
 * Since: 0.9.0
 */
GArrowRecordBatch *
garrow_record_batch_add_column(GArrowRecordBatch *record_batch,
                               guint i,
                               GArrowField *field,
                               GArrowArray *column,
                               GError **error)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  const auto arrow_field = garrow_field_get_raw(field);
  const auto arrow_column = garrow_array_get_raw(column);
  auto arrow_new_record_batch =
    arrow_record_batch->AddColumn(i, arrow_field, arrow_column);
  if (garrow::check(error,
                    arrow_new_record_batch,
                    "[record-batch][add-column]")) {
    return garrow_record_batch_new_raw(&(*arrow_new_record_batch));
  } else {
    return NULL;
  }
}

/**
 * garrow_record_batch_remove_column:
 * @record_batch: A #GArrowRecordBatch.
 * @i: The index of the new column.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The newly allocated
 *   #GArrowRecordBatch that doesn't have the column or %NULL on error.
 *
 * Since: 0.9.0
 */
GArrowRecordBatch *
garrow_record_batch_remove_column(GArrowRecordBatch *record_batch,
                                  guint i,
                                  GError **error)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  auto arrow_new_record_batch = arrow_record_batch->RemoveColumn(i);
  if (garrow::check(error,
                    arrow_new_record_batch,
                    "[record-batch][remove-column]")) {
    return garrow_record_batch_new_raw(&(*arrow_new_record_batch));
  } else {
    return NULL;
  }
}

/**
 * garrow_record_batch_serialize:
 * @record_batch: A #GArrowRecordBatch.
 * @options: (nullable): A #GArrowWriteOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The newly allocated
 *   #GArrowBuffer that contains a serialized record batch or %NULL on
 *   error.
 *
 * Since: 1.0.0
 */
GArrowBuffer *
garrow_record_batch_serialize(GArrowRecordBatch *record_batch,
                              GArrowWriteOptions *options,
                              GError **error)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  arrow::Result<std::shared_ptr<arrow::Buffer>> arrow_buffer;
  if (options) {
    auto arrow_options = garrow_write_options_get_raw(options);
    auto arrow_buffer = arrow::ipc::SerializeRecordBatch(*arrow_record_batch,
                                                         *arrow_options);
    if (garrow::check(error, arrow_buffer, "[record-batch][serialize]")) {
      return garrow_buffer_new_raw(&(*arrow_buffer));
    } else {
      return NULL;
    }
  } else {
    const auto arrow_options = arrow::ipc::IpcWriteOptions::Defaults();
    auto arrow_buffer = arrow::ipc::SerializeRecordBatch(*arrow_record_batch,
                                                         arrow_options);
    if (garrow::check(error, arrow_buffer, "[record-batch][serialize]")) {
      return garrow_buffer_new_raw(&(*arrow_buffer));
    } else {
      return NULL;
    }
  }
}


typedef struct GArrowRecordBatchIteratorPrivate_ {
  arrow::RecordBatchIterator iterator;
} GArrowRecordBatchIteratorPrivate;

enum {
  PROP_ITERATOR = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowRecordBatchIterator,
                           garrow_record_batch_iterator,
                           G_TYPE_OBJECT)

#define GARROW_RECORD_BATCH_ITERATOR_GET_PRIVATE(obj)        \
  static_cast<GArrowRecordBatchIteratorPrivate *>(           \
     garrow_record_batch_iterator_get_instance_private(      \
       GARROW_RECORD_BATCH_ITERATOR(obj)))

static void
garrow_record_batch_iterator_finalize(GObject *object)
{
  auto priv = GARROW_RECORD_BATCH_ITERATOR_GET_PRIVATE(object);

  priv->iterator.~Iterator();

  G_OBJECT_CLASS(garrow_record_batch_iterator_parent_class)->finalize(object);
}

static void
garrow_record_batch_iterator_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  auto priv = GARROW_RECORD_BATCH_ITERATOR_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_ITERATOR:
    priv->iterator =
      std::move(*static_cast<arrow::RecordBatchIterator *>(g_value_get_pointer(value)));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_record_batch_iterator_init(GArrowRecordBatchIterator *object)
{
  auto priv = GARROW_RECORD_BATCH_ITERATOR_GET_PRIVATE(object);
  new(&priv->iterator) arrow::RecordBatchIterator;
}

static void
garrow_record_batch_iterator_class_init(GArrowRecordBatchIteratorClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_record_batch_iterator_finalize;
  gobject_class->set_property = garrow_record_batch_iterator_set_property;

  GParamSpec *spec;

  spec = g_param_spec_pointer("iterator",
                              "Iterator",
                              "The raw arrow::RecordBatchIterator",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_ITERATOR, spec);
}

/**
 * garrow_record_batch_iterator_new:
 * @record_batches: (element-type GArrowRecordBatch):
 *   The record batches.
 *
 * Returns: A newly created #GArrowRecordBatchIterator.
 *
 * Since: 0.17.0
 */
GArrowRecordBatchIterator *
garrow_record_batch_iterator_new(GList *record_batches)
{
  std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_record_batches;
  for (auto node = record_batches; node; node = node->next) {
    auto record_batch = GARROW_RECORD_BATCH(node->data);
    arrow_record_batches.push_back(garrow_record_batch_get_raw(record_batch));
  }

  auto arrow_iterator = arrow::MakeVectorIterator(arrow_record_batches);
  return garrow_record_batch_iterator_new_raw(&arrow_iterator);
}

/**
 * garrow_record_batch_iterator_next:
 * @iterator: A #GArrowRecordBatchIterator.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   The next #GArrowRecordBatch, or %NULL when the iterator is completed.
 *
 * Since: 0.17.0
 */
GArrowRecordBatch *
garrow_record_batch_iterator_next(GArrowRecordBatchIterator *iterator,
                                  GError **error)
{
  auto priv = GARROW_RECORD_BATCH_ITERATOR_GET_PRIVATE(iterator);

  auto result = priv->iterator.Next();
  if (garrow::check(error, result, "[record-batch-iterator][next]")) {
    auto arrow_record_batch = *result;
    if (arrow_record_batch) {
      return garrow_record_batch_new_raw(&arrow_record_batch);
    }
  }
  return NULL;
}

/**
 * garrow_record_batch_iterator_equal:
 * @iterator: A #GArrowRecordBatchIterator.
 * @other_iterator: A #GArrowRecordBatchIterator to be compared.
 *
 * Returns: %TRUE if both iterators are the same, %FALSE otherwise.
 *
 * Since: 0.17.0
 */
gboolean
garrow_record_batch_iterator_equal(GArrowRecordBatchIterator *iterator,
                                   GArrowRecordBatchIterator *other_iterator)
{
  auto priv = GARROW_RECORD_BATCH_ITERATOR_GET_PRIVATE(iterator);
  auto priv_other = GARROW_RECORD_BATCH_ITERATOR_GET_PRIVATE(other_iterator);
  return priv->iterator.Equals(priv_other->iterator);
}

/**
 * garrow_record_batch_iterator_to_list:
 * @iterator: A #GArrowRecordBatchIterator.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (element-type GArrowRecordBatch) (transfer full):
 *   A #GList contains every moved elements from the iterator.
 *
 * Since: 0.17.0
 */
GList*
garrow_record_batch_iterator_to_list(GArrowRecordBatchIterator *iterator,
                                     GError **error)
{
  auto priv = GARROW_RECORD_BATCH_ITERATOR_GET_PRIVATE(iterator);
  GList *record_batches = NULL;
  for (auto arrow_record_batch_result : priv->iterator) {
    if (!garrow::check(error,
                       arrow_record_batch_result,
                       "[record-batch-iterator][to-list]")) {
      g_list_free_full(record_batches, g_object_unref);
      return NULL;
    }
    auto arrow_record_batch = *std::move(arrow_record_batch_result);
    auto record_batch = garrow_record_batch_new_raw(&arrow_record_batch);
    record_batches = g_list_prepend(record_batches, record_batch);
  }
  return g_list_reverse(record_batches);
}

G_END_DECLS

GArrowRecordBatch *
garrow_record_batch_new_raw(std::shared_ptr<arrow::RecordBatch> *arrow_record_batch)
{
  auto record_batch =
    GARROW_RECORD_BATCH(g_object_new(GARROW_TYPE_RECORD_BATCH,
                                     "record-batch", arrow_record_batch,
                                     NULL));
  return record_batch;
}

std::shared_ptr<arrow::RecordBatch>
garrow_record_batch_get_raw(GArrowRecordBatch *record_batch)
{
  auto priv = GARROW_RECORD_BATCH_GET_PRIVATE(record_batch);
  return priv->record_batch;
}

GArrowRecordBatchIterator *
garrow_record_batch_iterator_new_raw(arrow::RecordBatchIterator *arrow_iterator)
{
  auto iterator = g_object_new(GARROW_TYPE_RECORD_BATCH_ITERATOR,
                               "iterator", arrow_iterator,
                               NULL);
  return GARROW_RECORD_BATCH_ITERATOR(iterator);
}

arrow::RecordBatchIterator *
garrow_record_batch_iterator_get_raw(GArrowRecordBatchIterator *iterator)
{
  auto priv = GARROW_RECORD_BATCH_ITERATOR_GET_PRIVATE(iterator);
  return &priv->iterator;
}
