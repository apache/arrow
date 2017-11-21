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
#include <arrow-glib/error.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/schema.hpp>

#include <sstream>

G_BEGIN_DECLS

/**
 * SECTION: record-batch
 * @short_description: Record batch class
 *
 * #GArrowRecordBatch is a class for record batch. Record batch is
 * similar to #GArrowTable. Record batch also has also zero or more
 * columns and zero or more records.
 *
 * Record batch is used for shared memory IPC.
 */

typedef struct GArrowRecordBatchPrivate_ {
  std::shared_ptr<arrow::RecordBatch> record_batch;
} GArrowRecordBatchPrivate;

enum {
  PROP_0,
  PROP_RECORD_BATCH
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowRecordBatch,
                           garrow_record_batch,
                           G_TYPE_OBJECT)

#define GARROW_RECORD_BATCH_GET_PRIVATE(obj)               \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),               \
                               GARROW_TYPE_RECORD_BATCH,   \
                               GArrowRecordBatchPrivate))

static void
garrow_record_batch_finalize(GObject *object)
{
  GArrowRecordBatchPrivate *priv;

  priv = GARROW_RECORD_BATCH_GET_PRIVATE(object);

  priv->record_batch = nullptr;

  G_OBJECT_CLASS(garrow_record_batch_parent_class)->finalize(object);
}

static void
garrow_record_batch_set_property(GObject *object,
                          guint prop_id,
                          const GValue *value,
                          GParamSpec *pspec)
{
  GArrowRecordBatchPrivate *priv;

  priv = GARROW_RECORD_BATCH_GET_PRIVATE(object);

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
 * garrow_record_batch_new:
 * @schema: The schema of the record batch.
 * @n_rows: The number of the rows in the record batch.
 * @columns: (element-type GArrowArray): The columns in the record batch.
 *
 * Returns: A newly created #GArrowRecordBatch.
 */
GArrowRecordBatch *
garrow_record_batch_new(GArrowSchema *schema,
                        guint32 n_rows,
                        GList *columns)
{
  std::vector<std::shared_ptr<arrow::Array>> arrow_columns;
  for (GList *node = columns; node; node = node->next) {
    GArrowArray *column = GARROW_ARRAY(node->data);
    arrow_columns.push_back(garrow_array_get_raw(column));
  }

  auto arrow_record_batch =
    arrow::RecordBatch::Make(garrow_schema_get_raw(schema),
                             n_rows, arrow_columns);
  return garrow_record_batch_new_raw(&arrow_record_batch);
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
 * garrow_record_batch_get_column:
 * @record_batch: A #GArrowRecordBatch.
 * @i: The index of the target column.
 *
 * Returns: (transfer full): The i-th column in the record batch.
 */
GArrowArray *
garrow_record_batch_get_column(GArrowRecordBatch *record_batch,
                               guint i)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  auto arrow_column = arrow_record_batch->column(i);
  return garrow_array_new_raw(&arrow_column);
}

/**
 * garrow_record_batch_get_columns:
 * @record_batch: A #GArrowRecordBatch.
 *
 * Returns: (element-type GArrowArray) (transfer full):
 *   The columns in the record batch.
 */
GList *
garrow_record_batch_get_columns(GArrowRecordBatch *record_batch)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);

  GList *columns = NULL;
  for (int i = 0; i < arrow_record_batch->num_columns(); ++i) {
    auto arrow_column = arrow_record_batch->column(i);
    GArrowArray *column = garrow_array_new_raw(&arrow_column);
    columns = g_list_prepend(columns, column);
  }

  return g_list_reverse(columns);
}

/**
 * garrow_record_batch_get_column_name:
 * @record_batch: A #GArrowRecordBatch.
 * @i: The index of the target column.
 *
 * Returns: The name of the i-th column in the record batch.
 */
const gchar *
garrow_record_batch_get_column_name(GArrowRecordBatch *record_batch,
                                    guint i)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
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
 * Returns: (nullable): The formatted record batch content or %NULL on error.
 *
 *   The returned string should be freed when with g_free() when no
 *   longer needed.
 *
 * Since: 0.4.0
 */
gchar *
garrow_record_batch_to_string(GArrowRecordBatch *record_batch, GError **error)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  std::stringstream sink;
  auto status = arrow::PrettyPrint(*arrow_record_batch, 0, &sink);
  if (garrow_error_check(error, status, "[record-batch][to-string]")) {
    return g_strdup(sink.str().c_str());
  } else {
    return NULL;
  }
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
  GArrowRecordBatchPrivate *priv;

  priv = GARROW_RECORD_BATCH_GET_PRIVATE(record_batch);
  return priv->record_batch;
}
