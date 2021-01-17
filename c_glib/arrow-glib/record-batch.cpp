/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 5.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-5.0
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

#include <arrow/ipc/api.h>

#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>

#include <arrow-glib/array.hpp>
#include <arrow-glib/array-builder.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/schema.hpp>
#include <arrow-glib/field.hpp>
#include <arrow-glib/buffer.hpp>


#include <sstream>

GArrowArray *array_beginPos1;
GArrowArray *array_sam1;

GArrowInt32ArrayBuilder *builder_beginPos1;
GArrowStringArrayBuilder *builder_sam1;

GArrowArray *array_beginPos1_1;
GArrowArray *array_sam1_1;
GArrowInt32ArrayBuilder *builder_beginPos1_1;
GArrowStringArrayBuilder *builder_sam1_1;

GArrowArray *array_beginPos1_2;
GArrowArray *array_sam1_2;
GArrowInt32ArrayBuilder *builder_beginPos1_2;
GArrowStringArrayBuilder *builder_sam1_2;

GArrowArray *array_beginPos1_3;
GArrowArray *array_sam1_3;
GArrowInt32ArrayBuilder *builder_beginPos1_3;
GArrowStringArrayBuilder *builder_sam1_3;

GArrowArray *array_beginPos1_4;
GArrowArray *array_sam1_4;
GArrowInt32ArrayBuilder *builder_beginPos1_4;
GArrowStringArrayBuilder *builder_sam1_4;

////////////////////////////////////////////////
GArrowArray *array_beginPos2;
GArrowArray *array_sam2;
GArrowInt32ArrayBuilder *builder_beginPos2;
GArrowStringArrayBuilder *builder_sam2;

GArrowArray *array_beginPos2_1;
GArrowArray *array_sam2_1;
GArrowInt32ArrayBuilder *builder_beginPos2_1;
GArrowStringArrayBuilder *builder_sam2_1;

GArrowArray *array_beginPos2_2;
GArrowArray *array_sam2_2;
GArrowInt32ArrayBuilder *builder_beginPos2_2;
GArrowStringArrayBuilder *builder_sam2_2;

GArrowArray *array_beginPos2_3;
GArrowArray *array_sam2_3;
GArrowInt32ArrayBuilder *builder_beginPos2_3;
GArrowStringArrayBuilder *builder_sam2_3;

GArrowArray *array_beginPos2_4;
GArrowArray *array_sam2_4;
GArrowInt32ArrayBuilder *builder_beginPos2_4;
GArrowStringArrayBuilder *builder_sam2_4;

////////////////////////////////////////////////
GArrowArray *array_beginPos3;
GArrowArray *array_sam3;
GArrowInt32ArrayBuilder *builder_beginPos3;
GArrowStringArrayBuilder *builder_sam3;

GArrowArray *array_beginPos3_1;
GArrowArray *array_sam3_1;
GArrowInt32ArrayBuilder *builder_beginPos3_1;
GArrowStringArrayBuilder *builder_sam3_1;

GArrowArray *array_beginPos3_2;
GArrowArray *array_sam3_2;
GArrowInt32ArrayBuilder *builder_beginPos3_2;
GArrowStringArrayBuilder *builder_sam3_2;

GArrowArray *array_beginPos3_3;
GArrowArray *array_sam3_3;
GArrowInt32ArrayBuilder *builder_beginPos3_3;
GArrowStringArrayBuilder *builder_sam3_3;

////////////////////////////////////////////////
GArrowArray *array_beginPos4;
GArrowArray *array_sam4;
GArrowInt32ArrayBuilder *builder_beginPos4;
GArrowStringArrayBuilder *builder_sam4;

GArrowArray *array_beginPos4_1;
GArrowArray *array_sam4_1;
GArrowInt32ArrayBuilder *builder_beginPos4_1;
GArrowStringArrayBuilder *builder_sam4_1;

GArrowArray *array_beginPos4_2;
GArrowArray *array_sam4_2;
GArrowInt32ArrayBuilder *builder_beginPos4_2;
GArrowStringArrayBuilder *builder_sam4_2;

GArrowArray *array_beginPos4_3;
GArrowArray *array_sam4_3;
GArrowInt32ArrayBuilder *builder_beginPos4_3;
GArrowStringArrayBuilder *builder_sam4_3;

////////////////////////////////////////////////
GArrowArray *array_beginPos5;
GArrowArray *array_sam5;
GArrowInt32ArrayBuilder *builder_beginPos5;
GArrowStringArrayBuilder *builder_sam5;

GArrowArray *array_beginPos5_1;
GArrowArray *array_sam5_1;
GArrowInt32ArrayBuilder *builder_beginPos5_1;
GArrowStringArrayBuilder *builder_sam5_1;

GArrowArray *array_beginPos5_2;
GArrowArray *array_sam5_2;
GArrowInt32ArrayBuilder *builder_beginPos5_2;
GArrowStringArrayBuilder *builder_sam5_2;

GArrowArray *array_beginPos5_3;
GArrowArray *array_sam5_3;
GArrowInt32ArrayBuilder *builder_beginPos5_3;
GArrowStringArrayBuilder *builder_sam5_3;

////////////////////////////////////////////////
GArrowArray *array_beginPos6;
GArrowArray *array_sam6;
GArrowInt32ArrayBuilder *builder_beginPos6;
GArrowStringArrayBuilder *builder_sam6;

GArrowArray *array_beginPos6_1;
GArrowArray *array_sam6_1;
GArrowInt32ArrayBuilder *builder_beginPos6_1;
GArrowStringArrayBuilder *builder_sam6_1;

GArrowArray *array_beginPos6_2;
GArrowArray *array_sam6_2;
GArrowInt32ArrayBuilder *builder_beginPos6_2;
GArrowStringArrayBuilder *builder_sam6_2;

GArrowArray *array_beginPos6_3;
GArrowArray *array_sam6_3;
GArrowInt32ArrayBuilder *builder_beginPos6_3;
GArrowStringArrayBuilder *builder_sam6_3;

////////////////////////////////////////////////
GArrowArray *array_beginPos7;
GArrowArray *array_sam7;
GArrowInt32ArrayBuilder *builder_beginPos7;
GArrowStringArrayBuilder *builder_sam7;

GArrowArray *array_beginPos7_1
GArrowArray *array_sam7_1;
GArrowInt32ArrayBuilder *builder_beginPos7_1;
GArrowStringArrayBuilder *builder_sam7_1;

GArrowArray *array_beginPos7_2;
GArrowArray *array_sam7_2;
GArrowInt32ArrayBuilder *builder_beginPos7_2;
GArrowStringArrayBuilder *builder_sam7_2;


////////////////////////////////////////////////
GArrowArray *array_beginPos8;
GArrowArray *array_sam8;
GArrowInt32ArrayBuilder *builder_beginPos8;
GArrowStringArrayBuilder *builder_sam8;

GArrowArray *array_beginPos8_1
GArrowArray *array_sam8_1;
GArrowInt32ArrayBuilder *builder_beginPos8_1;
GArrowStringArrayBuilder *builder_sam8_1;

GArrowArray *array_beginPos8_2;
GArrowArray *array_sam8_2;
GArrowInt32ArrayBuilder *builder_beginPos8_2;
GArrowStringArrayBuilder *builder_sam8_2;

////////////////////////////////////////////////
GArrowArray *array_beginPos9;
GArrowArray *array_sam9;
GArrowInt32ArrayBuilder *builder_beginPos9;
GArrowStringArrayBuilder *builder_sam9;

GArrowArray *array_beginPos9_1
GArrowArray *array_sam9_1;
GArrowInt32ArrayBuilder *builder_beginPos9_1;
GArrowStringArrayBuilder *builder_sam9_1;

GArrowArray *array_beginPos9_2;
GArrowArray *array_sam9_2;
GArrowInt32ArrayBuilder *builder_beginPos9_2;
GArrowStringArrayBuilder *builder_sam9_2;

////////////////////////////////////////////////
GArrowArray *array_beginPos10;
GArrowArray *array_sam10;
GArrowInt32ArrayBuilder *builder_beginPos10;
GArrowStringArrayBuilder *builder_sam10;

GArrowArray *array_beginPos10_1
GArrowArray *array_sam10_1;
GArrowInt32ArrayBuilder *builder_beginPos10_1;
GArrowStringArrayBuilder *builder_sam10_1;

GArrowArray *array_beginPos10_2;
GArrowArray *array_sam10_2;
GArrowInt32ArrayBuilder *builder_beginPos10_2;
GArrowStringArrayBuilder *builder_sam10_2;

////////////////////////////////////////////////
GArrowArray *array_beginPos11;
GArrowArray *array_sam11;
GArrowInt32ArrayBuilder *builder_beginPos11;
GArrowStringArrayBuilder *builder_sam11;

GArrowArray *array_beginPos11_1
GArrowArray *array_sam11_1;
GArrowInt32ArrayBuilder *builder_beginPos11_1;
GArrowStringArrayBuilder *builder_sam11_1;

GArrowArray *array_beginPos11_2;
GArrowArray *array_sam11_2;
GArrowInt32ArrayBuilder *builder_beginPos11_2;
GArrowStringArrayBuilder *builder_sam11_2;

////////////////////////////////////////////////
GArrowArray *array_beginPos12;
GArrowArray *array_sam12;
GArrowInt32ArrayBuilder *builder_beginPos12;
GArrowStringArrayBuilder *builder_sam12;

GArrowArray *array_beginPos12_1
GArrowArray *array_sam12_1;
GArrowInt32ArrayBuilder *builder_beginPos12_1;
GArrowStringArrayBuilder *builder_sam12_1;

GArrowArray *array_beginPos12_2;
GArrowArray *array_sam12_2;
GArrowInt32ArrayBuilder *builder_beginPos12_2;
GArrowStringArrayBuilder *builder_sam12_2;

////////////////////////////////////////////////
GArrowArray *array_beginPos13;
GArrowArray *array_sam13;
GArrowInt32ArrayBuilder *builder_beginPos13;
GArrowStringArrayBuilder *builder_sam13;

GArrowArray *array_beginPos13_1
GArrowArray *array_sam13_1;
GArrowInt32ArrayBuilder *builder_beginPos13_1;
GArrowStringArrayBuilder *builder_sam13_1;

////////////////////////////////////////////////
GArrowArray *array_beginPos14;
GArrowArray *array_sam14;
GArrowInt32ArrayBuilder *builder_beginPos14;
GArrowStringArrayBuilder *builder_sam14;

GArrowArray *array_beginPos14_1
GArrowArray *array_sam14_1;
GArrowInt32ArrayBuilder *builder_beginPos14_1;
GArrowStringArrayBuilder *builder_sam14_1;

////////////////////////////////////////////////
GArrowArray *array_beginPos15;
GArrowArray *array_sam15;
GArrowInt32ArrayBuilder *builder_beginPos15;
GArrowStringArrayBuilder *builder_sam15;

GArrowArray *array_beginPos15_1
GArrowArray *array_sam15_1;
GArrowInt32ArrayBuilder *builder_beginPos15_1;
GArrowStringArrayBuilder *builder_sam15_1;

////////////////////////////////////////////////
GArrowArray *array_beginPos16;
GArrowArray *array_sam16;
GArrowInt32ArrayBuilder *builder_beginPos16;
GArrowStringArrayBuilder *builder_sam16;

GArrowArray *array_beginPos16_1
GArrowArray *array_sam16_1;
GArrowInt32ArrayBuilder *builder_beginPos16_1;
GArrowStringArrayBuilder *builder_sam16_1;

////////////////////////////////////////////////
GArrowArray *array_beginPos17;
GArrowArray *array_sam17;
GArrowInt32ArrayBuilder *builder_beginPos17;
GArrowStringArrayBuilder *builder_sam17;

GArrowArray *array_beginPos17_1
GArrowArray *array_sam17_1;
GArrowInt32ArrayBuilder *builder_beginPos17_1;
GArrowStringArrayBuilder *builder_sam17_1;

////////////////////////////////////////////////
GArrowArray *array_beginPos18;
GArrowArray *array_sam18;
GArrowInt32ArrayBuilder *builder_beginPos18;
GArrowStringArrayBuilder *builder_sam18;

GArrowArray *array_beginPos18_1
GArrowArray *array_sam18_1;
GArrowInt32ArrayBuilder *builder_beginPos18_1;
GArrowStringArrayBuilder *builder_sam18_1;


////////////////////////////////////////////////
GArrowArray *array_beginPos19;
GArrowArray *array_sam19;
GArrowInt32ArrayBuilder *builder_beginPos19;
GArrowStringArrayBuilder *builder_sam19;

////////////////////////////////////////////////
GArrowArray *array_beginPos20;
GArrowArray *array_sam20;
GArrowInt32ArrayBuilder *builder_beginPos20;
GArrowStringArrayBuilder *builder_sam20;

////////////////////////////////////////////////
GArrowArray *array_beginPos21;
GArrowArray *array_sam21;
GArrowInt32ArrayBuilder *builder_beginPos21;
GArrowStringArrayBuilder *builder_sam21;

////////////////////////////////////////////////
GArrowArray *array_beginPos22;
GArrowArray *array_sam22;
GArrowInt32ArrayBuilder *builder_beginPos22;
GArrowStringArrayBuilder *builder_sam22;


///////////////////////////////////////////////
GArrowArray *array_beginPosX;
GArrowArray *array_samX;
GArrowInt32ArrayBuilder *builder_beginPosX;
GArrowStringArrayBuilder *builder_samX;

GArrowArray *array_beginPosX_1
GArrowArray *array_samX_1;
GArrowInt32ArrayBuilder *builder_beginPosX_1;
GArrowStringArrayBuilder *builder_samX_1;

GArrowArray *array_beginPosX_2;
GArrowArray *array_samX_2;
GArrowInt32ArrayBuilder *builder_beginPosX_2;
GArrowStringArrayBuilder *builder_samX_2;

////////////////////////////////////////////////
GArrowArray *array_beginPosY;
GArrowArray *array_samY;
GArrowInt32ArrayBuilder *builder_beginPosY;
GArrowStringArrayBuilder *builder_samY;

////////////////////////////////////////////////
GArrowArray *array_beginPosM;
GArrowArray *array_samM;
GArrowInt32ArrayBuilder *builder_beginPosM;
GArrowStringArrayBuilder *builder_samM;

////////////////////////////////////////////////

static inline bool
garrow_record_batch_adjust_index(const std::shared_ptr<arrow::RecordBatch> arrow_record_batch,
                                 gint &i)
{
  auto n_columns = arrow_record_batch->num_columns();
  if (i < 0) {
    i += n_columns;
    if (i < 0) {
      return false;
    }
  }
  if (i >= n_columns) {
    return false;
  }
  return true;
}

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
  std::vector<std::shared_ptr<arrow::Array>> arrow_columns;
  for (GList *node = columns; node; node = node->next) {
    GArrowArray *column = GARROW_ARRAY(node->data);
    arrow_columns.push_back(garrow_array_get_raw(column));
  }

  auto arrow_record_batch =
    arrow::RecordBatch::Make(garrow_schema_get_raw(schema),
                             n_rows, arrow_columns);
  auto status = arrow_record_batch->Validate();
  if (garrow_error_check(error, status, "[record-batch][new]")) {
    return garrow_record_batch_new_raw(&arrow_record_batch);
  } else {
    return NULL;
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
 * Since: 0.5.0
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
 * @i: The index of the target column. If it's negative, index is
 *   counted backward from the end of the columns. `-1` means the last
 *   column.
 *
 * Returns: (transfer full) (nullable): The i-th column in the record batch
 *   on success, %NULL on out of index.
 */
GArrowArray *
garrow_record_batch_get_column(GArrowRecordBatch *record_batch,
                               gint i)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  if (!garrow_record_batch_adjust_index(arrow_record_batch, i)) {
    return NULL;
  }
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
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  if (!garrow_record_batch_adjust_index(arrow_record_batch, i)) {
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
 * Returns: (nullable): The formatted record batch content or %NULL on error.
 *
 *   The returned string should be freed when with g_free() when no
 *   longer needed.
 *
 * Since: 0.5.0
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
  std::shared_ptr<arrow::RecordBatch> arrow_new_record_batch;
  auto status = arrow_record_batch->AddColumn(i, arrow_field, arrow_column, &arrow_new_record_batch);
  if (garrow_error_check(error, status, "[record-batch][add-column]")) {
    return garrow_record_batch_new_raw(&arrow_new_record_batch);
  } else {
    return NULL;
  }
}

GArrowBuffer * 
GSerializeRecordBatch(GArrowRecordBatch *record_batch)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);

  std::shared_ptr<arrow::ResizableBuffer> resizable_buffer;
  arrow::AllocateResizableBuffer(arrow::default_memory_pool(), 0, &resizable_buffer);

  std::shared_ptr<arrow::Buffer> buffer = std::dynamic_pointer_cast<arrow::Buffer>(resizable_buffer);
  arrow::ipc::SerializeRecordBatch(*arrow_record_batch, arrow::default_memory_pool(), &buffer);

  return garrow_buffer_new_raw(&buffer);

}

GArrowRecordBatch * 
GDeSerializeRecordBatch(GArrowBuffer *buffer, GArrowSchema *schema)
{

  std::shared_ptr<arrow::RecordBatch> arrow_new_record_batch;
  const auto arrow_schema = garrow_schema_get_raw(schema);

  auto arrow_buffer = garrow_buffer_get_raw(buffer);
  //auto arrow_buffer_reader = std::make_shared<arrow::io::BufferReader>(arrow_buffer);
  arrow::io::BufferReader buf_reader(arrow_buffer);

  arrow::ipc::ReadRecordBatch(arrow_schema, &buf_reader, &arrow_new_record_batch);
 
return garrow_record_batch_new_raw(&arrow_new_record_batch);

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
  std::shared_ptr<arrow::RecordBatch> arrow_new_record_batch;
  auto status = arrow_record_batch->RemoveColumn(i, &arrow_new_record_batch);
  if (garrow_error_check(error, status, "[record-batch][remove-column]")) {
    return garrow_record_batch_new_raw(&arrow_new_record_batch);
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




GArrowSchema* getSchema(void)
{
    GArrowSchema *schema;
    //RecordBatch creation
    GArrowField *f0 = garrow_field_new("beginPoss", GARROW_DATA_TYPE(garrow_int32_data_type_new()));
    GArrowField *f1 = garrow_field_new("sam", GARROW_DATA_TYPE(garrow_string_data_type_new()));

    GList *fields = NULL;
    fields = g_list_append(fields, f0);
    fields = g_list_append(fields, f1);

    //Create schema and free unnecessary fields
    schema = garrow_schema_new(fields);
    g_list_free(fields);
    g_object_unref(f0);
    g_object_unref(f1);

    return schema;
}

GArrowRecordBatch * create_arrow_record_batch(gint64 count, GArrowArray *array_beginPos,GArrowArray *array_sam)
{
    GArrowSchema *schema;
    GArrowRecordBatch *batch_genomics;

    schema = getSchema();

    GList *columns_genomics;
    columns_genomics = g_list_append(columns_genomics,array_beginPos);
    columns_genomics = g_list_append(columns_genomics,array_sam);

    batch_genomics = garrow_record_batch_new(schema,count,columns_genomics,NULL);

    g_list_free(columns_genomics);

    return batch_genomics;
}

void arrow_builders_start(void)
{
  builder_beginPos1 = garrow_int32_array_builder_new();
  builder_sam1 = garrow_string_array_builder_new();

  builder_beginPos1_1 = garrow_int32_array_builder_new();
  builder_sam1_1 = garrow_string_array_builder_new();

  builder_beginPos1_2 = garrow_int32_array_builder_new();
  builder_sam1_2 = garrow_string_array_builder_new();

  builder_beginPos1_3 = garrow_int32_array_builder_new();
  builder_sam1_3 = garrow_string_array_builder_new();

  builder_beginPos1_4 = garrow_int32_array_builder_new();
  builder_sam1_4 = garrow_string_array_builder_new();
///////////////////////////////////////////////////

  builder_beginPos2 = garrow_int32_array_builder_new();
  builder_sam2 = garrow_string_array_builder_new();

  builder_beginPos2_1 = garrow_int32_array_builder_new();
  builder_sam2_1 = garrow_string_array_builder_new();

  builder_beginPos2_2 = garrow_int32_array_builder_new();
  builder_sam2_2 = garrow_string_array_builder_new();

  builder_beginPos2_3 = garrow_int32_array_builder_new();
  builder_sam2_3 = garrow_string_array_builder_new();

  builder_beginPos2_4 = garrow_int32_array_builder_new();
  builder_sam2_4 = garrow_string_array_builder_new();
///////////////////////////////////////////////////

  builder_beginPos3 = garrow_int32_array_builder_new();
  builder_sam3 = garrow_string_array_builder_new();

  builder_beginPos3_1 = garrow_int32_array_builder_new();
  builder_sam3_1 = garrow_string_array_builder_new();

  builder_beginPos3_2 = garrow_int32_array_builder_new();
  builder_sam3_2 = garrow_string_array_builder_new();

  builder_beginPos3_3 = garrow_int32_array_builder_new();
  builder_sam3_3 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos4 = garrow_int32_array_builder_new();
  builder_sam4 = garrow_string_array_builder_new();

  builder_beginPos4_1 = garrow_int32_array_builder_new();
  builder_sam4_1 = garrow_string_array_builder_new();

  builder_beginPos4_2 = garrow_int32_array_builder_new();
  builder_sam4_2 = garrow_string_array_builder_new();

  builder_beginPos4_3 = garrow_int32_array_builder_new();
  builder_sam4_3 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos5 = garrow_int32_array_builder_new();
  builder_sam5 = garrow_string_array_builder_new();

  builder_beginPos5_1 = garrow_int32_array_builder_new();
  builder_sam5_1 = garrow_string_array_builder_new();

  builder_beginPos5_2 = garrow_int32_array_builder_new();
  builder_sam5_2 = garrow_string_array_builder_new();

  builder_beginPos5_3 = garrow_int32_array_builder_new();
  builder_sam5_3 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos6 = garrow_int32_array_builder_new();
  builder_sam6 = garrow_string_array_builder_new();

  builder_beginPos6_1 = garrow_int32_array_builder_new();
  builder_sam6_1 = garrow_string_array_builder_new();

  builder_beginPos6_2 = garrow_int32_array_builder_new();
  builder_sam6_2 = garrow_string_array_builder_new();

  builder_beginPos6_3 = garrow_int32_array_builder_new();
  builder_sam6_3 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos7 = garrow_int32_array_builder_new();
  builder_sam7 = garrow_string_array_builder_new();

  builder_beginPos7_1 = garrow_int32_array_builder_new();
  builder_sam7_1 = garrow_string_array_builder_new();

  builder_beginPos7_2 = garrow_int32_array_builder_new();
  builder_sam7_2 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos8 = garrow_int32_array_builder_new();
  builder_sam8 = garrow_string_array_builder_new();

  builder_beginPos8_1 = garrow_int32_array_builder_new();
  builder_sam8_1 = garrow_string_array_builder_new();

  builder_beginPos8_2 = garrow_int32_array_builder_new();
  builder_sam8_2 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos9 = garrow_int32_array_builder_new();
  builder_sam9 = garrow_string_array_builder_new();

  builder_beginPos9_1 = garrow_int32_array_builder_new();
  builder_sam9_1 = garrow_string_array_builder_new();

  builder_beginPos9_2 = garrow_int32_array_builder_new();
  builder_sam9_2 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos10 = garrow_int32_array_builder_new();
  builder_sam10 = garrow_string_array_builder_new();

  builder_beginPos10_1 = garrow_int32_array_builder_new();
  builder_sam10_1 = garrow_string_array_builder_new();

  builder_beginPos10_2 = garrow_int32_array_builder_new();
  builder_sam10_2 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos11 = garrow_int32_array_builder_new();
  builder_sam11 = garrow_string_array_builder_new();

  builder_beginPos11_1 = garrow_int32_array_builder_new();
  builder_sam11_1 = garrow_string_array_builder_new();

  builder_beginPos11_2 = garrow_int32_array_builder_new();
  builder_sam11_2 = garrow_string_array_builder_new();


///////////////////////////////////////////////////

  builder_beginPos12 = garrow_int32_array_builder_new();
  builder_sam12 = garrow_string_array_builder_new();

  builder_beginPos12_1 = garrow_int32_array_builder_new();
  builder_sam12_1 = garrow_string_array_builder_new();

  builder_beginPos12_2 = garrow_int32_array_builder_new();
  builder_sam12_2 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos13 = garrow_int32_array_builder_new();
  builder_sam13 = garrow_string_array_builder_new();

  builder_beginPos13_1 = garrow_int32_array_builder_new();
  builder_sam13_1 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos14 = garrow_int32_array_builder_new();
  builder_sam14 = garrow_string_array_builder_new();

  builder_beginPos14_1 = garrow_int32_array_builder_new();
  builder_sam14_1 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos15 = garrow_int32_array_builder_new();
  builder_sam15 = garrow_string_array_builder_new();

  builder_beginPos15_1 = garrow_int32_array_builder_new();
  builder_sam15_1 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos16 = garrow_int32_array_builder_new();
  builder_sam16 = garrow_string_array_builder_new();

  builder_beginPos16_1 = garrow_int32_array_builder_new();
  builder_sam16_1 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos17 = garrow_int32_array_builder_new();
  builder_sam17 = garrow_string_array_builder_new();

  builder_beginPos17_1 = garrow_int32_array_builder_new();
  builder_sam17_1 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos18 = garrow_int32_array_builder_new();
  builder_sam18 = garrow_string_array_builder_new();

  builder_beginPos18_1 = garrow_int32_array_builder_new();
  builder_sam18_1 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos19 = garrow_int32_array_builder_new();
  builder_sam19 = garrow_string_array_builder_new();
///////////////////////////////////////////////////

  builder_beginPos20 = garrow_int32_array_builder_new();
  builder_sam20 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPos21 = garrow_int32_array_builder_new();
  builder_sam21 = garrow_string_array_builder_new();
///////////////////////////////////////////////////

  builder_beginPos22 = garrow_int32_array_builder_new();
  builder_sam22 = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPosX = garrow_int32_array_builder_new();
  builder_samX = garrow_string_array_builder_new();

  builder_beginPosX_1 = garrow_int32_array_builder_new();
  builder_samX_1 = garrow_string_array_builder_new();

  builder_beginPosX_2 = garrow_int32_array_builder_new();
  builder_samX_2 = garrow_string_array_builder_new();


///////////////////////////////////////////////////

  builder_beginPosY = garrow_int32_array_builder_new();
  builder_samY = garrow_string_array_builder_new();

///////////////////////////////////////////////////

  builder_beginPosM = garrow_int32_array_builder_new();
  builder_samM = garrow_string_array_builder_new();



}

gboolean
arrow_builders_append(gint32 builder_id, gint32 beginPos, const gchar *sam)
    {
        gboolean success = TRUE;
        GError *error = NULL;

        if(builder_id == 1) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam1, sam, &error);
            }
        }
	else if(builder_id == 110) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos1_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam1_1, sam, &error);
            }
        }
	else if(builder_id == 120) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos1_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam1_2, sam, &error);
            }
        }
        else if(builder_id == 130) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos1_3, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam1_3, sam, &error);
            }
        }
        else if(builder_id == 140) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos1_4, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam1_4, sam, &error);
            }
        }

else if(builder_id == 2) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam2, sam, &error);
            }
        }
	else if(builder_id == 210) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos2_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam2_1, sam, &error);
            }
        }
	else if(builder_id == 220) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos2_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam2_2, sam, &error);
            }
        }
        else if(builder_id == 230) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos2_3, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam2_3, sam, &error);
            }
        }
        else if(builder_id == 240) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos2_4, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam2_4, sam, &error);
            }
        }
else if(builder_id == 3) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos3, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam3, sam, &error);
            }
        }
	else if(builder_id == 31) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos3_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam3_1, sam, &error);
            }
        }
	else if(builder_id == 32) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos3_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam3_2, sam, &error);
            }
        }
        else if(builder_id == 33) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos3_3, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam3_3, sam, &error);
            }
        }

else if(builder_id == 4) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos4, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam4, sam, &error);
            }
        }
	else if(builder_id == 41) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos4_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam4_1, sam, &error);
            }
        }
	else if(builder_id == 42) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos4_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam4_2, sam, &error);
            }
        }
        else if(builder_id == 43) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos4_3, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam4_3, sam, &error);
            }
        }
else if(builder_id == 5) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos5, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam5, sam, &error);
            }
        }
	else if(builder_id == 51) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos5_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam5_1, sam, &error);
            }
        }
	else if(builder_id == 52) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos5_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam5_2, sam, &error);
            }
        }
        else if(builder_id == 53) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos5_3, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam5_3, sam, &error);
            }
        }
else if(builder_id == 6) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos6, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam6, sam, &error);
            }
        }
	else if(builder_id == 61) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos6_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam6_1, sam, &error);
            }
        }
	else if(builder_id == 62) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos6_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam6_2, sam, &error);
            }
        }
        else if(builder_id == 63) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos6_3, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam6_3, sam, &error);
            }
        }

else if(builder_id == 7) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos7, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam7, sam, &error);
            }
        }
	else if(builder_id == 71) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos7_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam7_1, sam, &error);
            }
        }
	else if(builder_id == 72) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos7_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam7_2, sam, &error);
            }
        }
else if(builder_id == 8) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos8, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam8, sam, &error);
            }
        }
	else if(builder_id == 81) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos8_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam8_1, sam, &error);
            }
        }
	else if(builder_id == 82) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos8_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam8_2, sam, &error);
            }
        }
else if(builder_id == 9) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos9, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam9, sam, &error);
            }
        }
	else if(builder_id == 91) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos9_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam9_1, sam, &error);
            }
        }
	else if(builder_id == 92) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos9_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam9_2, sam, &error);
            }
        }
else if(builder_id == 10) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos10, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam10, sam, &error);
            }
        }
	else if(builder_id == 101) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos10_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam10_1, sam, &error);
            }
        }
	else if(builder_id == 102) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos10_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam10_2, sam, &error);
            }
        }

else if(builder_id == 11) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos11, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam11, sam, &error);
            }
        }
	else if(builder_id == 111) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos11_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam11_1, sam, &error);
            }
        }
	else if(builder_id == 112) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos11_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam11_2, sam, &error);
            }
        }

else if(builder_id == 12) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos12, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam12, sam, &error);
            }
        }
	else if(builder_id == 121) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos12_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam12_1, sam, &error);
            }
        }
	else if(builder_id == 122) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos12_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam12_2, sam, &error);
            }
        }

else if(builder_id == 13) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos13, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam13, sam, &error);
            }
        }
	else if(builder_id == 131) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos13_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam13_1, sam, &error);
            }
        }

else if(builder_id == 14) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos14, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam14, sam, &error);
            }
        }
	else if(builder_id == 141) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos14_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam14_1, sam, &error);
            }
        }

else if(builder_id == 15) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos15, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam15, sam, &error);
            }
        }
	else if(builder_id == 151) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos15_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam15_1, sam, &error);
            }
        }

else if(builder_id == 16) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos16, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam16, sam, &error);
            }
        }
	else if(builder_id == 161) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos16_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam16_1, sam, &error);
            }
        }

else if(builder_id == 17) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos17, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam17, sam, &error);
            }
        }
	else if(builder_id == 171) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos17_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam17_1, sam, &error);
            }
        }

else if(builder_id == 18) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos18, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam18, sam, &error);
            }
        }
	else if(builder_id == 181) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos18_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam18_1, sam, &error);
            }
        }

else if(builder_id == 19) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos19, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam19, sam, &error);
            }
        }
else if(builder_id == 20) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos20, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam20, sam, &error);
            }
        }
else if(builder_id == 21) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos21, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam21, sam, &error);
            }
        }
else if(builder_id == 22) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos22, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_sam22, sam, &error);
            }
        }

else if(builder_id == 23) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPosX, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_samX, sam, &error);
            }
        }
	else if(builder_id == 231) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPosX_1, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_samX_1, sam, &error);
            }
        }
	else if(builder_id == 232) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPosX_2, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_samX_2, sam, &error);
            }
        }


else if(builder_id == 24) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPosY, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_samY, sam, &error);
            }
        }
else if(builder_id == 25) {
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPosM, beginPos, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_samM, sam, &error);
            }
        }


   return success;
}

GArrowRecordBatch *
arrow_builders_finish(gint32 builder_id, gint64 count)
{
 GError *error = NULL;
 GArrowRecordBatch *batch_genomics;
    if(builder_id == 1) {
        array_beginPos1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos1), &error);
        array_sam1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam1), &error);
        g_object_unref(builder_beginPos1);
        g_object_unref(builder_sam1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos1,array_sam1);

        g_object_unref(array_beginPos1);
        g_object_unref(array_sam1);
    }
    else if(builder_id == 110) {
        array_beginPos1_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos1_1), &error);
	array_sam1_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam1_1), &error);
        g_object_unref(builder_beginPos1_1);
        g_object_unref(builder_sam1_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos1_1,array_sam1_1);

	g_object_unref(array_beginPos1_1);
        g_object_unref(array_sam1_1);   
    }
    else if(builder_id == 120) {
        array_beginPos1_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos1_2), &error);
        array_sam1_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam1_2), &error);
        g_object_unref(builder_beginPos1_2);
        g_object_unref(builder_sam1_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos1_2,array_sam1_2);

        g_object_unref(array_beginPos1_2);
        g_object_unref(array_sam1_2);
    }
    else if(builder_id == 130) {
        array_beginPos1_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos1_3), &error);
        array_sam1_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam1_3), &error);
        g_object_unref(builder_beginPos1_3);
        g_object_unref(builder_sam1_3);

        batch_genomics = create_arrow_record_batch(count, array_beginPos1_3,array_sam1_3);

        g_object_unref(array_beginPos1_3);
        g_object_unref(array_sam1_3);
    }
    else if(builder_id == 140) {
        array_beginPos1_4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos1_4), &error);
        array_sam1_4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam1_4), &error);
        g_object_unref(builder_beginPos1_4);
        g_object_unref(builder_sam1_4);

        batch_genomics = create_arrow_record_batch(count, array_beginPos1_4,array_sam1_4);

        g_object_unref(array_beginPos1_4);
        g_object_unref(array_sam1_4);
    }

if(builder_id == 2) {
        array_beginPos2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos2), &error);
        array_sam2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam2), &error);
        g_object_unref(builder_beginPos2);
        g_object_unref(builder_sam2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos2,array_sam2);

        g_object_unref(array_beginPos2);
        g_object_unref(array_sam2);
    }
    else if(builder_id == 210) {
        array_beginPos2_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos2_1), &error);
	array_sam2_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam2_1), &error);
        g_object_unref(builder_beginPos2_1);
        g_object_unref(builder_sam2_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos2_1,array_sam2_1);

	g_object_unref(array_beginPos2_1);
        g_object_unref(array_sam2_1);   
    }
    else if(builder_id == 220) {
        array_beginPos2_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos2_2), &error);
        array_sam2_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam2_2), &error);
        g_object_unref(builder_beginPos2_2);
        g_object_unref(builder_sam2_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos2_2,array_sam2_2);

        g_object_unref(array_beginPos2_2);
        g_object_unref(array_sam2_2);
    }
    else if(builder_id == 230) {
        array_beginPos2_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos2_3), &error);
        array_sam2_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam2_3), &error);
        g_object_unref(builder_beginPos2_3);
        g_object_unref(builder_sam2_3);

        batch_genomics = create_arrow_record_batch(count, array_beginPos2_3,array_sam2_3);

        g_object_unref(array_beginPos2_3);
        g_object_unref(array_sam2_3);
    }
    else if(builder_id == 240) {
        array_beginPos2_4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos2_4), &error);
        array_sam2_4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam2_4), &error);
        g_object_unref(builder_beginPos2_4);
        g_object_unref(builder_sam2_4);

        batch_genomics = create_arrow_record_batch(count, array_beginPos2_4,array_sam2_4);

        g_object_unref(array_beginPos2_4);
        g_object_unref(array_sam2_4);
    }

if(builder_id == 3) {
        array_beginPos3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos3), &error);
        array_sam3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam3), &error);
        g_object_unref(builder_beginPos3);
        g_object_unref(builder_sam3);

        batch_genomics = create_arrow_record_batch(count, array_beginPos3,array_sam3);

        g_object_unref(array_beginPos3);
        g_object_unref(array_sam3);
    }
    else if(builder_id == 31) {
        array_beginPos3_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos3_1), &error);
	array_sam3_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam3_1), &error);
        g_object_unref(builder_beginPos3_1);
        g_object_unref(builder_sam3_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos3_1,array_sam3_1);

	g_object_unref(array_beginPos3_1);
        g_object_unref(array_sam3_1);   
    }
    else if(builder_id == 32) {
        array_beginPos3_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos3_2), &error);
        array_sam3_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam3_2), &error);
        g_object_unref(builder_beginPos3_2);
        g_object_unref(builder_sam3_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos3_2,array_sam3_2);

        g_object_unref(array_beginPos3_2);
        g_object_unref(array_sam3_2);
    }
    else if(builder_id == 33) {
        array_beginPos3_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos3_3), &error);
        array_sam3_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam3_3), &error);
        g_object_unref(builder_beginPos3_3);
        g_object_unref(builder_sam3_3);

        batch_genomics = create_arrow_record_batch(count, array_beginPos3_3,array_sam3_3);

        g_object_unref(array_beginPos3_3);
        g_object_unref(array_sam3_3);
    }


if(builder_id == 4) {
        array_beginPos4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos4), &error);
        array_sam4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam4), &error);
        g_object_unref(builder_beginPos4);
        g_object_unref(builder_sam4);

        batch_genomics = create_arrow_record_batch(count, array_beginPos4,array_sam4);

        g_object_unref(array_beginPos4);
        g_object_unref(array_sam4);
    }
    else if(builder_id == 41) {
        array_beginPos4_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos4_1), &error);
	array_sam4_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam4_1), &error);
        g_object_unref(builder_beginPos4_1);
        g_object_unref(builder_sam4_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos4_1,array_sam4_1);

	g_object_unref(array_beginPos4_1);
        g_object_unref(array_sam4_1);   
    }
    else if(builder_id == 42) {
        array_beginPos4_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos4_2), &error);
        array_sam4_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam4_2), &error);
        g_object_unref(builder_beginPos4_2);
        g_object_unref(builder_sam4_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos4_2,array_sam4_2);

        g_object_unref(array_beginPos4_2);
        g_object_unref(array_sam4_2);
    }
    else if(builder_id == 43) {
        array_beginPos4_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos4_3), &error);
        array_sam4_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam4_3), &error);
        g_object_unref(builder_beginPos4_3);
        g_object_unref(builder_sam4_3);

        batch_genomics = create_arrow_record_batch(count, array_beginPos4_3,array_sam4_3);

        g_object_unref(array_beginPos4_3);
        g_object_unref(array_sam4_3);
    }

if(builder_id == 5) {
        array_beginPos5 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos5), &error);
        array_sam5 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam5), &error);
        g_object_unref(builder_beginPos5);
        g_object_unref(builder_sam5);

        batch_genomics = create_arrow_record_batch(count, array_beginPos5,array_sam5);

        g_object_unref(array_beginPos5);
        g_object_unref(array_sam5);
    }
    else if(builder_id == 51) {
        array_beginPos5_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos5_1), &error);
	array_sam5_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam5_1), &error);
        g_object_unref(builder_beginPos5_1);
        g_object_unref(builder_sam5_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos5_1,array_sam5_1);

	g_object_unref(array_beginPos5_1);
        g_object_unref(array_sam5_1);   
    }
    else if(builder_id == 52) {
        array_beginPos5_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos5_2), &error);
        array_sam5_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam5_2), &error);
        g_object_unref(builder_beginPos5_2);
        g_object_unref(builder_sam5_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos5_2,array_sam5_2);

        g_object_unref(array_beginPos5_2);
        g_object_unref(array_sam5_2);
    }
    else if(builder_id == 53) {
        array_beginPos5_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos5_3), &error);
        array_sam5_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam5_3), &error);
        g_object_unref(builder_beginPos5_3);
        g_object_unref(builder_sam5_3);

        batch_genomics = create_arrow_record_batch(count, array_beginPos5_3,array_sam5_3);

        g_object_unref(array_beginPos5_3);
        g_object_unref(array_sam5_3);
    }

if(builder_id == 6) {
        array_beginPos6 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos6), &error);
        array_sam6 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam6), &error);
        g_object_unref(builder_beginPos6);
        g_object_unref(builder_sam6);

        batch_genomics = create_arrow_record_batch(count, array_beginPos6,array_sam6);

        g_object_unref(array_beginPos6);
        g_object_unref(array_sam6);
    }
    else if(builder_id == 61) {
        array_beginPos6_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos6_1), &error);
	array_sam6_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam6_1), &error);
        g_object_unref(builder_beginPos6_1);
        g_object_unref(builder_sam6_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos6_1,array_sam6_1);

	g_object_unref(array_beginPos6_1);
        g_object_unref(array_sam6_1);   
    }
    else if(builder_id == 62) {
        array_beginPos6_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos6_2), &error);
        array_sam6_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam6_2), &error);
        g_object_unref(builder_beginPos6_2);
        g_object_unref(builder_sam6_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos6_2,array_sam6_2);

        g_object_unref(array_beginPos6_2);
        g_object_unref(array_sam6_2);
    }
    else if(builder_id == 63) {
        array_beginPos6_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos6_3), &error);
        array_sam6_3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam6_3), &error);
        g_object_unref(builder_beginPos6_3);
        g_object_unref(builder_sam6_3);

        batch_genomics = create_arrow_record_batch(count, array_beginPos6_3,array_sam6_3);

        g_object_unref(array_beginPos6_3);
        g_object_unref(array_sam6_3);
    }
if(builder_id == 7) {
        array_beginPos7 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos7), &error);
        array_sam7 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam7), &error);
        g_object_unref(builder_beginPos7);
        g_object_unref(builder_sam7);

        batch_genomics = create_arrow_record_batch(count, array_beginPos7,array_sam7);

        g_object_unref(array_beginPos7);
        g_object_unref(array_sam7);
    }
    else if(builder_id == 71) {
        array_beginPos7_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos7_1), &error);
	array_sam7_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam7_1), &error);
        g_object_unref(builder_beginPos7_1);
        g_object_unref(builder_sam7_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos7_1,array_sam7_1);

	g_object_unref(array_beginPos7_1);
        g_object_unref(array_sam7_1);   
    }
    else if(builder_id == 72) {
        array_beginPos7_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos7_2), &error);
        array_sam7_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam7_2), &error);
        g_object_unref(builder_beginPos7_2);
        g_object_unref(builder_sam7_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos7_2,array_sam7_2);

        g_object_unref(array_beginPos7_2);
        g_object_unref(array_sam7_2);
    }
if(builder_id == 8) {
        array_beginPos8 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos8), &error);
        array_sam8 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam8), &error);
        g_object_unref(builder_beginPos8);
        g_object_unref(builder_sam8);

        batch_genomics = create_arrow_record_batch(count, array_beginPos8,array_sam8);

        g_object_unref(array_beginPos8);
        g_object_unref(array_sam8);
    }
    else if(builder_id == 81) {
        array_beginPos8_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos8_1), &error);
	array_sam8_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam8_1), &error);
        g_object_unref(builder_beginPos8_1);
        g_object_unref(builder_sam8_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos8_1,array_sam8_1);

	g_object_unref(array_beginPos8_1);
        g_object_unref(array_sam8_1);   
    }
    else if(builder_id == 82) {
        array_beginPos8_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos8_2), &error);
        array_sam8_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam8_2), &error);
        g_object_unref(builder_beginPos8_2);
        g_object_unref(builder_sam8_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos8_2,array_sam8_2);

        g_object_unref(array_beginPos8_2);
        g_object_unref(array_sam8_2);
    }
if(builder_id == 9) {
        array_beginPos9 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos9), &error);
        array_sam9 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam9), &error);
        g_object_unref(builder_beginPos9);
        g_object_unref(builder_sam9);

        batch_genomics = create_arrow_record_batch(count, array_beginPos9,array_sam9);

        g_object_unref(array_beginPos9);
        g_object_unref(array_sam9);
    }
    else if(builder_id == 91) {
        array_beginPos9_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos9_1), &error);
	array_sam9_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam9_1), &error);
        g_object_unref(builder_beginPos9_1);
        g_object_unref(builder_sam9_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos9_1,array_sam9_1);

	g_object_unref(array_beginPos9_1);
        g_object_unref(array_sam9_1);   
    }
    else if(builder_id == 92) {
        array_beginPos9_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos9_2), &error);
        array_sam9_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam9_2), &error);
        g_object_unref(builder_beginPos9_2);
        g_object_unref(builder_sam9_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos9_2,array_sam9_2);

        g_object_unref(array_beginPos9_2);
        g_object_unref(array_sam9_2);
    }
if(builder_id == 10) {
        array_beginPos10 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos10), &error);
        array_sam10 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam10), &error);
        g_object_unref(builder_beginPos10);
        g_object_unref(builder_sam10);

        batch_genomics = create_arrow_record_batch(count, array_beginPos10,array_sam10);

        g_object_unref(array_beginPos10);
        g_object_unref(array_sam10);
    }
    else if(builder_id == 101) {
        array_beginPos10_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos10_1), &error);
	array_sam10_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam10_1), &error);
        g_object_unref(builder_beginPos10_1);
        g_object_unref(builder_sam10_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos10_1,array_sam10_1);

	g_object_unref(array_beginPos10_1);
        g_object_unref(array_sam10_1);   
    }
    else if(builder_id == 102) {
        array_beginPos10_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos10_2), &error);
        array_sam10_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam10_2), &error);
        g_object_unref(builder_beginPos10_2);
        g_object_unref(builder_sam10_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos10_2,array_sam10_2);

        g_object_unref(array_beginPos10_2);
        g_object_unref(array_sam10_2);
    }
if(builder_id == 11) {
        array_beginPos11 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos11), &error);
        array_sam11 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam11), &error);
        g_object_unref(builder_beginPos11);
        g_object_unref(builder_sam11);

        batch_genomics = create_arrow_record_batch(count, array_beginPos11,array_sam11);

        g_object_unref(array_beginPos11);
        g_object_unref(array_sam11);
    }
    else if(builder_id == 111) {
        array_beginPos11_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos11_1), &error);
	array_sam11_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam11_1), &error);
        g_object_unref(builder_beginPos11_1);
        g_object_unref(builder_sam11_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos11_1,array_sam11_1);

	g_object_unref(array_beginPos11_1);
        g_object_unref(array_sam11_1);   
    }
    else if(builder_id == 112) {
        array_beginPos11_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos11_2), &error);
        array_sam11_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam11_2), &error);
        g_object_unref(builder_beginPos11_2);
        g_object_unref(builder_sam11_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos11_2,array_sam11_2);

        g_object_unref(array_beginPos11_2);
        g_object_unref(array_sam11_2);
    }
if(builder_id == 12) {
        array_beginPos12 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos12), &error);
        array_sam12 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam12), &error);
        g_object_unref(builder_beginPos12);
        g_object_unref(builder_sam12);

        batch_genomics = create_arrow_record_batch(count, array_beginPos12,array_sam12);

        g_object_unref(array_beginPos12);
        g_object_unref(array_sam12);
    }
    else if(builder_id == 121) {
        array_beginPos12_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos12_1), &error);
	array_sam12_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam12_1), &error);
        g_object_unref(builder_beginPos12_1);
        g_object_unref(builder_sam12_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos12_1,array_sam12_1);

	g_object_unref(array_beginPos12_1);
        g_object_unref(array_sam12_1);   
    }
    else if(builder_id == 122) {
        array_beginPos12_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos12_2), &error);
        array_sam12_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam12_2), &error);
        g_object_unref(builder_beginPos12_2);
        g_object_unref(builder_sam12_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPos12_2,array_sam12_2);

        g_object_unref(array_beginPos12_2);
        g_object_unref(array_sam12_2);
    }
if(builder_id == 13) {
        array_beginPos13 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos13), &error);
        array_sam13 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam13), &error);
        g_object_unref(builder_beginPos13);
        g_object_unref(builder_sam13);

        batch_genomics = create_arrow_record_batch(count, array_beginPos13,array_sam13);

        g_object_unref(array_beginPos13);
        g_object_unref(array_sam13);
    }
    else if(builder_id == 131) {
        array_beginPos13_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos13_1), &error);
	array_sam13_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam13_1), &error);
        g_object_unref(builder_beginPos13_1);
        g_object_unref(builder_sam13_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos13_1,array_sam13_1);

	g_object_unref(array_beginPos13_1);
        g_object_unref(array_sam13_1);   
    }

if(builder_id == 14) {
        array_beginPos14 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos14), &error);
        array_sam14 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam14), &error);
        g_object_unref(builder_beginPos14);
        g_object_unref(builder_sam14);

        batch_genomics = create_arrow_record_batch(count, array_beginPos14,array_sam14);

        g_object_unref(array_beginPos14);
        g_object_unref(array_sam14);
    }
    else if(builder_id == 141) {
        array_beginPos14_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos14_1), &error);
	array_sam14_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam14_1), &error);
        g_object_unref(builder_beginPos14_1);
        g_object_unref(builder_sam14_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos14_1,array_sam14_1);

	g_object_unref(array_beginPos14_1);
        g_object_unref(array_sam14_1);   
    }

if(builder_id == 15) {
        array_beginPos15 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos15), &error);
        array_sam15 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam15), &error);
        g_object_unref(builder_beginPos15);
        g_object_unref(builder_sam15);

        batch_genomics = create_arrow_record_batch(count, array_beginPos15,array_sam15);

        g_object_unref(array_beginPos15);
        g_object_unref(array_sam15);
    }
    else if(builder_id == 151) {
        array_beginPos15_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos15_1), &error);
	array_sam15_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam15_1), &error);
        g_object_unref(builder_beginPos15_1);
        g_object_unref(builder_sam15_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos15_1,array_sam15_1);

	g_object_unref(array_beginPos15_1);
        g_object_unref(array_sam15_1);   
    }

if(builder_id == 16) {
        array_beginPos16 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos16), &error);
        array_sam16 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam16), &error);
        g_object_unref(builder_beginPos16);
        g_object_unref(builder_sam16);

        batch_genomics = create_arrow_record_batch(count, array_beginPos16,array_sam16);

        g_object_unref(array_beginPos16);
        g_object_unref(array_sam16);
    }
    else if(builder_id == 161) {
        array_beginPos16_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos16_1), &error);
	array_sam16_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam16_1), &error);
        g_object_unref(builder_beginPos16_1);
        g_object_unref(builder_sam16_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos16_1,array_sam16_1);

	g_object_unref(array_beginPos16_1);
        g_object_unref(array_sam16_1);   
    }
if(builder_id == 17) {
        array_beginPos17 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos17), &error);
        array_sam17 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam17), &error);
        g_object_unref(builder_beginPos17);
        g_object_unref(builder_sam17);

        batch_genomics = create_arrow_record_batch(count, array_beginPos17,array_sam17);

        g_object_unref(array_beginPos17);
        g_object_unref(array_sam17);
    }
    else if(builder_id == 171) {
        array_beginPos17_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos17_1), &error);
	array_sam17_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam17_1), &error);
        g_object_unref(builder_beginPos17_1);
        g_object_unref(builder_sam17_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos17_1,array_sam17_1);

	g_object_unref(array_beginPos17_1);
        g_object_unref(array_sam17_1);   
    }
if(builder_id == 18) {
        array_beginPos18 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos18), &error);
        array_sam18 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam18), &error);
        g_object_unref(builder_beginPos18);
        g_object_unref(builder_sam18);

        batch_genomics = create_arrow_record_batch(count, array_beginPos18,array_sam18);

        g_object_unref(array_beginPos18);
        g_object_unref(array_sam18);
    }
    else if(builder_id == 181) {
        array_beginPos18_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos18_1), &error);
	array_sam18_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam18_1), &error);
        g_object_unref(builder_beginPos18_1);
        g_object_unref(builder_sam18_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPos18_1,array_sam18_1);

	g_object_unref(array_beginPos18_1);
        g_object_unref(array_sam18_1);   
    }

if(builder_id == 19) {
        array_beginPos19 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos19), &error);
        array_sam19 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam19), &error);
        g_object_unref(builder_beginPos19);
        g_object_unref(builder_sam19);

        batch_genomics = create_arrow_record_batch(count, array_beginPos19,array_sam19);

        g_object_unref(array_beginPos19);
        g_object_unref(array_sam19);
    }

if(builder_id == 20) {
        array_beginPos20 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos20), &error);
        array_sam20 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam20), &error);
        g_object_unref(builder_beginPos20);
        g_object_unref(builder_sam20);

        batch_genomics = create_arrow_record_batch(count, array_beginPos20,array_sam20);

        g_object_unref(array_beginPos20);
        g_object_unref(array_sam20);
    }
if(builder_id == 21) {
        array_beginPos21 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos21), &error);
        array_sam21 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam21), &error);
        g_object_unref(builder_beginPos21);
        g_object_unref(builder_sam21);

        batch_genomics = create_arrow_record_batch(count, array_beginPos21,array_sam21);

        g_object_unref(array_beginPos21);
        g_object_unref(array_sam21);
    }
if(builder_id == 22) {
        array_beginPos22 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos22), &error);
        array_sam22 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_sam22), &error);
        g_object_unref(builder_beginPos22);
        g_object_unref(builder_sam22);

        batch_genomics = create_arrow_record_batch(count, array_beginPos22,array_sam22);

        g_object_unref(array_beginPos22);
        g_object_unref(array_sam22);
    }

if(builder_id == 23) {
        array_beginPosX = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPosX), &error);
        array_samX = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_samX), &error);
        g_object_unref(builder_beginPosX);
        g_object_unref(builder_samX);

        batch_genomics = create_arrow_record_batch(count, array_beginPosX,array_samX);

        g_object_unref(array_beginPosX);
        g_object_unref(array_samX);
    }
    else if(builder_id == 231) {
        array_beginPosX_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPosX_1), &error);
	array_samX_1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_samX_1), &error);
        g_object_unref(builder_beginPosX_1);
        g_object_unref(builder_samX_1);

        batch_genomics = create_arrow_record_batch(count, array_beginPosX_1,array_samX_1);

	g_object_unref(array_beginPosX_1);
        g_object_unref(array_samX_1);   
    }
    else if(builder_id == 232) {
        array_beginPosX_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPosX_2), &error);
        array_samX_2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_samX_2), &error);
        g_object_unref(builder_beginPosX_2);
        g_object_unref(builder_samX_2);

        batch_genomics = create_arrow_record_batch(count, array_beginPosX_2,array_samX_2);

        g_object_unref(array_beginPosX_2);
        g_object_unref(array_samX_2);
    }

if(builder_id == 24) {
        array_beginPosY = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPosY), &error);
        array_samY = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_samY), &error);
        g_object_unref(builder_beginPosY);
        g_object_unref(builder_samY);

        batch_genomics = create_arrow_record_batch(count, array_beginPosY,array_samY);

        g_object_unref(array_beginPosY);
        g_object_unref(array_samY);
    }

if(builder_id == 25) {
        array_beginPosM = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPosM), &error);
        array_samM = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_samM), &error);
        g_object_unref(builder_beginPosM);
        g_object_unref(builder_samM);

        batch_genomics = create_arrow_record_batch(count, array_beginPosM,array_samM);

        g_object_unref(array_beginPosM);
        g_object_unref(array_samM);
    }

    return batch_genomics;
}
