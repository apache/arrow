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

#include <arrow-glib/error.hpp>
#include <arrow-glib/input-stream.hpp>
#include <arrow-glib/orc-file-reader.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/schema.hpp>
#include <arrow-glib/table.hpp>

G_BEGIN_DECLS

/**
 * SECTION: orc-file-reader
 * @section_id: orc-file-reader
 * @title: ORC reader
 * @include: arrow-glib/orc-file-reader.h
 *
 * #GArrowORCFileReader is a class for reading stripes in ORC file
 * format from input.
 */

typedef struct GArrowORCFileReaderPrivate_
{
  GArrowSeekableInputStream *input;
  arrow::adapters::orc::ORCFileReader *orc_file_reader;
  GArray *field_indices;
} GArrowORCFileReaderPrivate;

enum {
  PROP_0,
  PROP_INPUT,
  PROP_ORC_FILE_READER
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowORCFileReader, garrow_orc_file_reader, G_TYPE_OBJECT);

#define GARROW_ORC_FILE_READER_GET_PRIVATE(obj)                                          \
  static_cast<GArrowORCFileReaderPrivate *>(                                             \
    garrow_orc_file_reader_get_instance_private(GARROW_ORC_FILE_READER(obj)))

static void
garrow_orc_file_reader_dispose(GObject *object)
{
  auto priv = GARROW_ORC_FILE_READER_GET_PRIVATE(object);

  if (priv->input) {
    g_object_unref(priv->input);
    priv->input = NULL;
  }

  G_OBJECT_CLASS(garrow_orc_file_reader_parent_class)->dispose(object);
}

static void
garrow_orc_file_reader_finalize(GObject *object)
{
  auto priv = GARROW_ORC_FILE_READER_GET_PRIVATE(object);

  delete priv->orc_file_reader;

  if (priv->field_indices) {
    g_array_free(priv->field_indices, TRUE);
  }

  G_OBJECT_CLASS(garrow_orc_file_reader_parent_class)->finalize(object);
}

static void
garrow_orc_file_reader_set_property(GObject *object,
                                    guint prop_id,
                                    const GValue *value,
                                    GParamSpec *pspec)
{
  auto priv = GARROW_ORC_FILE_READER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_INPUT:
    priv->input = GARROW_SEEKABLE_INPUT_STREAM(g_value_dup_object(value));
    break;
  case PROP_ORC_FILE_READER:
    priv->orc_file_reader =
      static_cast<arrow::adapters::orc::ORCFileReader *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_orc_file_reader_get_property(GObject *object,
                                    guint prop_id,
                                    GValue *value,
                                    GParamSpec *pspec)
{
  auto priv = GARROW_ORC_FILE_READER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_INPUT:
    g_value_set_object(value, priv->input);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_orc_file_reader_init(GArrowORCFileReader *object)
{
}

static void
garrow_orc_file_reader_class_init(GArrowORCFileReaderClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = garrow_orc_file_reader_dispose;
  gobject_class->finalize = garrow_orc_file_reader_finalize;
  gobject_class->set_property = garrow_orc_file_reader_set_property;
  gobject_class->get_property = garrow_orc_file_reader_get_property;

  GParamSpec *spec;
  spec = g_param_spec_object(
    "input",
    "Input",
    "The input stream",
    GARROW_TYPE_SEEKABLE_INPUT_STREAM,
    static_cast<GParamFlags>(G_PARAM_READWRITE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_INPUT, spec);

  spec = g_param_spec_pointer(
    "orc-file-reader",
    "arrow::adapters::orc::ORCFileReader",
    "The raw arrow::adapters::orc::ORCFileReader *",
    static_cast<GParamFlags>(G_PARAM_WRITABLE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_ORC_FILE_READER, spec);
}

/**
 * garrow_orc_file_reader_new:
 * @file: The file to be read.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowORCFileReader
 *   or %NULL on error.
 *
 * Since: 0.10.0
 */
GArrowORCFileReader *
garrow_orc_file_reader_new(GArrowSeekableInputStream *input, GError **error)
{
  auto arrow_random_access_file = garrow_seekable_input_stream_get_raw(input);
  auto pool = arrow::default_memory_pool();
  auto arrow_reader_result =
    arrow::adapters::orc::ORCFileReader::Open(arrow_random_access_file, pool);
  if (garrow::check(error, arrow_reader_result, "[orc-file-reader][new]")) {
    return garrow_orc_file_reader_new_raw(input, (*arrow_reader_result).release());
  } else {
    return NULL;
  }
}

/**
 * garrow_orc_file_reader_set_field_indexes:
 * @reader: A #GArrowORCFileReader.
 * @field_indexes: (nullable) (array length=n_field_indexes):
 *   The field indexes to be read.
 * @n_field_indexes: The number of the specified indexes.
 *
 * Since: 0.10.0
 *
 * Deprecated: 0.12.0:
 *   Use garrow_orc_file_reader_set_field_indices() instead.
 */
void
garrow_orc_file_reader_set_field_indexes(GArrowORCFileReader *reader,
                                         const gint *field_indexes,
                                         guint n_field_indexes)
{
  garrow_orc_file_reader_set_field_indices(reader, field_indexes, n_field_indexes);
}

/**
 * garrow_orc_file_reader_set_field_indices:
 * @reader: A #GArrowORCFileReader.
 * @field_indices: (nullable) (array length=n_field_indices):
 *   The field indices to be read.
 * @n_field_indices: The number of the specified indices.
 *
 * Since: 0.12.0
 */
void
garrow_orc_file_reader_set_field_indices(GArrowORCFileReader *reader,
                                         const gint *field_indices,
                                         guint n_field_indices)
{
  auto priv = GARROW_ORC_FILE_READER_GET_PRIVATE(reader);
  if (priv->field_indices) {
    g_array_free(priv->field_indices, TRUE);
  }
  if (n_field_indices == 0) {
    priv->field_indices = NULL;
  } else {
    priv->field_indices = g_array_sized_new(FALSE, FALSE, sizeof(gint), n_field_indices);
    g_array_append_vals(priv->field_indices, field_indices, n_field_indices);
  }
}

/**
 * garrow_orc_file_reader_get_field_indexes:
 * @reader: A #GArrowORCFileReader.
 * @n_field_indexes: The number of the specified indexes.
 *
 * Returns: (nullable) (array length=n_field_indexes) (transfer none):
 *  The field indexes to be read.
 *
 * Since: 0.10.0
 *
 * Deprecated: 0.12.0:
 *   Use garrow_orc_file_reader_get_field_indices() instead.
 */
const gint *
garrow_orc_file_reader_get_field_indexes(GArrowORCFileReader *reader,
                                         guint *n_field_indexes)
{
  return garrow_orc_file_reader_get_field_indices(reader, n_field_indexes);
}

/**
 * garrow_orc_file_reader_get_field_indices:
 * @reader: A #GArrowORCFileReader.
 * @n_field_indices: The number of the specified indices.
 *
 * Returns: (nullable) (array length=n_field_indices) (transfer none):
 *  The field indices to be read.
 *
 * Since: 0.12.0
 */
const gint *
garrow_orc_file_reader_get_field_indices(GArrowORCFileReader *reader,
                                         guint *n_field_indices)
{
  auto priv = GARROW_ORC_FILE_READER_GET_PRIVATE(reader);
  if (priv->field_indices) {
    *n_field_indices = priv->field_indices->len;
    return reinterpret_cast<gint *>(priv->field_indices->data);
  } else {
    *n_field_indices = 0;
    return NULL;
  }
}

/**
 * garrow_orc_file_reader_read_type:
 * @reader: A #GArrowORCFileReader.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): A newly read type as
 *   #GArrowSchema or %NULL on error.
 *
 * Since: 0.10.0
 */
GArrowSchema *
garrow_orc_file_reader_read_type(GArrowORCFileReader *reader, GError **error)
{
  auto arrow_reader = garrow_orc_file_reader_get_raw(reader);
  auto arrow_schema_result = arrow_reader->ReadSchema();
  if (garrow::check(error, arrow_schema_result, "[orc-file-reader][read-type]")) {
    auto arrow_schema = *arrow_schema_result;
    return garrow_schema_new_raw(&arrow_schema);
  } else {
    return NULL;
  }
}

/**
 * garrow_orc_file_reader_read_stripes:
 * @reader: A #GArrowORCFileReader.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): A newly read stripes as
 *   #GArrowTable or %NULL on error.
 *
 * Since: 0.10.0
 */
GArrowTable *
garrow_orc_file_reader_read_stripes(GArrowORCFileReader *reader, GError **error)
{
  auto arrow_reader = garrow_orc_file_reader_get_raw(reader);
  auto priv = GARROW_ORC_FILE_READER_GET_PRIVATE(reader);
  if (priv->field_indices) {
    std::vector<int> arrow_field_indices;
    auto field_indices = priv->field_indices;
    for (guint i = 0; i < field_indices->len; ++i) {
      arrow_field_indices.push_back(g_array_index(field_indices, gint, i));
    }
    auto arrow_table_result = arrow_reader->Read(arrow_field_indices);
    if (garrow::check(error, arrow_table_result, "[orc-file-reader][read-stripes]")) {
      auto arrow_table = *arrow_table_result;
      return garrow_table_new_raw(&arrow_table);
    } else {
      return NULL;
    }
  } else {
    auto arrow_table_result = arrow_reader->Read();
    if (garrow::check(error, arrow_table_result, "[orc-file-reader][read-stripes]")) {
      auto arrow_table = *arrow_table_result;
      return garrow_table_new_raw(&arrow_table);
    } else {
      return NULL;
    }
  }
}

/**
 * garrow_orc_file_reader_read_stripe:
 * @reader: A #GArrowORCFileReader.
 * @i: The stripe index to be read.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): A newly read stripe as
 *   #GArrowRecordBatch or %NULL on error.
 *
 * Since: 0.10.0
 */
GArrowRecordBatch *
garrow_orc_file_reader_read_stripe(GArrowORCFileReader *reader, gint64 i, GError **error)
{
  auto arrow_reader = garrow_orc_file_reader_get_raw(reader);
  if (i < 0) {
    i += arrow_reader->NumberOfStripes();
  }
  auto priv = GARROW_ORC_FILE_READER_GET_PRIVATE(reader);
  if (priv->field_indices) {
    std::vector<int> arrow_field_indices;
    auto field_indices = priv->field_indices;
    for (guint j = 0; j < field_indices->len; ++j) {
      arrow_field_indices.push_back(g_array_index(field_indices, gint, j));
    }
    std::shared_ptr<arrow::RecordBatch> arrow_record_batch;
    auto arrow_record_batch_result = arrow_reader->ReadStripe(i, arrow_field_indices);
    if (garrow::check(error,
                      arrow_record_batch_result,
                      "[orc-file-reader][read-stripe]")) {
      auto arrow_record_batch = *arrow_record_batch_result;
      return garrow_record_batch_new_raw(&arrow_record_batch);
    } else {
      return NULL;
    }
  } else {
    auto arrow_record_batch_result = arrow_reader->ReadStripe(i);
    if (garrow::check(error,
                      arrow_record_batch_result,
                      "[orc-file-reader][read-stripe]")) {
      auto arrow_record_batch = *arrow_record_batch_result;
      return garrow_record_batch_new_raw(&arrow_record_batch);
    } else {
      return NULL;
    }
  }
}

/**
 * garrow_orc_file_reader_get_n_stripes:
 * @reader: A #GArrowORCFileReader.
 *
 * Returns: The number of stripes in the file.
 *
 * Since: 0.10.0
 */
gint64
garrow_orc_file_reader_get_n_stripes(GArrowORCFileReader *reader)
{
  auto arrow_reader = garrow_orc_file_reader_get_raw(reader);
  return arrow_reader->NumberOfStripes();
}

/**
 * garrow_orc_file_reader_get_n_rows:
 * @reader: A #GArrowORCFileReader.
 *
 * Returns: The number of rows in the file.
 *
 * Since: 0.10.0
 */
gint64
garrow_orc_file_reader_get_n_rows(GArrowORCFileReader *reader)
{
  auto arrow_reader = garrow_orc_file_reader_get_raw(reader);
  return arrow_reader->NumberOfRows();
}

G_END_DECLS

GArrowORCFileReader *
garrow_orc_file_reader_new_raw(GArrowSeekableInputStream *input,
                               arrow::adapters::orc::ORCFileReader *arrow_reader)
{
  auto reader = GARROW_ORC_FILE_READER(g_object_new(GARROW_TYPE_ORC_FILE_READER,
                                                    "input",
                                                    input,
                                                    "orc-file-reader",
                                                    arrow_reader,
                                                    NULL));
  return reader;
}

arrow::adapters::orc::ORCFileReader *
garrow_orc_file_reader_get_raw(GArrowORCFileReader *reader)
{
  auto priv = GARROW_ORC_FILE_READER_GET_PRIVATE(reader);
  return priv->orc_file_reader;
}
