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

#include <arrow-glib/arrow-glib.hpp>

#include <parquet-glib/arrow-file-writer.hpp>

G_BEGIN_DECLS

/**
 * SECTION: arrow-file-writer
 * @short_description: Arrow file writer class
 * @include: parquet-glib/parquet-glib.h
 *
 * #GParquetArrowFileWriter is a class for writer Apache Arrow data to
 * file as Apache Parquet format.
 */

typedef struct GParquetArrowFileWriterPrivate_ {
  parquet::arrow::FileWriter *arrow_file_writer;
} GParquetArrowFileWriterPrivate;

enum {
  PROP_0,
  PROP_ARROW_FILE_WRITER
};

G_DEFINE_TYPE_WITH_PRIVATE(GParquetArrowFileWriter,
                           gparquet_arrow_file_writer,
                           G_TYPE_OBJECT)

#define GPARQUET_ARROW_FILE_WRITER_GET_PRIVATE(obj)     \
  static_cast<GParquetArrowFileWriterPrivate *>(        \
     gparquet_arrow_file_writer_get_instance_private(   \
       GPARQUET_ARROW_FILE_WRITER(obj)))

static void
gparquet_arrow_file_writer_finalize(GObject *object)
{
  auto priv = GPARQUET_ARROW_FILE_WRITER_GET_PRIVATE(object);

  delete priv->arrow_file_writer;

  G_OBJECT_CLASS(gparquet_arrow_file_writer_parent_class)->finalize(object);
}

static void
gparquet_arrow_file_writer_set_property(GObject *object,
                                        guint prop_id,
                                        const GValue *value,
                                        GParamSpec *pspec)
{
  auto priv = GPARQUET_ARROW_FILE_WRITER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_ARROW_FILE_WRITER:
    priv->arrow_file_writer =
      static_cast<parquet::arrow::FileWriter *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gparquet_arrow_file_writer_get_property(GObject *object,
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
gparquet_arrow_file_writer_init(GParquetArrowFileWriter *object)
{
}

static void
gparquet_arrow_file_writer_class_init(GParquetArrowFileWriterClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = gparquet_arrow_file_writer_finalize;
  gobject_class->set_property = gparquet_arrow_file_writer_set_property;
  gobject_class->get_property = gparquet_arrow_file_writer_get_property;

  spec = g_param_spec_pointer("arrow-file-writer",
                              "ArrowFileWriter",
                              "The raw std::shared<parquet::arrow::FileWriter> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_ARROW_FILE_WRITER, spec);
}

/**
 * gparquet_arrow_file_writer_new_arrow:
 * @schema: Arrow schema for written data.
 * @sink: Arrow output stream to be written.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GParquetArrowFileWriter.
 *
 * Since: 0.11.0
 */
GParquetArrowFileWriter *
gparquet_arrow_file_writer_new_arrow(GArrowSchema *schema,
                                     GArrowOutputStream *sink,
                                     GError **error)
{
  auto arrow_schema = garrow_schema_get_raw(schema).get();
  auto arrow_output_stream = garrow_output_stream_get_raw(sink);
  auto arrow_memory_pool = arrow::default_memory_pool();
  auto parquet_writer_properties = parquet::default_writer_properties();
  std::unique_ptr<parquet::arrow::FileWriter> parquet_arrow_file_writer;
  auto status = parquet::arrow::FileWriter::Open(*arrow_schema,
                                                 arrow_memory_pool,
                                                 arrow_output_stream,
                                                 parquet_writer_properties,
                                                 &parquet_arrow_file_writer);
  if (garrow_error_check(error,
                         status,
                         "[parquet][arrow][file-writer][new-arrow]")) {
    return gparquet_arrow_file_writer_new_raw(parquet_arrow_file_writer.release());
  } else {
    return NULL;
  }
}

/**
 * gparquet_arrow_file_writer_new_path:
 * @schema: Arrow schema for written data.
 * @path: Path to be read.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GParquetArrowFileWriter.
 *
 * Since: 0.11.0
 */
GParquetArrowFileWriter *
gparquet_arrow_file_writer_new_path(GArrowSchema *schema,
                                    const gchar *path,
                                    GError **error)
{
  std::shared_ptr<arrow::io::FileOutputStream> arrow_file_output_stream;
  auto status = arrow::io::FileOutputStream::Open(path,
                                                  false,
                                                  &arrow_file_output_stream);
  if (!garrow_error_check(error,
                          status,
                          "[parquet][arrow][file-writer][new-path]")) {
    return NULL;
  }

  auto arrow_schema = garrow_schema_get_raw(schema).get();
  std::shared_ptr<arrow::io::OutputStream> arrow_output_stream =
    arrow_file_output_stream;
  auto arrow_memory_pool = arrow::default_memory_pool();
  auto parquet_writer_properties = parquet::default_writer_properties();
  std::unique_ptr<parquet::arrow::FileWriter> parquet_arrow_file_writer;
  status = parquet::arrow::FileWriter::Open(*arrow_schema,
                                            arrow_memory_pool,
                                            arrow_output_stream,
                                            parquet_writer_properties,
                                            &parquet_arrow_file_writer);
  if (garrow_error_check(error,
                         status,
                         "[parquet][arrow][file-writer][new-path]")) {
    return gparquet_arrow_file_writer_new_raw(parquet_arrow_file_writer.release());
  } else {
    return NULL;
  }
}

/**
 * gparquet_arrow_file_writer_write_table:
 * @writer: A #GParquetArrowFileWriter.
 * @table: A table to be written.
 * @chunk_size: The max number of rows in a row group.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Since: 0.11.0
 */
gboolean
gparquet_arrow_file_writer_write_table(GParquetArrowFileWriter *writer,
                                       GArrowTable *table,
                                       guint64 chunk_size,
                                       GError **error)
{
  auto parquet_arrow_file_writer = gparquet_arrow_file_writer_get_raw(writer);
  auto arrow_table = garrow_table_get_raw(table).get();
  auto status = parquet_arrow_file_writer->WriteTable(*arrow_table, chunk_size);
  return garrow_error_check(error,
                            status,
                            "[parquet][arrow][file-writer][write-table]");
}

/**
 * gparquet_arrow_file_writer_close:
 * @writer: A #GParquetArrowFileWriter.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Since: 0.11.0
 */
gboolean
gparquet_arrow_file_writer_close(GParquetArrowFileWriter *writer,
                                 GError **error)
{
  auto parquet_arrow_file_writer = gparquet_arrow_file_writer_get_raw(writer);
  auto status = parquet_arrow_file_writer->Close();
  return garrow_error_check(error,
                            status,
                            "[parquet][arrow][file-writer][close]");
}


G_END_DECLS

GParquetArrowFileWriter *
gparquet_arrow_file_writer_new_raw(parquet::arrow::FileWriter *parquet_arrow_file_writer)
{
  auto arrow_file_writer =
    GPARQUET_ARROW_FILE_WRITER(g_object_new(GPARQUET_TYPE_ARROW_FILE_WRITER,
                                            "arrow-file-writer", parquet_arrow_file_writer,
                                            NULL));
  return arrow_file_writer;
}

parquet::arrow::FileWriter *
gparquet_arrow_file_writer_get_raw(GParquetArrowFileWriter *arrow_file_writer)
{
  auto priv = GPARQUET_ARROW_FILE_WRITER_GET_PRIVATE(arrow_file_writer);
  return priv->arrow_file_writer;
}
