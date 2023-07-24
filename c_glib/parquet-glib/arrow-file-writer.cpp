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

#include <arrow-glib/arrow-glib.hpp>

#include <parquet-glib/arrow-file-writer.hpp>

G_BEGIN_DECLS

/**
 * SECTION: arrow-file-writer
 * @short_description: Arrow file writer class
 * @include: parquet-glib/parquet-glib.h
 *
 * #GParquetWriterProperties is a class for the writer properties.
 *
 * #GParquetArrowFileWriter is a class for writer Apache Arrow data to
 * file as Apache Parquet format.
 */

typedef struct GParquetWriterPropertiesPrivate_ {
  std::shared_ptr<parquet::WriterProperties> properties;
  parquet::WriterProperties::Builder *builder;
  gboolean changed;
} GParquetWriterPropertiesPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GParquetWriterProperties,
                           gparquet_writer_properties,
                           G_TYPE_OBJECT)

#define GPARQUET_WRITER_PROPERTIES_GET_PRIVATE(object) \
  static_cast<GParquetWriterPropertiesPrivate *>(      \
    gparquet_writer_properties_get_instance_private(   \
      GPARQUET_WRITER_PROPERTIES(object)))

static void
gparquet_writer_properties_finalize(GObject *object)
{
  auto priv = GPARQUET_WRITER_PROPERTIES_GET_PRIVATE(object);

  priv->properties.~shared_ptr();
  delete priv->builder;

  G_OBJECT_CLASS(gparquet_writer_properties_parent_class)->finalize(object);
}

static void
gparquet_writer_properties_init(GParquetWriterProperties *object)
{
  auto priv = GPARQUET_WRITER_PROPERTIES_GET_PRIVATE(object);
  new(&priv->properties) std::shared_ptr<parquet::WriterProperties>;
  priv->builder = new parquet::WriterProperties::Builder();
  priv->changed = TRUE;
}

static void
gparquet_writer_properties_class_init(GParquetWriterPropertiesClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = gparquet_writer_properties_finalize;
}

/**
 * gparquet_writer_properties_new:
 *
 * Return: A newly created #GParquetWriterProperties.
 *
 * Since: 0.17.0
 */
GParquetWriterProperties *
gparquet_writer_properties_new(void)
{
  auto writer_properties = g_object_new(GPARQUET_TYPE_WRITER_PROPERTIES,
                                        NULL);
  return GPARQUET_WRITER_PROPERTIES(writer_properties);
}

/**
 * gparquet_writer_properties_set_compression:
 * @properties: A #GParquetWriterProperties.
 * @compression_type: A #GArrowCompressionType.
 * @path: (nullable): The column path as dot string.
 *
 * Since: 0.17.0
 */
void
gparquet_writer_properties_set_compression(GParquetWriterProperties *properties,
                                           GArrowCompressionType compression_type,
                                           const gchar *path)
{
  auto arrow_compression_type = garrow_compression_type_to_raw(compression_type);
  auto priv = GPARQUET_WRITER_PROPERTIES_GET_PRIVATE(properties);
  if (path) {
    priv->builder->compression(path, arrow_compression_type);
  } else {
    priv->builder->compression(arrow_compression_type);
  }
  priv->changed = TRUE;
}

/**
 * gparquet_writer_properties_get_compression_path:
 * @properties: A #GParquetWriterProperties.
 * @path: The path as dot string.
 *
 * Returns: The compression type of #GParquetWriterProperties.
 *
 * Since: 0.17.0
 */
GArrowCompressionType
gparquet_writer_properties_get_compression_path(GParquetWriterProperties *properties,
                                                const gchar *path)
{
  auto parquet_properties = gparquet_writer_properties_get_raw(properties);
  auto parquet_column_path = parquet::schema::ColumnPath::FromDotString(path);
  auto arrow_compression = parquet_properties->compression(parquet_column_path);
  return garrow_compression_type_from_raw(arrow_compression);
}

/**
 * gparquet_writer_properties_enable_dictionary:
 * @properties: A #GParquetWriterProperties.
 * @path: (nullable): The column path as dot string.
 *
 * Since: 0.17.0
 */
void
gparquet_writer_properties_enable_dictionary(GParquetWriterProperties *properties,
                                             const gchar *path)
{
  auto priv = GPARQUET_WRITER_PROPERTIES_GET_PRIVATE(properties);
  if (path) {
    priv->builder->enable_dictionary(path);
  } else {
    priv->builder->enable_dictionary();
  }
  priv->changed = TRUE;
}

/**
 * gparquet_writer_properties_disable_dictionary:
 * @properties: A #GParquetWriterProperties.
 * @path: (nullable): The column path as dot string.
 *
 * Since: 0.17.0
 */
void
gparquet_writer_properties_disable_dictionary(GParquetWriterProperties *properties,
                                              const gchar *path)
{
  auto priv = GPARQUET_WRITER_PROPERTIES_GET_PRIVATE(properties);
  if (path) {
    priv->builder->disable_dictionary(path);
  } else {
    priv->builder->disable_dictionary();
  }
  priv->changed = TRUE;
}

/**
 * gparquet_writer_properties_is_dictionary_enabled:
 * @properties: A #GParquetWriterProperties.
 * @path: The path as dot string.
 *
 * Returns: %TRUE on dictionary enabled, %FALSE on dictionary disabled.
 *
 * Since: 0.17.0
 */
gboolean
gparquet_writer_properties_is_dictionary_enabled(GParquetWriterProperties *properties,
                                                 const gchar *path)
{
  auto parquet_properties = gparquet_writer_properties_get_raw(properties);
  auto parquet_column_path = parquet::schema::ColumnPath::FromDotString(path);
  return parquet_properties->dictionary_enabled(parquet_column_path);
}

/**
 * gparquet_writer_properties_set_dictionary_page_size_limit:
 * @properties: A #GParquetWriterProperties.
 * @limit: The dictionary page size limit.
 *
 * Since: 0.17.0
 */
void
gparquet_writer_properties_set_dictionary_page_size_limit(GParquetWriterProperties *properties,
                                                          gint64 limit)
{
  auto priv = GPARQUET_WRITER_PROPERTIES_GET_PRIVATE(properties);
  priv->builder->dictionary_pagesize_limit(limit);
  priv->changed = TRUE;
}

/**
 * gparquet_writer_properties_get_dictionary_page_size_limit:
 * @properties: A #GParquetWriterProperties.
 *
 * Returns: The dictionary page size limit.
 *
 * Since: 0.17.0
 */
gint64
gparquet_writer_properties_get_dictionary_page_size_limit(GParquetWriterProperties *properties)
{
  auto parquet_properties = gparquet_writer_properties_get_raw(properties);
  return parquet_properties->dictionary_pagesize_limit();
}

/**
 * gparquet_writer_properties_set_batch_size:
 * @properties: A #GParquetWriterProperties.
 * @batch_size: The batch size.
 *
 * Since: 0.17.0
 */
void
gparquet_writer_properties_set_batch_size(GParquetWriterProperties *properties,
                                          gint64 batch_size)
{
  auto priv = GPARQUET_WRITER_PROPERTIES_GET_PRIVATE(properties);
  priv->builder->write_batch_size(batch_size);
  priv->changed = TRUE;
}

/**
 * gparquet_writer_properties_get_batch_size:
 * @properties: A #GParquetWriterProperties.
 *
 * Returns: The batch size.
 *
 * Since: 0.17.0
 */
gint64
gparquet_writer_properties_get_batch_size(GParquetWriterProperties *properties)
{
  auto parquet_properties = gparquet_writer_properties_get_raw(properties);
  return parquet_properties->write_batch_size();
}

/**
 * gparquet_writer_properties_set_max_row_group_length:
 * @properties: A #GParquetWriterProperties.
 * @length: The max row group length.
 *
 * Since: 0.17.0
 */
void
gparquet_writer_properties_set_max_row_group_length(GParquetWriterProperties *properties,
                                                    gint64 length)
{
  auto priv = GPARQUET_WRITER_PROPERTIES_GET_PRIVATE(properties);
  priv->builder->max_row_group_length(length);
  priv->changed = TRUE;
}

/**
 * gparquet_writer_properties_get_max_row_group_length:
 * @properties: A #GParquetWriterProperties.
 *
 * Returns: The max row group length.
 *
 * Since: 0.17.0
 */
gint64
gparquet_writer_properties_get_max_row_group_length(GParquetWriterProperties *properties)
{
  auto parquet_properties = gparquet_writer_properties_get_raw(properties);
  return parquet_properties->max_row_group_length();
}

/**
 * gparquet_writer_properties_set_data_page_size:
 * @properties: A #GParquetWriterProperties.
 * @data_page_size: The data page size.
 *
 * Since: 0.17.0
 */
void
gparquet_writer_properties_set_data_page_size(GParquetWriterProperties *properties,
                                              gint64 data_page_size)
{
  auto priv = GPARQUET_WRITER_PROPERTIES_GET_PRIVATE(properties);
  priv->builder->data_pagesize(data_page_size);
  priv->changed = TRUE;
}

/**
 * gparquet_writer_properties_get_data_page_size:
 * @properties: A #GParquetWriterProperties.
 *
 * Returns: The data page size.
 *
 * Since: 0.17.0
 */
gint64
gparquet_writer_properties_get_data_page_size(GParquetWriterProperties *properties)
{
  auto parquet_properties = gparquet_writer_properties_get_raw(properties);
  return parquet_properties->data_pagesize();
}


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
 * @writer_properties: (nullable): A #GParquetWriterProperties.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GParquetArrowFileWriter.
 *
 * Since: 0.11.0
 */
GParquetArrowFileWriter *
gparquet_arrow_file_writer_new_arrow(GArrowSchema *schema,
                                     GArrowOutputStream *sink,
                                     GParquetWriterProperties *writer_properties,
                                     GError **error)
{
  auto arrow_schema = garrow_schema_get_raw(schema).get();
  auto arrow_output_stream = garrow_output_stream_get_raw(sink);
  auto arrow_memory_pool = arrow::default_memory_pool();
  std::unique_ptr<parquet::arrow::FileWriter> parquet_arrow_file_writer;
  arrow::Result<std::unique_ptr<parquet::arrow::FileWriter>> maybe_writer;
  if (writer_properties) {
    auto parquet_writer_properties = gparquet_writer_properties_get_raw(writer_properties);
    maybe_writer = parquet::arrow::FileWriter::Open(*arrow_schema,
                                                    arrow_memory_pool,
                                                    arrow_output_stream,
                                                    parquet_writer_properties);
  } else {
    auto parquet_writer_properties = parquet::default_writer_properties();
    maybe_writer = parquet::arrow::FileWriter::Open(*arrow_schema,
                                                    arrow_memory_pool,
                                                    arrow_output_stream,
                                                    parquet_writer_properties);
  }
  if (garrow::check(error,
                    maybe_writer,
                    "[parquet][arrow][file-writer][new-arrow]")) {
    parquet_arrow_file_writer = std::move(*maybe_writer);
    return gparquet_arrow_file_writer_new_raw(parquet_arrow_file_writer.release());
  } else {
    return NULL;
  }
}

/**
 * gparquet_arrow_file_writer_new_path:
 * @schema: Arrow schema for written data.
 * @path: Path to be read.
 * @writer_properties: (nullable): A #GParquetWriterProperties.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GParquetArrowFileWriter.
 *
 * Since: 0.11.0
 */
GParquetArrowFileWriter *
gparquet_arrow_file_writer_new_path(GArrowSchema *schema,
                                    const gchar *path,
                                    GParquetWriterProperties *writer_properties,
                                    GError **error)
{
  auto arrow_file_output_stream =
    arrow::io::FileOutputStream::Open(path, false);
  if (!garrow::check(error,
                     arrow_file_output_stream,
                     "[parquet][arrow][file-writer][new-path]")) {
    return NULL;
  }

  auto arrow_schema = garrow_schema_get_raw(schema).get();
  std::shared_ptr<arrow::io::OutputStream> arrow_output_stream =
    arrow_file_output_stream.ValueOrDie();
  auto arrow_memory_pool = arrow::default_memory_pool();
  std::unique_ptr<parquet::arrow::FileWriter> parquet_arrow_file_writer;
  arrow::Result<std::unique_ptr<parquet::arrow::FileWriter>> maybe_writer;
  if (writer_properties) {
    auto parquet_writer_properties = gparquet_writer_properties_get_raw(writer_properties);
    maybe_writer = parquet::arrow::FileWriter::Open(*arrow_schema,
                                                    arrow_memory_pool,
                                                    arrow_output_stream,
                                                    parquet_writer_properties);
  } else {
    auto parquet_writer_properties = parquet::default_writer_properties();
    maybe_writer = parquet::arrow::FileWriter::Open(*arrow_schema,
                                                    arrow_memory_pool,
                                                    arrow_output_stream,
                                                    parquet_writer_properties);
  }
  if (garrow::check(error,
                    maybe_writer,
                    "[parquet][arrow][file-writer][new-path]")) {
    parquet_arrow_file_writer = std::move(*maybe_writer);
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

std::shared_ptr<parquet::WriterProperties>
gparquet_writer_properties_get_raw(GParquetWriterProperties *properties)
{
  auto priv = GPARQUET_WRITER_PROPERTIES_GET_PRIVATE(properties);
  if (priv->changed) {
    priv->properties = priv->builder->build();
    priv->changed = FALSE;
  }
  return priv->properties;
}
