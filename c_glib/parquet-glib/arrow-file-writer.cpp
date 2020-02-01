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
 * #GParquetWriterProperties is a class for the writer properties.
 * #GParquetWriterPropertiesBuilder is a class to create a new #GParquetWriterProperties.
 * #GParquetArrowFileWriter is a class for writer Apache Arrow data to
 * file as Apache Parquet format.
 */

typedef struct GParquetWriterPropertiesPrivate_ {
  std::shared_ptr<parquet::WriterProperties> writer_properties;
} GParquetWriterPropertiesPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GParquetWriterProperties,
                           gparquet_writer_properties,
                           G_TYPE_OBJECT)

#define GPARQUET_WRITER_PROPERTIES_GET_PRIVATE(object) \
  static_cast<GParquetWriterPropertiesPrivate *>(      \
    gparquet_writer_properties_get_instance_private(   \
      GPARQUET_WRITER_PROPERTIES(object)))

enum {
  PROP_WRITER_PROPERTIES = 1
};

static void
gparquet_writer_properties_finalize(GObject *object)
{
  auto priv = GPARQUET_WRITER_PROPERTIES_GET_PRIVATE(object);

  priv->writer_properties = nullptr;

  G_OBJECT_CLASS(gparquet_writer_properties_parent_class)->finalize(object);
}

static void
gparquet_writer_properties_set_property(GObject *object,
                                        guint prop_id,
                                        const GValue *value,
                                        GParamSpec *pspec)
{
  auto priv = GPARQUET_WRITER_PROPERTIES_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_WRITER_PROPERTIES:
    priv->writer_properties =
      *static_cast<std::shared_ptr<parquet::WriterProperties> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gparquet_writer_properties_init(GParquetWriterProperties *object)
{
}

static void
gparquet_writer_properties_class_init(GParquetWriterPropertiesClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = gparquet_writer_properties_finalize;
  gobject_class->set_property = gparquet_writer_properties_set_property;

  spec = g_param_spec_pointer("writer-properties",
                              "WriterProperties",
                              "The raw std::shared_ptr<parque::WriterProperties> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_WRITER_PROPERTIES, spec);
}

/**
 * gparquet_writer_properties_new:
 *
 * Return: A newly created #GParquetWriterProperties.
 *
 * Since: 1.0.0
 */
GParquetWriterProperties *
gparquet_writer_properties_new(void)
{
  auto parquet_writer_properties = parquet::default_writer_properties();
  auto writer_properties =
    gparquet_writer_properties_new_raw(&parquet_writer_properties);
  return GPARQUET_WRITER_PROPERTIES(writer_properties);
}


typedef struct GParquetWriterPropertiesBuilderPrivate_ {
  parquet::WriterProperties::Builder *writer_properties_builder;
  GArrowCompressionType compression_type;
} GParquetWriterPropertiesBuilderPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GParquetWriterPropertiesBuilder,
                           gparquet_writer_properties_builder,
                           G_TYPE_OBJECT)

#define GPARQUET_WRITER_PROPERTIES_BUILDER_GET_PRIVATE(object) \
  static_cast<GParquetWriterPropertiesBuilderPrivate *>(       \
    gparquet_writer_properties_builder_get_instance_private(   \
      GPARQUET_WRITER_PROPERTIES_BUILDER(object)))

enum {
  PROP_WRITER_PROPERTIES_BUILDER = 1
};

static void
gparquet_writer_properties_builder_finalize(GObject *object)
{
  auto priv = GPARQUET_WRITER_PROPERTIES_BUILDER_GET_PRIVATE(object);

  delete priv->writer_properties_builder;

  G_OBJECT_CLASS(gparquet_writer_properties_builder_parent_class)->finalize(object);
}

static void
gparquet_writer_properties_builder_set_property(GObject *object,
                                                guint prop_id,
                                                const GValue *value,
                                                GParamSpec *pspec)
{
  auto priv = GPARQUET_WRITER_PROPERTIES_BUILDER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_WRITER_PROPERTIES_BUILDER:
    priv->writer_properties_builder =
      static_cast<parquet::WriterProperties::Builder *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gparquet_writer_properties_builder_init(GParquetWriterPropertiesBuilder *object)
{
}

static void
gparquet_writer_properties_builder_class_init(GParquetWriterPropertiesBuilderClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = gparquet_writer_properties_builder_finalize;
  gobject_class->set_property = gparquet_writer_properties_builder_set_property;

  spec = g_param_spec_pointer("writer-properties-builder",
                              "WriterPropertiesBuilder",
                              "The raw parque::WriterProperties::Builder *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_WRITER_PROPERTIES_BUILDER, spec);
}

/**
 * gparquet_writer_properties_builder_new:
 *
 * Return: A newly created #GParquetWriterPropertiesBuilder.
 *
 * Since: 1.0.0
 */
GParquetWriterPropertiesBuilder *
gparquet_writer_properties_builder_new(void)
{
  auto parquet_writer_properties_builder = new parquet::WriterProperties::Builder();
  return gparquet_writer_properties_builder_new_raw(parquet_writer_properties_builder);
}

/**
 * gparquet_writer_properties_builder_set_compression:
 * @builder: A #GParquetWriterPropertiesBuilder.
 * @compression_type: A #GArrowCompressionType.
 *
 * Since: 1.0.0
 */
void
gparquet_writer_properties_builder_set_compression(GParquetWriterPropertiesBuilder *builder,
                                                   GArrowCompressionType compression_type)
{
  auto parquet_writer_properties_builder = gparquet_writer_properties_builder_get_raw(builder);
  auto arrow_type = garrow_compression_type_to_raw(compression_type);
  parquet_writer_properties_builder->compression(arrow_type);
  auto priv = GPARQUET_WRITER_PROPERTIES_BUILDER_GET_PRIVATE(builder);
  priv->compression_type = compression_type;
}

/**
 * gparquet_writer_properties_builder_get_compression:
 * @builder: A #GParquetWriterPropertiesBuilder.
 *
 * Returns: The compression type of #GParquetWriterPropertiesBuilder.
 *
 * Since: 1.0.0
 */
GArrowCompressionType
gparquet_writer_properties_builder_get_compression(GParquetWriterPropertiesBuilder *builder)
{
  auto priv = GPARQUET_WRITER_PROPERTIES_BUILDER_GET_PRIVATE(builder);
  return priv->compression_type;
}

/**
 * gparquet_writer_properties_builder_build:
 * @builder: A #GParquetWriterPropertiesBuilder.
 *
 * Returns: (transfer full): The built #GParquetWriterProperties.
 *
 * Since: 1.0.0
 */
GParquetWriterProperties *
gparquet_writer_properties_builder_build(GParquetWriterPropertiesBuilder *builder)
{
  auto parquet_writer_properties_builder = gparquet_writer_properties_builder_get_raw(builder);
  auto parquet_writer_properties = parquet_writer_properties_builder->build();
  return gparquet_writer_properties_new_raw(&parquet_writer_properties);
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
  arrow::Status status;
  if (writer_properties) {
    auto parquet_writer_properties = gparquet_writer_properties_get_raw(writer_properties);
    status = parquet::arrow::FileWriter::Open(*arrow_schema,
                                              arrow_memory_pool,
                                              arrow_output_stream,
                                              parquet_writer_properties,
                                              &parquet_arrow_file_writer);
  } else {
    auto parquet_writer_properties = parquet::default_writer_properties();
    status = parquet::arrow::FileWriter::Open(*arrow_schema,
                                              arrow_memory_pool,
                                              arrow_output_stream,
                                              parquet_writer_properties,
                                              &parquet_arrow_file_writer);
  }
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
  arrow::Status status;
  if (writer_properties) {
    auto parquet_writer_properties = gparquet_writer_properties_get_raw(writer_properties);
    status = parquet::arrow::FileWriter::Open(*arrow_schema,
                                              arrow_memory_pool,
                                              arrow_output_stream,
                                              parquet_writer_properties,
                                              &parquet_arrow_file_writer);
  } else {
    auto parquet_writer_properties = parquet::default_writer_properties();
    status = parquet::arrow::FileWriter::Open(*arrow_schema,
                                              arrow_memory_pool,
                                              arrow_output_stream,
                                              parquet_writer_properties,
                                              &parquet_arrow_file_writer);
  }
  if (garrow::check(error,
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

GParquetWriterProperties *
gparquet_writer_properties_new_raw(std::shared_ptr<parquet::WriterProperties> *parquet_writer_properties)
{
  auto writer_properties = g_object_new(GPARQUET_TYPE_WRITER_PROPERTIES,
                                        "writer-properties", parquet_writer_properties,
                                        NULL);
  return GPARQUET_WRITER_PROPERTIES(writer_properties);
}

std::shared_ptr<parquet::WriterProperties>
gparquet_writer_properties_get_raw(GParquetWriterProperties *writer_properties)
{
  auto priv = GPARQUET_WRITER_PROPERTIES_GET_PRIVATE(writer_properties);
  return priv->writer_properties;
}

GParquetWriterPropertiesBuilder *
gparquet_writer_properties_builder_new_raw(parquet::WriterProperties::Builder *parquet_writer_properties_builder)
{
  auto writer_properties_builder = g_object_new(GPARQUET_TYPE_WRITER_PROPERTIES_BUILDER,
                                                "writer-properties-builder", parquet_writer_properties_builder,
                                                NULL);
  return GPARQUET_WRITER_PROPERTIES_BUILDER(writer_properties_builder);
}

parquet::WriterProperties::Builder *
gparquet_writer_properties_builder_get_raw(GParquetWriterPropertiesBuilder *builder)
{
  auto priv = GPARQUET_WRITER_PROPERTIES_BUILDER_GET_PRIVATE(builder);
  return priv->writer_properties_builder;
}
