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
#include <arrow-glib/file-system.hpp>
#include <arrow-glib/output-stream.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/reader.hpp>
#include <arrow-glib/schema.hpp>

#include <arrow-dataset-glib/file-format.hpp>

G_BEGIN_DECLS

/**
 * SECTION: file-format
 * @section_id: file-format
 * @title: File format classes
 * @include: arrow-dataset-glib/arrow-dataset-glib.h
 *
 * #GADatasetFileWriteOptions is a class for options to write a file
 * of this format.
 *
 * #GADatasetFileWriter is a class for writing a file of this format.
 *
 * #GADatasetFileFormat is a base class for file format classes.
 *
 * #GADatasetCSVFileFormat is a class for CSV file format.
 *
 * #GADatasetIPCFileFormat is a class for IPC file format.
 *
 * #GADatasetParquetFileFormat is a class for Parquet file format.
 *
 * Since: 3.0.0
 */

typedef struct GADatasetFileWriteOptionsPrivate_ {
  std::shared_ptr<arrow::dataset::FileWriteOptions> options;
} GADatasetFileWriteOptionsPrivate;

enum {
  PROP_OPTIONS = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GADatasetFileWriteOptions,
                           gadataset_file_write_options,
                           G_TYPE_OBJECT)

#define GADATASET_FILE_WRITE_OPTIONS_GET_PRIVATE(obj)       \
  static_cast<GADatasetFileWriteOptionsPrivate *>(          \
    gadataset_file_write_options_get_instance_private(      \
      GADATASET_FILE_WRITE_OPTIONS(obj)))

static void
gadataset_file_write_options_finalize(GObject *object)
{
  auto priv = GADATASET_FILE_WRITE_OPTIONS_GET_PRIVATE(object);
  priv->options.~shared_ptr();
  G_OBJECT_CLASS(gadataset_file_write_options_parent_class)->finalize(object);
}

static void
gadataset_file_write_options_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  auto priv = GADATASET_FILE_WRITE_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_OPTIONS:
    priv->options =
      *static_cast<std::shared_ptr<arrow::dataset::FileWriteOptions> *>(
        g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gadataset_file_write_options_init(GADatasetFileWriteOptions *object)
{
  auto priv = GADATASET_FILE_WRITE_OPTIONS_GET_PRIVATE(object);
  new(&priv->options) std::shared_ptr<arrow::dataset::FileWriteOptions>;
}

static void
gadataset_file_write_options_class_init(GADatasetFileWriteOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = gadataset_file_write_options_finalize;
  gobject_class->set_property = gadataset_file_write_options_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("options",
                              "Options",
                              "The raw "
                              "std::shared<arrow::dataset::FileWriteOptions> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_OPTIONS, spec);
}


typedef struct GADatasetFileWriterPrivate_ {
  std::shared_ptr<arrow::dataset::FileWriter> writer;
} GADatasetFileWriterPrivate;

enum {
  PROP_WRITER = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GADatasetFileWriter,
                           gadataset_file_writer,
                           G_TYPE_OBJECT)

#define GADATASET_FILE_WRITER_GET_PRIVATE(obj)              \
  static_cast<GADatasetFileWriterPrivate *>(                \
    gadataset_file_writer_get_instance_private(             \
      GADATASET_FILE_WRITER(obj)))

static void
gadataset_file_writer_finalize(GObject *object)
{
  auto priv = GADATASET_FILE_WRITER_GET_PRIVATE(object);
  priv->writer.~shared_ptr();
  G_OBJECT_CLASS(gadataset_file_writer_parent_class)->finalize(object);
}

static void
gadataset_file_writer_set_property(GObject *object,
                                   guint prop_id,
                                   const GValue *value,
                                   GParamSpec *pspec)
{
  auto priv = GADATASET_FILE_WRITER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_WRITER:
    priv->writer =
      *static_cast<std::shared_ptr<arrow::dataset::FileWriter> *>(
        g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gadataset_file_writer_init(GADatasetFileWriter *object)
{
  auto priv = GADATASET_FILE_WRITER_GET_PRIVATE(object);
  new(&(priv->writer)) std::shared_ptr<arrow::dataset::FileWriter>;
}

static void
gadataset_file_writer_class_init(GADatasetFileWriterClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = gadataset_file_writer_finalize;
  gobject_class->set_property = gadataset_file_writer_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("writer",
                              "Writer",
                              "The raw "
                              "std::shared<arrow::dataset::FileWriter> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_WRITER, spec);
}

/**
 * gadataset_file_writer_write_record_batch:
 * @writer: A #GADatasetFileWriter.
 * @record_batch: A #GArrowRecordBatch to be written.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 6.0.0
 */
gboolean
gadataset_file_writer_write_record_batch(GADatasetFileWriter *writer,
                                         GArrowRecordBatch *record_batch,
                                         GError **error)
{
  const auto arrow_writer = gadataset_file_writer_get_raw(writer);
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  auto status = arrow_writer->Write(arrow_record_batch);
  return garrow::check(error, status, "[file-writer][write-record-batch]");
}

/**
 * gadataset_file_writer_write_record_batch_reader:
 * @writer: A #GADatasetFileWriter.
 * @reader: A #GArrowRecordBatchReader to be written.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 6.0.0
 */
gboolean
gadataset_file_writer_write_record_batch_reader(GADatasetFileWriter *writer,
                                                GArrowRecordBatchReader *reader,
                                                GError **error)
{
  const auto arrow_writer = gadataset_file_writer_get_raw(writer);
  auto arrow_reader = garrow_record_batch_reader_get_raw(reader);
  auto status = arrow_writer->Write(arrow_reader.get());
  return garrow::check(error,
                       status,
                       "[file-writer][write-record-batch-reader]");
}

/**
 * gadataset_file_writer_finish:
 * @writer: A #GADatasetFileWriter.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 6.0.0
 */
gboolean
gadataset_file_writer_finish(GADatasetFileWriter *writer,
                             GError **error)
{
  const auto arrow_writer = gadataset_file_writer_get_raw(writer);
  auto status = arrow_writer->Finish().status();
  return garrow::check(error,
                       status,
                       "[file-writer][finish]");
}


typedef struct GADatasetFileFormatPrivate_ {
  std::shared_ptr<arrow::dataset::FileFormat> format;
} GADatasetFileFormatPrivate;

enum {
  PROP_FORMAT = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GADatasetFileFormat,
                           gadataset_file_format,
                           G_TYPE_OBJECT)

#define GADATASET_FILE_FORMAT_GET_PRIVATE(obj)        \
  static_cast<GADatasetFileFormatPrivate *>(          \
    gadataset_file_format_get_instance_private(       \
      GADATASET_FILE_FORMAT(obj)))

static void
gadataset_file_format_finalize(GObject *object)
{
  auto priv = GADATASET_FILE_FORMAT_GET_PRIVATE(object);
  priv->format.~shared_ptr();
  G_OBJECT_CLASS(gadataset_file_format_parent_class)->finalize(object);
}

static void
gadataset_file_format_set_property(GObject *object,
                                   guint prop_id,
                                   const GValue *value,
                                   GParamSpec *pspec)
{
  auto priv = GADATASET_FILE_FORMAT_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FORMAT:
    priv->format =
      *static_cast<std::shared_ptr<arrow::dataset::FileFormat> *>(
        g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gadataset_file_format_init(GADatasetFileFormat *object)
{
  auto priv = GADATASET_FILE_FORMAT_GET_PRIVATE(object);
  new(&priv->format) std::shared_ptr<arrow::dataset::FileFormat>;
}

static void
gadataset_file_format_class_init(GADatasetFileFormatClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = gadataset_file_format_finalize;
  gobject_class->set_property = gadataset_file_format_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("format",
                              "Format",
                              "The raw std::shared<arrow::dataset::FileFormat> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FORMAT, spec);
}

/**
 * gadataset_file_format_get_type_name:
 * @format: A #GADatasetFileFormat.
 *
 * Returns: The type name of @format.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 3.0.0
 */
gchar *
gadataset_file_format_get_type_name(GADatasetFileFormat *format)
{
  const auto arrow_format = gadataset_file_format_get_raw(format);
  const auto &type_name = arrow_format->type_name();
  return g_strndup(type_name.data(), type_name.size());
}

/**
 * gadataset_file_format_get_default_write_options:
 * @format: A #GADatasetFileFormat.
 *
 * Returns: (transfer full): The default #GADatasetFileWriteOptions of @format.
 *
 * Since: 6.0.0
 */
GADatasetFileWriteOptions *
gadataset_file_format_get_default_write_options(GADatasetFileFormat *format)
{
  const auto arrow_format = gadataset_file_format_get_raw(format);
  auto arrow_options = arrow_format->DefaultWriteOptions();
  return gadataset_file_write_options_new_raw(&arrow_options);
}

/**
 * gadataset_file_format_open_writer:
 * @format: A #GADatasetFileFormat.
 * @destination: A #GArrowOutputStream.
 * @file_system: The #GArrowFileSystem of @destination.
 * @path: The path of @destination.
 * @schema: A #GArrowSchema that is used by written record batches.
 * @options: A #GADatasetFileWriteOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): The newly created #GADatasetFileWriter of @format
 *   on success, %NULL on error.
 *
 * Since: 6.0.0
 */
GADatasetFileWriter *
gadataset_file_format_open_writer(GADatasetFileFormat *format,
                                  GArrowOutputStream *destination,
                                  GArrowFileSystem *file_system,
                                  const gchar *path,
                                  GArrowSchema *schema,
                                  GADatasetFileWriteOptions *options,
                                  GError **error)
{
  const auto arrow_format = gadataset_file_format_get_raw(format);
  auto arrow_destination = garrow_output_stream_get_raw(destination);
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto arrow_schema = garrow_schema_get_raw(schema);
  auto arrow_options = gadataset_file_write_options_get_raw(options);
  auto arrow_writer_result =
    arrow_format->MakeWriter(arrow_destination,
                             arrow_schema,
                             arrow_options,
                             {arrow_file_system, path});
  if (garrow::check(error, arrow_writer_result, "[file-format][open-writer]")) {
    auto arrow_writer = *arrow_writer_result;
    return gadataset_file_writer_new_raw(&arrow_writer);
  } else {
    return NULL;
  }
}

/**
 * gadataset_file_format_equal:
 * @format: A #GADatasetFileFormat.
 * @other_format: A #GADatasetFileFormat to be compared.
 *
 * Returns: %TRUE if they are the same content file format, %FALSE otherwise.
 *
 * Since: 3.0.0
 */
gboolean
gadataset_file_format_equal(GADatasetFileFormat *format,
                            GADatasetFileFormat *other_format)
{
  const auto arrow_format = gadataset_file_format_get_raw(format);
  const auto arrow_other_format = gadataset_file_format_get_raw(other_format);
  return arrow_format->Equals(*arrow_other_format);
}


G_DEFINE_TYPE(GADatasetCSVFileFormat,
              gadataset_csv_file_format,
              GADATASET_TYPE_FILE_FORMAT)

static void
gadataset_csv_file_format_init(GADatasetCSVFileFormat *object)
{
}

static void
gadataset_csv_file_format_class_init(GADatasetCSVFileFormatClass *klass)
{
}

/**
 * gadataset_csv_file_format_new:
 *
 * Returns: The newly created CSV file format.
 *
 * Since: 3.0.0
 */
GADatasetCSVFileFormat *
gadataset_csv_file_format_new(void)
{
  std::shared_ptr<arrow::dataset::FileFormat> arrow_format =
    std::make_shared<arrow::dataset::CsvFileFormat>();
  return GADATASET_CSV_FILE_FORMAT(gadataset_file_format_new_raw(&arrow_format));
}


G_DEFINE_TYPE(GADatasetIPCFileFormat,
              gadataset_ipc_file_format,
              GADATASET_TYPE_FILE_FORMAT)

static void
gadataset_ipc_file_format_init(GADatasetIPCFileFormat *object)
{
}

static void
gadataset_ipc_file_format_class_init(GADatasetIPCFileFormatClass *klass)
{
}

/**
 * gadataset_ipc_file_format_new:
 *
 * Returns: The newly created IPC file format.
 *
 * Since: 3.0.0
 */
GADatasetIPCFileFormat *
gadataset_ipc_file_format_new(void)
{
  std::shared_ptr<arrow::dataset::FileFormat> arrow_format =
    std::make_shared<arrow::dataset::IpcFileFormat>();
  return GADATASET_IPC_FILE_FORMAT(gadataset_file_format_new_raw(&arrow_format));
}


G_DEFINE_TYPE(GADatasetParquetFileFormat,
              gadataset_parquet_file_format,
              GADATASET_TYPE_FILE_FORMAT)

static void
gadataset_parquet_file_format_init(GADatasetParquetFileFormat *object)
{
}

static void
gadataset_parquet_file_format_class_init(GADatasetParquetFileFormatClass *klass)
{
}

/**
 * gadataset_parquet_file_format_new:
 *
 * Returns: The newly created Parquet file format.
 *
 * Since: 3.0.0
 */
GADatasetParquetFileFormat *
gadataset_parquet_file_format_new(void)
{
  std::shared_ptr<arrow::dataset::FileFormat> arrow_format =
    std::make_shared<arrow::dataset::ParquetFileFormat>();
  return GADATASET_PARQUET_FILE_FORMAT(
    gadataset_file_format_new_raw(&arrow_format));
}


G_END_DECLS

GADatasetFileWriteOptions *
gadataset_file_write_options_new_raw(
  std::shared_ptr<arrow::dataset::FileWriteOptions> *arrow_options)
{
  return GADATASET_FILE_WRITE_OPTIONS(
    g_object_new(GADATASET_TYPE_FILE_WRITE_OPTIONS,
                 "options", arrow_options,
                 NULL));
}

std::shared_ptr<arrow::dataset::FileWriteOptions>
gadataset_file_write_options_get_raw(GADatasetFileWriteOptions *options)
{
  auto priv = GADATASET_FILE_WRITE_OPTIONS_GET_PRIVATE(options);
  return priv->options;
}


GADatasetFileWriter *
gadataset_file_writer_new_raw(
  std::shared_ptr<arrow::dataset::FileWriter> *arrow_writer)
{
  return GADATASET_FILE_WRITER(g_object_new(GADATASET_TYPE_FILE_WRITER,
                                            "writer", arrow_writer,
                                            NULL));
}

std::shared_ptr<arrow::dataset::FileWriter>
gadataset_file_writer_get_raw(GADatasetFileWriter *writer)
{
  auto priv = GADATASET_FILE_WRITER_GET_PRIVATE(writer);
  return priv->writer;
}


GADatasetFileFormat *
gadataset_file_format_new_raw(
  std::shared_ptr<arrow::dataset::FileFormat> *arrow_format)
{
  GType type = GADATASET_TYPE_FILE_FORMAT;
  const auto &type_name = (*arrow_format)->type_name();
  if (type_name == "csv") {
    type = GADATASET_TYPE_CSV_FILE_FORMAT;
  } else if (type_name == "ipc") {
    type = GADATASET_TYPE_IPC_FILE_FORMAT;
  } else if (type_name == "parquet") {
    type = GADATASET_TYPE_PARQUET_FILE_FORMAT;
  }
  return GADATASET_FILE_FORMAT(g_object_new(type,
                                            "format", arrow_format,
                                            NULL));
}

std::shared_ptr<arrow::dataset::FileFormat>
gadataset_file_format_get_raw(GADatasetFileFormat *format)
{
  auto priv = GADATASET_FILE_FORMAT_GET_PRIVATE(format);
  return priv->format;
}
