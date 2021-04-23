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

#include <arrow-dataset-glib/file-format.hpp>

G_BEGIN_DECLS

/**
 * SECTION: file-format
 * @section_id: file-format
 * @title: File format classes
 * @include: arrow-dataset-glib/arrow-dataset-glib.h
 *
 * #GADFileFormat is a base class for file format classes.
 *
 * #GADCSVFileFormat is a class for CSV file format.
 *
 * #GADIPCFileFormat is a class for IPC file format.
 *
 * #GADParquetFileFormat is a class for Parquet file format.
 *
 * * Since: 3.0.0
 */

typedef struct GADFileFormatPrivate_ {
  std::shared_ptr<arrow::dataset::FileFormat> file_format;
} GADFileFormatPrivate;

enum {
  PROP_FILE_FORMAT = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GADFileFormat,
                           gad_file_format,
                           G_TYPE_OBJECT)

#define GAD_FILE_FORMAT_GET_PRIVATE(obj)        \
  static_cast<GADFileFormatPrivate *>(          \
    gad_file_format_get_instance_private(       \
      GAD_FILE_FORMAT(obj)))

static void
gad_file_format_finalize(GObject *object)
{
  auto priv = GAD_FILE_FORMAT_GET_PRIVATE(object);

  priv->file_format.~shared_ptr();

  G_OBJECT_CLASS(gad_file_format_parent_class)->finalize(object);
}

static void
gad_file_format_set_property(GObject *object,
                             guint prop_id,
                             const GValue *value,
                             GParamSpec *pspec)
{
  auto priv = GAD_FILE_FORMAT_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FILE_FORMAT:
    priv->file_format =
      *static_cast<std::shared_ptr<arrow::dataset::FileFormat> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gad_file_format_init(GADFileFormat *object)
{
  auto priv = GAD_FILE_FORMAT_GET_PRIVATE(object);
  new(&priv->file_format) std::shared_ptr<arrow::dataset::FileFormat>;
}

static void
gad_file_format_class_init(GADFileFormatClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = gad_file_format_finalize;
  gobject_class->set_property = gad_file_format_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("file-format",
                              "FileFormat",
                              "The raw std::shared<arrow::dataset::FileFormat> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FILE_FORMAT, spec);
}

/**
 * gad_file_format_get_type_name:
 * @file_format: A #GADFileFormat.
 *
 * Returns: The type name of @file_format.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 3.0.0
 */
gchar *
gad_file_format_get_type_name(GADFileFormat *file_format)
{
  const auto arrow_file_format = gad_file_format_get_raw(file_format);
  const auto &type_name = arrow_file_format->type_name();
  return g_strndup(type_name.data(), type_name.size());
}

/**
 * gad_file_format_equal:
 * @file_format: A #GADFileFormat.
 * @other_file_format: A #GADFileFormat to be compared.
 *
 * Returns: %TRUE if they are the same content file format, %FALSE otherwise.
 *
 * Since: 3.0.0
 */
gboolean
gad_file_format_equal(GADFileFormat *file_format,
                      GADFileFormat *other_file_format)
{
  const auto arrow_file_format = gad_file_format_get_raw(file_format);
  const auto arrow_other_file_format = gad_file_format_get_raw(other_file_format);
  return arrow_file_format->Equals(*arrow_other_file_format);
}


G_DEFINE_TYPE(GADCSVFileFormat,
              gad_csv_file_format,
              GAD_TYPE_FILE_FORMAT)

static void
gad_csv_file_format_init(GADCSVFileFormat *object)
{
}

static void
gad_csv_file_format_class_init(GADCSVFileFormatClass *klass)
{
}

/**
 * gad_csv_file_format_new:
 *
 * Returns: The newly created CSV file format.
 *
 * Since: 3.0.0
 */
GADCSVFileFormat *
gad_csv_file_format_new(void)
{
  std::shared_ptr<arrow::dataset::FileFormat> arrow_file_format =
    std::make_shared<arrow::dataset::CsvFileFormat>();
  return GAD_CSV_FILE_FORMAT(gad_file_format_new_raw(&arrow_file_format));
}


G_DEFINE_TYPE(GADIPCFileFormat,
              gad_ipc_file_format,
              GAD_TYPE_FILE_FORMAT)

static void
gad_ipc_file_format_init(GADIPCFileFormat *object)
{
}

static void
gad_ipc_file_format_class_init(GADIPCFileFormatClass *klass)
{
}

/**
 * gad_ipc_file_format_new:
 *
 * Returns: The newly created IPC file format.
 *
 * Since: 3.0.0
 */
GADIPCFileFormat *
gad_ipc_file_format_new(void)
{
  std::shared_ptr<arrow::dataset::FileFormat> arrow_file_format =
    std::make_shared<arrow::dataset::IpcFileFormat>();
  return GAD_IPC_FILE_FORMAT(gad_file_format_new_raw(&arrow_file_format));
}


G_DEFINE_TYPE(GADParquetFileFormat,
              gad_parquet_file_format,
              GAD_TYPE_FILE_FORMAT)

static void
gad_parquet_file_format_init(GADParquetFileFormat *object)
{
}

static void
gad_parquet_file_format_class_init(GADParquetFileFormatClass *klass)
{
}

/**
 * gad_parquet_file_format_new:
 *
 * Returns: The newly created Parquet file format.
 *
 * Since: 3.0.0
 */
GADParquetFileFormat *
gad_parquet_file_format_new(void)
{
  std::shared_ptr<arrow::dataset::FileFormat> arrow_file_format =
    std::make_shared<arrow::dataset::ParquetFileFormat>();
  return GAD_PARQUET_FILE_FORMAT(gad_file_format_new_raw(&arrow_file_format));
}


G_END_DECLS

GADFileFormat *
gad_file_format_new_raw(
  std::shared_ptr<arrow::dataset::FileFormat> *arrow_file_format)
{
  GType type = GAD_TYPE_FILE_FORMAT;
  const auto &type_name = (*arrow_file_format)->type_name();
  if (type_name == "csv") {
    type = GAD_TYPE_CSV_FILE_FORMAT;
  } else if (type_name == "ipc") {
    type = GAD_TYPE_IPC_FILE_FORMAT;
  } else if (type_name == "parquet") {
    type = GAD_TYPE_PARQUET_FILE_FORMAT;
  }
  return GAD_FILE_FORMAT(g_object_new(type,
                                      "file-format", arrow_file_format,
                                      NULL));
}

std::shared_ptr<arrow::dataset::FileFormat>
gad_file_format_get_raw(GADFileFormat *file_format)
{
  auto priv = GAD_FILE_FORMAT_GET_PRIVATE(file_format);
  return priv->file_format;
}
