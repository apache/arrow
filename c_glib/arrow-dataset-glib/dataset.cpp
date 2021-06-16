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
#include <arrow-glib/table.hpp>

#include <arrow-dataset-glib/dataset-factory.hpp>
#include <arrow-dataset-glib/dataset.hpp>
#include <arrow-dataset-glib/scanner.h>

G_BEGIN_DECLS

/**
 * SECTION: dataset
 * @section_id: dataset
 * @title: Dataset related classes
 * @include: arrow-dataset-glib/arrow-dataset-glib.h
 *
 * #GADatasetDataset is a base class for datasets.
 *
 * #GADatasetFileSystemDataset is a class for file system dataset.
 *
 * #GADatasetFileFormat is a base class for file formats.
 *
 * #GADatasetCSVFileFormat is a class for CSV file format.
 *
 * #GADatasetIPCFileFormat is a class for IPC file format.
 *
 * #GADatasetParquetFileFormat is a class for Apache Parquet file format.
 *
 * Since: 5.0.0
 */

typedef struct GADatasetDatasetPrivate_ {
  std::shared_ptr<arrow::dataset::Dataset> dataset;
} GADatasetDatasetPrivate;

enum {
  PROP_DATASET = 1,
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GADatasetDataset,
                                    gadataset_dataset,
                                    G_TYPE_OBJECT)

#define GADATASET_DATASET_GET_PRIVATE(obj)         \
  static_cast<GADatasetDatasetPrivate *>(          \
    gadataset_dataset_get_instance_private(        \
      GADATASET_DATASET(obj)))

static void
gadataset_dataset_finalize(GObject *object)
{
  auto priv = GADATASET_DATASET_GET_PRIVATE(object);
  priv->dataset.~shared_ptr();
  G_OBJECT_CLASS(gadataset_dataset_parent_class)->finalize(object);
}

static void
gadataset_dataset_set_property(GObject *object,
                               guint prop_id,
                               const GValue *value,
                               GParamSpec *pspec)
{
  auto priv = GADATASET_DATASET_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_DATASET:
    priv->dataset =
      *static_cast<std::shared_ptr<arrow::dataset::Dataset> *>(
        g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gadataset_dataset_init(GADatasetDataset *object)
{
  auto priv = GADATASET_DATASET_GET_PRIVATE(object);
  new(&priv->dataset) std::shared_ptr<arrow::dataset::Dataset>;
}

static void
gadataset_dataset_class_init(GADatasetDatasetClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->finalize     = gadataset_dataset_finalize;
  gobject_class->set_property = gadataset_dataset_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("dataset",
                              "Dataset",
                              "The raw "
                              "std::shared<arrow::dataset::Dataset> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_DATASET, spec);
}

/**
 * gadataset_dataset_begin_scan:
 * @dataset: A #GADatasetDataset.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable):
 *   A newly created #GADatasetScannerBuilder on success, %NULL on error.
 *
 * Since: 5.0.0
 */
GADatasetScannerBuilder *
gadataset_dataset_begin_scan(GADatasetDataset *dataset,
                             GError **error)
{
  return gadataset_scanner_builder_new(dataset, error);
}

/**
 * gadataset_dataset_to_table:
 * @dataset: A #GADatasetDataset.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable):
 *   A loaded #GArrowTable on success, %NULL on error.
 *
 * Since: 5.0.0
 */
GArrowTable *
gadataset_dataset_to_table(GADatasetDataset *dataset,
                           GError **error)
{
  auto arrow_dataset = gadataset_dataset_get_raw(dataset);
  auto arrow_scanner_builder_result = arrow_dataset->NewScan();
  if (!garrow::check(error,
                     arrow_scanner_builder_result,
                     "[dataset][to-table]")) {
    return NULL;
  }
  auto arrow_scanner_builder = *arrow_scanner_builder_result;
  auto arrow_scanner_result = arrow_scanner_builder->Finish();
  if (!garrow::check(error,
                     arrow_scanner_result,
                     "[dataset][to-table]")) {
    return NULL;
  }
  auto arrow_scanner = *arrow_scanner_result;
  auto arrow_table_result = arrow_scanner->ToTable();
  if (!garrow::check(error,
                     arrow_scanner_result,
                     "[dataset][to-table]")) {
    return NULL;
  }
  return garrow_table_new_raw(&(*arrow_table_result));
}

/**
 * gadataset_dataset_get_type_name:
 * @dataset: A #GADatasetDataset.
 *
 * Returns: The type name of @dataset.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 5.0.0
 */
gchar *
gadataset_dataset_get_type_name(GADatasetDataset *dataset)
{
  const auto arrow_dataset = gadataset_dataset_get_raw(dataset);
  const auto &type_name = arrow_dataset->type_name();
  return g_strndup(type_name.data(), type_name.size());
}


typedef struct GADatasetFileSystemDatasetPrivate_ {
  GADatasetFileFormat *format;
  GArrowFileSystem *file_system;
} GADatasetFileSystemDatasetPrivate;

enum {
  PROP_FORMAT = 1,
  PROP_FILE_SYSTEM,
};

G_DEFINE_TYPE_WITH_PRIVATE(GADatasetFileSystemDataset,
                           gadataset_file_system_dataset,
                           GADATASET_TYPE_DATASET)

#define GADATASET_FILE_SYSTEM_DATASET_GET_PRIVATE(obj)   \
  static_cast<GADatasetFileSystemDatasetPrivate *>(      \
    gadataset_file_system_dataset_get_instance_private(  \
      GADATASET_FILE_SYSTEM_DATASET(obj)))

static void
gadataset_file_system_dataset_dispose(GObject *object)
{
  auto priv = GADATASET_FILE_SYSTEM_DATASET_GET_PRIVATE(object);

  if (priv->format) {
    g_object_unref(priv->format);
    priv->format = NULL;
  }

  if (priv->file_system) {
    g_object_unref(priv->file_system);
    priv->file_system = NULL;
  }

  G_OBJECT_CLASS(gadataset_file_system_dataset_parent_class)->dispose(object);
}

static void
gadataset_file_system_dataset_set_property(GObject *object,
                                           guint prop_id,
                                           const GValue *value,
                                           GParamSpec *pspec)
{
  auto priv = GADATASET_FILE_SYSTEM_DATASET_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FORMAT:
    priv->format = GADATASET_FILE_FORMAT(g_value_dup_object(value));
    break;
  case PROP_FILE_SYSTEM:
    priv->file_system = GARROW_FILE_SYSTEM(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gadataset_file_system_dataset_get_property(GObject *object,
                                           guint prop_id,
                                           GValue *value,
                                           GParamSpec *pspec)
{
  auto priv = GADATASET_FILE_SYSTEM_DATASET_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FORMAT:
    g_value_set_object(value, priv->format);
    break;
  case PROP_FILE_SYSTEM:
    g_value_set_object(value, priv->file_system);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gadataset_file_system_dataset_init(GADatasetFileSystemDataset *object)
{
}

static void
gadataset_file_system_dataset_class_init(GADatasetFileSystemDatasetClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->dispose      = gadataset_file_system_dataset_dispose;
  gobject_class->set_property = gadataset_file_system_dataset_set_property;
  gobject_class->get_property = gadataset_file_system_dataset_get_property;

  GParamSpec *spec;
  /**
   * GADatasetFileSystemDataset:format:
   *
   * Format of the dataset.
   *
   * Since: 5.0.0
   */
  spec = g_param_spec_object("format",
                             "Format",
                             "Format of the dataset",
                             GADATASET_TYPE_FILE_FORMAT,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FORMAT, spec);

  /**
   * GADatasetFileSystemDataset:file-system:
   *
   * File system of the dataset.
   *
   * Since: 5.0.0
   */
  spec = g_param_spec_object("file-system",
                             "File system",
                             "File system of the dataset",
                             GARROW_TYPE_FILE_SYSTEM,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FILE_SYSTEM, spec);
}


G_END_DECLS

GADatasetDataset *
gadataset_dataset_new_raw(
  std::shared_ptr<arrow::dataset::Dataset> *arrow_dataset)
{
  return gadataset_dataset_new_raw(arrow_dataset,
                                   "dataset", arrow_dataset,
                                   NULL);
}

GADatasetDataset *
gadataset_dataset_new_raw(
  std::shared_ptr<arrow::dataset::Dataset> *arrow_dataset,
  const gchar *first_property_name,
  ...)
{
  va_list args;
  va_start(args, first_property_name);
  auto array = gadataset_dataset_new_raw_valist(arrow_dataset,
                                                first_property_name,
                                                args);
  va_end(args);
  return array;
}

GADatasetDataset *
gadataset_dataset_new_raw_valist(
  std::shared_ptr<arrow::dataset::Dataset> *arrow_dataset,
  const gchar *first_property_name,
  va_list args)
{
  GType type = GADATASET_TYPE_DATASET;
  const auto type_name = (*arrow_dataset)->type_name();
  if (type_name == "filesystem") {
    type = GADATASET_TYPE_FILE_SYSTEM_DATASET;
  }
  return GADATASET_DATASET(g_object_new_valist(type,
                                               first_property_name,
                                               args));
}

std::shared_ptr<arrow::dataset::Dataset>
gadataset_dataset_get_raw(GADatasetDataset *dataset)
{
  auto priv = GADATASET_DATASET_GET_PRIVATE(dataset);
  return priv->dataset;
}
