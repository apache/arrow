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
#include <arrow-glib/table.hpp>

#include <arrow-dataset-glib/dataset-factory.hpp>
#include <arrow-dataset-glib/dataset.hpp>
#include <arrow-dataset-glib/file-format.hpp>
#include <arrow-dataset-glib/partitioning.hpp>
#include <arrow-dataset-glib/scanner.hpp>

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
 * #GADatasetFileSystemDatasetWriteOptions is a class for options to
 * write a dataset to file system dataset.
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


typedef struct GADatasetFileSystemDatasetWriteOptionsPrivate_ {
  arrow::dataset::FileSystemDatasetWriteOptions options;
  GADatasetFileWriteOptions *file_write_options;
  GArrowFileSystem *file_system;
  GADatasetPartitioning *partitioning;
} GADatasetFileSystemDatasetWriteOptionsPrivate;

enum {
  PROP_FILE_WRITE_OPTIONS = 1,
  PROP_FILE_SYSTEM,
  PROP_BASE_DIR,
  PROP_PARTITIONING,
  PROP_MAX_PARTITIONS,
  PROP_BASE_NAME_TEMPLATE,
};

G_DEFINE_TYPE_WITH_PRIVATE(GADatasetFileSystemDatasetWriteOptions,
                           gadataset_file_system_dataset_write_options,
                           G_TYPE_OBJECT)

#define GADATASET_FILE_SYSTEM_DATASET_WRITE_OPTIONS_GET_PRIVATE(obj)    \
  static_cast<GADatasetFileSystemDatasetWriteOptionsPrivate *>(         \
    gadataset_file_system_dataset_write_options_get_instance_private(   \
      GADATASET_FILE_SYSTEM_DATASET_WRITE_OPTIONS(obj)))

static void
gadataset_file_system_dataset_write_options_finalize(GObject *object)
{
  auto priv = GADATASET_FILE_SYSTEM_DATASET_WRITE_OPTIONS_GET_PRIVATE(object);
  priv->options.~FileSystemDatasetWriteOptions();
  G_OBJECT_CLASS(gadataset_file_system_dataset_write_options_parent_class)->
    finalize(object);
}

static void
gadataset_file_system_dataset_write_options_dispose(GObject *object)
{
  auto priv = GADATASET_FILE_SYSTEM_DATASET_WRITE_OPTIONS_GET_PRIVATE(object);

  if (priv->file_write_options) {
    g_object_unref(priv->file_write_options);
    priv->file_write_options = NULL;
  }

  if (priv->file_system) {
    g_object_unref(priv->file_system);
    priv->file_system = NULL;
  }

  if (priv->partitioning) {
    g_object_unref(priv->partitioning);
    priv->partitioning = NULL;
  }

  G_OBJECT_CLASS(gadataset_file_system_dataset_write_options_parent_class)->
    dispose(object);
}

static void
gadataset_file_system_dataset_write_options_set_property(GObject *object,
                                                         guint prop_id,
                                                         const GValue *value,
                                                         GParamSpec *pspec)
{
  auto priv = GADATASET_FILE_SYSTEM_DATASET_WRITE_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FILE_WRITE_OPTIONS:
    {
      auto file_write_options = g_value_get_object(value);
      if (file_write_options == priv->file_write_options) {
        break;
      }
      auto old_file_write_options = priv->file_write_options;
      if (file_write_options) {
        g_object_ref(file_write_options);
        priv->file_write_options =
          GADATASET_FILE_WRITE_OPTIONS(file_write_options);
        priv->options.file_write_options =
          gadataset_file_write_options_get_raw(priv->file_write_options);
      } else {
        priv->options.file_write_options = nullptr;
      }
      if (old_file_write_options) {
        g_object_unref(old_file_write_options);
      }
    }
    break;
  case PROP_FILE_SYSTEM:
    {
      auto file_system = g_value_get_object(value);
      if (file_system == priv->file_system) {
        break;
      }
      auto old_file_system = priv->file_system;
      if (file_system) {
        g_object_ref(file_system);
        priv->file_system = GARROW_FILE_SYSTEM(file_system);
        priv->options.filesystem = garrow_file_system_get_raw(priv->file_system);
      } else {
        priv->options.filesystem = nullptr;
      }
      if (old_file_system) {
        g_object_unref(old_file_system);
      }
    }
    break;
  case PROP_BASE_DIR:
    priv->options.base_dir = g_value_get_string(value);
    break;
  case PROP_PARTITIONING:
    {
      auto partitioning = g_value_get_object(value);
      if (partitioning == priv->partitioning) {
        break;
      }
      auto old_partitioning = priv->partitioning;
      if (partitioning) {
        g_object_ref(partitioning);
        priv->partitioning = GADATASET_PARTITIONING(partitioning);
        priv->options.partitioning =
          gadataset_partitioning_get_raw(priv->partitioning);
      } else {
        priv->options.partitioning = arrow::dataset::Partitioning::Default();
      }
      if (old_partitioning) {
        g_object_unref(old_partitioning);
      }
    }
    break;
  case PROP_MAX_PARTITIONS:
    priv->options.max_partitions = g_value_get_uint(value);
    break;
  case PROP_BASE_NAME_TEMPLATE:
    priv->options.basename_template = g_value_get_string(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gadataset_file_system_dataset_write_options_get_property(GObject *object,
                                                         guint prop_id,
                                                         GValue *value,
                                                         GParamSpec *pspec)
{
  auto priv = GADATASET_FILE_SYSTEM_DATASET_WRITE_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FILE_WRITE_OPTIONS:
    g_value_set_object(value, priv->file_write_options);
    break;
  case PROP_FILE_SYSTEM:
    g_value_set_object(value, priv->file_system);
    break;
  case PROP_BASE_DIR:
    g_value_set_string(value, priv->options.base_dir.c_str());
    break;
  case PROP_PARTITIONING:
    g_value_set_object(value, priv->partitioning);
    break;
  case PROP_MAX_PARTITIONS:
    g_value_set_uint(value, priv->options.max_partitions);
    break;
  case PROP_BASE_NAME_TEMPLATE:
    g_value_set_string(value, priv->options.basename_template.c_str());
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gadataset_file_system_dataset_write_options_init(
  GADatasetFileSystemDatasetWriteOptions *object)
{
  auto priv = GADATASET_FILE_SYSTEM_DATASET_WRITE_OPTIONS_GET_PRIVATE(object);
  new(&(priv->options)) arrow::dataset::FileSystemDatasetWriteOptions;
  priv->options.partitioning = arrow::dataset::Partitioning::Default();
}

static void
gadataset_file_system_dataset_write_options_class_init(
  GADatasetFileSystemDatasetWriteOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->finalize =
    gadataset_file_system_dataset_write_options_finalize;
  gobject_class->dispose =
    gadataset_file_system_dataset_write_options_dispose;
  gobject_class->set_property =
    gadataset_file_system_dataset_write_options_set_property;
  gobject_class->get_property =
    gadataset_file_system_dataset_write_options_get_property;

  arrow::dataset::FileSystemDatasetWriteOptions default_options;
  GParamSpec *spec;
  /**
   * GADatasetFileSystemDatasetWriteOptions:file_write_options:
   *
   * Options for individual fragment writing.
   *
   * Since: 6.0.0
   */
  spec = g_param_spec_object("file-write-options",
                             "File write options",
                             "Options for individual fragment writing",
                             GADATASET_TYPE_FILE_WRITE_OPTIONS,
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_FILE_WRITE_OPTIONS, spec);

  /**
   * GADatasetFileSystemDatasetWriteOptions:file_system:
   *
   * #GArrowFileSystem into which a dataset will be written.
   *
   * Since: 6.0.0
   */
  spec = g_param_spec_object("file-system",
                             "File system",
                             "GArrowFileSystem into which "
                             "a dataset will be written",
                             GARROW_TYPE_FILE_SYSTEM,
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_FILE_SYSTEM, spec);

  /**
   * GADatasetFileSystemDatasetWriteOptions:base_dir:
   *
   * Root directory into which the dataset will be written.
   *
   * Since: 6.0.0
   */
  spec = g_param_spec_string("base-dir",
                             "Base directory",
                             "Root directory into which "
                             "the dataset will be written",
                             NULL,
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_BASE_DIR, spec);

  /**
   * GADatasetFileSystemDatasetWriteOptions:partitioning:
   *
   * #GADatasetPartitioning used to generate fragment paths.
   *
   * Since: 6.0.0
   */
  spec = g_param_spec_object("partitioning",
                             "Partitioning",
                             "GADatasetPartitioning used to "
                             "generate fragment paths",
                             GADATASET_TYPE_PARTITIONING,
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_PARTITIONING, spec);

  /**
   * GADatasetFileSystemDatasetWriteOptions:max-partitions:
   *
   * Maximum number of partitions any batch may be written into.
   *
   * Since: 6.0.0
   */
  spec = g_param_spec_uint("max-partitions",
                           "Max partitions",
                           "Maximum number of partitions "
                           "any batch may be written into",
                           0,
                           G_MAXINT,
                           default_options.max_partitions,
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_MAX_PARTITIONS, spec);

  /**
   * GADatasetFileSystemDatasetWriteOptions:base-name-template:
   *
   * Template string used to generate fragment base names. {i} will be
   * replaced by an auto incremented integer.
   *
   * Since: 6.0.0
   */
  spec = g_param_spec_string("base-name-template",
                             "Base name template",
                             "Template string used to generate fragment "
                             "base names. {i} will be replaced by "
                             "an auto incremented integer",
                             NULL,
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_BASE_NAME_TEMPLATE, spec);
}

/**
 * gadataset_file_system_dataset_write_options_new:
 *
 * Returns: The newly created #GADatasetFileSystemDatasetWriteOptions.
 *
 * Since: 6.0.0
 */
GADatasetFileSystemDatasetWriteOptions *
gadataset_file_system_dataset_write_options_new(void)
{
  return GADATASET_FILE_SYSTEM_DATASET_WRITE_OPTIONS(
    g_object_new(GADATASET_TYPE_FILE_SYSTEM_DATASET_WRITE_OPTIONS,
                 NULL));
}


typedef struct GADatasetFileSystemDatasetPrivate_ {
  GADatasetFileFormat *format;
  GArrowFileSystem *file_system;
  GADatasetPartitioning *partitioning;
} GADatasetFileSystemDatasetPrivate;

enum {
  PROP_FILE_SYSTEM_DATASET_FORMAT = 1,
  PROP_FILE_SYSTEM_DATASET_FILE_SYSTEM,
  PROP_FILE_SYSTEM_DATASET_PARTITIONING,
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
  case PROP_FILE_SYSTEM_DATASET_FORMAT:
    priv->format = GADATASET_FILE_FORMAT(g_value_dup_object(value));
    break;
  case PROP_FILE_SYSTEM_DATASET_FILE_SYSTEM:
    priv->file_system = GARROW_FILE_SYSTEM(g_value_dup_object(value));
    break;
  case PROP_FILE_SYSTEM_DATASET_PARTITIONING:
    priv->partitioning = GADATASET_PARTITIONING(g_value_dup_object(value));
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
  case PROP_FILE_SYSTEM_DATASET_FORMAT:
    g_value_set_object(value, priv->format);
    break;
  case PROP_FILE_SYSTEM_DATASET_FILE_SYSTEM:
    g_value_set_object(value, priv->file_system);
    break;
  case PROP_FILE_SYSTEM_DATASET_PARTITIONING:
    g_value_set_object(value, priv->partitioning);
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
  g_object_class_install_property(gobject_class,
                                  PROP_FILE_SYSTEM_DATASET_FORMAT,
                                  spec);

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
  g_object_class_install_property(gobject_class,
                                  PROP_FILE_SYSTEM_DATASET_FILE_SYSTEM,
                                  spec);

  /**
   * GADatasetFileSystemDataset:partitioning:
   *
   * Partitioning of the dataset.
   *
   * Since: 6.0.0
   */
  spec = g_param_spec_object("partitioning",
                             "Partitioning",
                             "Partitioning of the dataset",
                             GADATASET_TYPE_PARTITIONING,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class,
                                  PROP_FILE_SYSTEM_DATASET_PARTITIONING,
                                  spec);
}

/**
 * gadataset_file_system_dataset_write_scanner:
 * @scanner: A #GADatasetScanner that produces data to be written.
 * @options: A #GADatasetFileSystemDatasetWriteOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 6.0.0
 */
gboolean
gadataset_file_system_dataset_write_scanner(
  GADatasetScanner *scanner,
  GADatasetFileSystemDatasetWriteOptions *options,
  GError **error)
{
  auto arrow_scanner = gadataset_scanner_get_raw(scanner);
  auto arrow_options =
    gadataset_file_system_dataset_write_options_get_raw(options);
  auto status =
    arrow::dataset::FileSystemDataset::Write(*arrow_options, arrow_scanner);
  return garrow::check(error,
                       status,
                       "[file-system-dataset][write-scanner]");
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

arrow::dataset::FileSystemDatasetWriteOptions *
gadataset_file_system_dataset_write_options_get_raw(
  GADatasetFileSystemDatasetWriteOptions *options)
{
  auto priv = GADATASET_FILE_SYSTEM_DATASET_WRITE_OPTIONS_GET_PRIVATE(options);
  return &(priv->options);
}
