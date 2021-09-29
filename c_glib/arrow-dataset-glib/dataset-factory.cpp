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

#include <arrow-dataset-glib/dataset-factory.hpp>
#include <arrow-dataset-glib/dataset.hpp>
#include <arrow-dataset-glib/file-format.hpp>
#include <arrow-dataset-glib/partitioning.hpp>

G_BEGIN_DECLS

/**
 * SECTION: dataset-factory
 * @section_id: dataset-factory
 * @title: Dataset factory related classes
 * @include: arrow-dataset-glib/arrow-dataset-glib.h
 *
 * #GADatasetDatasetFactory is a base class for dataset factories.
 *
 * #GADatasetFileSystemDatasetFactory is a class for
 * #GADatasetFileSystemDataset factory.
 *
 * Since: 5.0.0
 */

typedef struct GADatasetDatasetFactoryPrivate_ {
  std::shared_ptr<arrow::dataset::DatasetFactory> factory;
} GADatasetDatasetFactoryPrivate;

enum {
  PROP_DATASET_FACTORY = 1,
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GADatasetDatasetFactory,
                                    gadataset_dataset_factory,
                                    G_TYPE_OBJECT)

#define GADATASET_DATASET_FACTORY_GET_PRIVATE(obj)        \
  static_cast<GADatasetDatasetFactoryPrivate *>(          \
    gadataset_dataset_factory_get_instance_private(       \
      GADATASET_DATASET_FACTORY(obj)))

static void
gadataset_dataset_factory_finalize(GObject *object)
{
  auto priv = GADATASET_DATASET_FACTORY_GET_PRIVATE(object);
  priv->factory.~shared_ptr();
  G_OBJECT_CLASS(gadataset_dataset_factory_parent_class)->finalize(object);
}

static void
gadataset_dataset_factory_set_property(GObject *object,
                                       guint prop_id,
                                       const GValue *value,
                                       GParamSpec *pspec)
{
  auto priv = GADATASET_DATASET_FACTORY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_DATASET_FACTORY:
    {
      auto arrow_factory_pointer =
        static_cast<std::shared_ptr<arrow::dataset::DatasetFactory> *>(
          g_value_get_pointer(value));
      if (arrow_factory_pointer) {
        priv->factory = *arrow_factory_pointer;
      }
    }
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gadataset_dataset_factory_init(GADatasetDatasetFactory *object)
{
  auto priv = GADATASET_DATASET_FACTORY_GET_PRIVATE(object);
  new(&priv->factory) std::shared_ptr<arrow::dataset::DatasetFactory>;
}

static void
gadataset_dataset_factory_class_init(GADatasetDatasetFactoryClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->finalize     = gadataset_dataset_factory_finalize;
  gobject_class->set_property = gadataset_dataset_factory_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("dataset-factory",
                              "Dataset factory",
                              "The raw "
                              "std::shared<arrow::dataset::DatasetFactory> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_DATASET_FACTORY, spec);
}

/**
 * gadataset_dataset_factory_finish:
 * @factory: A #GADatasetDatasetFactory.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable):
 *   A newly created #GADatasetDataset on success, %NULL on error.
 *
 * Since: 5.0.0
 */
GADatasetDataset *
gadataset_dataset_factory_finish(GADatasetDatasetFactory *factory,
                                 GError **error)
{
  auto arrow_factory = gadataset_dataset_factory_get_raw(factory);
  auto arrow_dataset_result = arrow_factory->Finish();
  if (garrow::check(error, arrow_dataset_result, "[dataset-factory][finish]")) {
    auto arrow_dataset = *arrow_dataset_result;
    return gadataset_dataset_new_raw(&arrow_dataset);
  } else {
    return NULL;
  }
}


typedef struct GADatasetFileSystemDatasetFactoryPrivate_ {
  GADatasetFileFormat *format;
  GArrowFileSystem *file_system;
  GADatasetPartitioning *partitioning;
  GList *files;
  arrow::dataset::FileSystemFactoryOptions options;
} GADatasetFileSystemDatasetFactoryPrivate;

enum {
  PROP_FORMAT = 1,
  PROP_FILE_SYSTEM,
  PROP_PARTITIONING,
  PROP_PARTITION_BASE_DIR,
};

G_DEFINE_TYPE_WITH_PRIVATE(GADatasetFileSystemDatasetFactory,
                           gadataset_file_system_dataset_factory,
                           GADATASET_TYPE_DATASET_FACTORY)

#define GADATASET_FILE_SYSTEM_DATASET_FACTORY_GET_PRIVATE(obj)  \
  static_cast<GADatasetFileSystemDatasetFactoryPrivate *>(      \
    gadataset_file_system_dataset_factory_get_instance_private( \
      GADATASET_FILE_SYSTEM_DATASET_FACTORY(obj)))

static void
gadataset_file_system_dataset_factory_dispose(GObject *object)
{
  auto priv = GADATASET_FILE_SYSTEM_DATASET_FACTORY_GET_PRIVATE(object);

  if (priv->format) {
    g_object_unref(priv->format);
    priv->format = NULL;
  }

  if (priv->file_system) {
    g_object_unref(priv->file_system);
    priv->file_system = NULL;
  }

  if (priv->partitioning) {
    g_object_unref(priv->partitioning);
    priv->partitioning = NULL;
  }

  if (priv->files) {
    g_list_free_full(priv->files, g_object_unref);
    priv->files = NULL;
  }

  G_OBJECT_CLASS(
    gadataset_file_system_dataset_factory_parent_class)->dispose(object);
}

static void
gadataset_file_system_dataset_factory_finalize(GObject *object)
{
  auto priv = GADATASET_FILE_SYSTEM_DATASET_FACTORY_GET_PRIVATE(object);
  priv->options.~FileSystemFactoryOptions();
  G_OBJECT_CLASS(
    gadataset_file_system_dataset_factory_parent_class)->finalize(object);
}

static void
gadataset_file_system_dataset_factory_set_property(GObject *object,
                                                   guint prop_id,
                                                   const GValue *value,
                                                   GParamSpec *pspec)
{
  auto priv = GADATASET_FILE_SYSTEM_DATASET_FACTORY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FORMAT:
    priv->format = GADATASET_FILE_FORMAT(g_value_dup_object(value));
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
  case PROP_PARTITION_BASE_DIR:
    priv->options.partition_base_dir = g_value_get_string(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gadataset_file_system_dataset_factory_get_property(GObject *object,
                                                   guint prop_id,
                                                   GValue *value,
                                                   GParamSpec *pspec)
{
  auto priv = GADATASET_FILE_SYSTEM_DATASET_FACTORY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FORMAT:
    g_value_set_object(value, priv->format);
    break;
  case PROP_FILE_SYSTEM:
    g_value_set_object(value, priv->file_system);
    break;
  case PROP_PARTITIONING:
    g_value_set_object(value, priv->partitioning);
    break;
  case PROP_PARTITION_BASE_DIR:
    g_value_set_string(value, priv->options.partition_base_dir.c_str());
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gadataset_file_system_dataset_factory_init(
  GADatasetFileSystemDatasetFactory *object)
{
  auto priv = GADATASET_FILE_SYSTEM_DATASET_FACTORY_GET_PRIVATE(object);
  new(&priv->options) arrow::dataset::FileSystemFactoryOptions;
}

static void
gadataset_file_system_dataset_factory_class_init(
  GADatasetFileSystemDatasetFactoryClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->dispose      = gadataset_file_system_dataset_factory_dispose;
  gobject_class->finalize     = gadataset_file_system_dataset_factory_finalize;
  gobject_class->set_property = gadataset_file_system_dataset_factory_set_property;
  gobject_class->get_property = gadataset_file_system_dataset_factory_get_property;

  GParamSpec *spec;
  /**
   * GADatasetFileSystemDatasetFactory:format:
   *
   * Format passed to #GADatasetFileSystemDataset.
   *
   * Since: 5.0.0
   */
  spec = g_param_spec_object("format",
                             "Format",
                             "Format passed to GADatasetFileSystemDataset",
                             GADATASET_TYPE_FILE_FORMAT,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FORMAT, spec);

  /**
   * GADatasetFileSystemDatasetFactory:file-system:
   *
   * File system passed to #GADatasetFileSystemDataset.
   *
   * Since: 5.0.0
   */
  spec = g_param_spec_object("file-system",
                             "File system",
                             "File system passed to GADatasetFileSystemDataset",
                             GARROW_TYPE_FILE_SYSTEM,
                             static_cast<GParamFlags>(G_PARAM_READABLE));
  g_object_class_install_property(gobject_class, PROP_FILE_SYSTEM, spec);

  /**
   * GADatasetFileSystemDatasetFactory:partitioning:
   *
   * Partitioning used by #GADatasetFileSystemDataset.
   *
   * Since: 6.0.0
   */
  spec = g_param_spec_object("partitioning",
                             "Partitioning",
                             "Partitioning used by GADatasetFileSystemDataset",
                             GADATASET_TYPE_PARTITIONING,
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_PARTITIONING, spec);

  /**
   * GADatasetFileSystemDatasetFactory:partition-base-dir:
   *
   * Partition base directory used by #GADatasetFileSystemDataset.
   *
   * Since: 6.0.0
   */
  spec = g_param_spec_string("partition-base-dir",
                             "Partition base directory",
                             "Partition base directory "
                             "used by GADatasetFileSystemDataset",
                             NULL,
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_PARTITION_BASE_DIR, spec);
}

/**
 * gadataset_file_system_factory_new:
 * @format: A #GADatasetFileFormat.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: A newly created #GADatasetDatasetFileSystemFactory on success,
 *   %NULL on error.
 *
 * Since: 5.0.0
 */
GADatasetFileSystemDatasetFactory *
gadataset_file_system_dataset_factory_new(GADatasetFileFormat *format)
{
  return GADATASET_FILE_SYSTEM_DATASET_FACTORY(
    g_object_new(GADATASET_TYPE_FILE_SYSTEM_DATASET_FACTORY,
                 "format", format,
                 NULL));
}

/**
 * gadataset_file_system_dataset_factory_set_file_system:
 * @factory: A #GADatasetFileSystemDatasetFactory.
 * @file_system: A #GArrowFileSystem.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE otherwise.
 *
 * Since: 5.0.0
 */
gboolean
gadataset_file_system_dataset_factory_set_file_system(
  GADatasetFileSystemDatasetFactory *factory,
  GArrowFileSystem *file_system,
  GError **error)
{
  const gchar *context = "[file-system-dataset-factory][set-file-system]";
  auto priv = GADATASET_FILE_SYSTEM_DATASET_FACTORY_GET_PRIVATE(factory);
  if (priv->file_system) {
    garrow::check(error,
                  arrow::Status::Invalid("file system is already set"),
                  context);
    return FALSE;
  }
  priv->file_system = file_system;
  g_object_ref(priv->file_system);
  return TRUE;
}

/**
 * gadataset_file_system_dataset_factory_set_file_system_uri:
 * @factory: A #GADatasetFileSystemDatasetFactory.
 * @uri: An URI for file system.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE otherwise.
 *
 * Since: 5.0.0
 */
gboolean
gadataset_file_system_dataset_factory_set_file_system_uri(
  GADatasetFileSystemDatasetFactory *factory,
  const gchar *uri,
  GError **error)
{
  const gchar *context = "[file-system-dataset-factory][set-file-system-uri]";
  auto priv = GADATASET_FILE_SYSTEM_DATASET_FACTORY_GET_PRIVATE(factory);
  if (priv->file_system) {
    garrow::check(error,
                  arrow::Status::Invalid("file system is already set"),
                  context);
    return FALSE;
  }
  std::string internal_path;
  auto arrow_file_system_result =
    arrow::fs::FileSystemFromUri(uri, &internal_path);
  if (!garrow::check(error, arrow_file_system_result, context)) {
    return FALSE;
  }
  auto arrow_file_system = *arrow_file_system_result;
  auto arrow_file_info_result = arrow_file_system->GetFileInfo(internal_path);
  if (!garrow::check(error, arrow_file_info_result, context)) {
    return FALSE;
  }
  priv->file_system = garrow_file_system_new_raw(&arrow_file_system);
  auto file_info = garrow_file_info_new_raw(*arrow_file_info_result);
  priv->files = g_list_prepend(priv->files, file_info);
  return TRUE;
}

/**
 * gadataset_file_system_dataset_factory_add_path:
 * @factory: A #GADatasetFileSystemDatasetFactory.
 * @path: A path to be added.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE otherwise.
 *
 * Since: 5.0.0
 */
gboolean
gadataset_file_system_dataset_factory_add_path(
  GADatasetFileSystemDatasetFactory *factory,
  const gchar *path,
  GError **error)
{
  const gchar *context = "[file-system-dataset-factory][add-path]";
  auto priv = GADATASET_FILE_SYSTEM_DATASET_FACTORY_GET_PRIVATE(factory);
  if (!priv->file_system) {
    garrow::check(error,
                  arrow::Status::Invalid("file system isn't set"),
                  context);
    return FALSE;
  }
  auto arrow_file_system = garrow_file_system_get_raw(priv->file_system);
  auto arrow_file_info_result = arrow_file_system->GetFileInfo(path);
  if (!garrow::check(error, arrow_file_info_result, context)) {
    return FALSE;
  }
  auto file_info = garrow_file_info_new_raw(*arrow_file_info_result);
  priv->files = g_list_prepend(priv->files, file_info);
  return TRUE;
}

/**
 * gadataset_file_system_dataset_factory_finish:
 * @factory: A #GADatasetFileSystemDatasetFactory.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable):
 *   A newly created #GADatasetFileSystemDataset on success, %NULL on error.
 *
 * Since: 5.0.0
 */
GADatasetFileSystemDataset *
gadataset_file_system_dataset_factory_finish(
  GADatasetFileSystemDatasetFactory *factory,
  GError **error)
{
  const gchar *context = "[file-system-dataset-factory][finish]";
  auto priv = GADATASET_FILE_SYSTEM_DATASET_FACTORY_GET_PRIVATE(factory);
  if (!priv->file_system) {
    garrow::check(error,
                  arrow::Status::Invalid("file system isn't set"),
                  context);
    return NULL;
  }
  auto arrow_file_system = garrow_file_system_get_raw(priv->file_system);
  auto arrow_format = gadataset_file_format_get_raw(priv->format);
  arrow::Result<std::shared_ptr<arrow::dataset::DatasetFactory>>
    arrow_factory_result;
  if (priv->files &&
      !priv->files->next &&
      garrow_file_info_is_dir(GARROW_FILE_INFO(priv->files->data))) {
    auto file = GARROW_FILE_INFO(priv->files->data);
    arrow::fs::FileSelector arrow_selector;
    arrow_selector.base_dir = garrow_file_info_get_raw(file)->path();
    arrow_selector.recursive = true;
    arrow_factory_result =
      arrow::dataset::FileSystemDatasetFactory::Make(arrow_file_system,
                                                     arrow_selector,
                                                     arrow_format,
                                                     priv->options);
  } else {
    std::vector<arrow::fs::FileInfo> arrow_files;
    priv->files = g_list_reverse(priv->files);
    for (auto node = priv->files; node; node = node->next) {
      auto file = GARROW_FILE_INFO(node->data);
      arrow_files.push_back(*garrow_file_info_get_raw(file));
    }
    priv->files = g_list_reverse(priv->files);
    arrow_factory_result =
      arrow::dataset::FileSystemDatasetFactory::Make(arrow_file_system,
                                                     arrow_files,
                                                     arrow_format,
                                                     priv->options);
  }
  if (!garrow::check(error, arrow_factory_result, context)) {
    return NULL;
  }
  auto arrow_dataset_result = (*arrow_factory_result)->Finish();
  if (!garrow::check(error, arrow_dataset_result, context)) {
    return NULL;
  }
  auto arrow_dataset = *arrow_dataset_result;
  return GADATASET_FILE_SYSTEM_DATASET(
    gadataset_dataset_new_raw(&arrow_dataset,
                              "dataset", &arrow_dataset,
                              "file-system", priv->file_system,
                              "format", priv->format,
                              "partitioning", priv->partitioning,
                              NULL));
}


G_END_DECLS

std::shared_ptr<arrow::dataset::DatasetFactory>
gadataset_dataset_factory_get_raw(GADatasetDatasetFactory *factory)
{
  auto priv = GADATASET_DATASET_FACTORY_GET_PRIVATE(factory);
  return priv->factory;
}
