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

#include <arrow-glib/enums.h>

#include <arrow-glib/error.hpp>
#include <arrow-glib/file-system.hpp>
#include <arrow-glib/input-stream.hpp>
#include <arrow-glib/local-file-system.h>
#include <arrow-glib/output-stream.hpp>

G_BEGIN_DECLS

/**
 * SECTION: file-system
 * @section_id: file-system-classes
 * @title: File system classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowFileInfo is a class for information for a file system entry.
 *
 * #GArrowFileSelector is a class for a selector for file system APIs.
 *
 * #GArrowFileSystem is an interface for file system.
 *
 * #GArrowSubTreeFileSystem is a delegator to another file system that is
 * a logical view of a subtree of a file system, such as a directory in
 * a local file system.
 *
 * #GArrowSlowFileSystem is a delegator to another file system.
 * This inserts latencies at various points.
 *
 * #GArrowMockFileSystem is a class for mock file system that holds
 * its contents in memory.
 *
 * #GArrowHDFSFileSystem is a class for HDFS-backed file system.
 *
 * #GArrowS3GlobalOptions is a class for options to initialize S3 APIs.
 *
 * #GArrowS3FileSystem is a class for S3-backed file system.
 *
 * #GArrowGCSFileSystem is a class for GCS-backed file system.
 */

/* arrow::fs::FileInfo */

typedef struct GArrowFileInfoPrivate_ {
  arrow::fs::FileInfo file_info;
} GArrowFileInfoPrivate;

enum {
  PROP_FILE_INFO_TYPE = 1,
  PROP_FILE_INFO_PATH,
  PROP_FILE_INFO_BASE_NAME,
  PROP_FILE_INFO_DIR_NAME,
  PROP_FILE_INFO_EXTENSION,
  PROP_FILE_INFO_SIZE,
  PROP_FILE_INFO_MTIME,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowFileInfo, garrow_file_info, G_TYPE_OBJECT)

#define GARROW_FILE_INFO_GET_PRIVATE(object)    \
  static_cast<GArrowFileInfoPrivate *>(         \
    garrow_file_info_get_instance_private(      \
      GARROW_FILE_INFO(object)))

static void
garrow_file_info_finalize(GObject *object)
{
  auto priv = GARROW_FILE_INFO_GET_PRIVATE(object);

  priv->file_info.~FileInfo();

  G_OBJECT_CLASS(garrow_file_info_parent_class)->finalize(object);
}

static void
garrow_file_info_set_property(GObject *object,
                              guint prop_id,
                              const GValue *value,
                              GParamSpec *pspec)
{
  auto arrow_file_info = garrow_file_info_get_raw(GARROW_FILE_INFO(object));

  switch (prop_id) {
  case PROP_FILE_INFO_TYPE:
    {
      auto arrow_file_type =
        static_cast<arrow::fs::FileType>(g_value_get_enum(value));
      arrow_file_info->set_type(arrow_file_type);
    }
    break;
  case PROP_FILE_INFO_PATH:
    arrow_file_info->set_path(g_value_get_string(value));
    break;
  case PROP_FILE_INFO_SIZE:
    arrow_file_info->set_size(g_value_get_int64(value));
    break;
  case PROP_FILE_INFO_MTIME:
    {
      const gint64 mtime = g_value_get_int64(value);
      const arrow::fs::TimePoint::duration duration(mtime);
      arrow_file_info->set_mtime(arrow::fs::TimePoint(duration));
    }
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_file_info_get_property(GObject *object,
                              guint prop_id,
                              GValue *value,
                              GParamSpec *pspec)
{
  const auto arrow_file_info =
    garrow_file_info_get_raw(GARROW_FILE_INFO(object));

  switch (prop_id) {
  case PROP_FILE_INFO_TYPE:
    {
      const auto arrow_file_type = arrow_file_info->type();
      const auto file_type = static_cast<GArrowFileType>(arrow_file_type);
      g_value_set_enum(value, file_type);
    }
    break;
  case PROP_FILE_INFO_PATH:
    g_value_set_string(value, arrow_file_info->path().c_str());
    break;
  case PROP_FILE_INFO_BASE_NAME:
    g_value_set_string(value, arrow_file_info->base_name().c_str());
    break;
  case PROP_FILE_INFO_DIR_NAME:
    g_value_set_string(value, arrow_file_info->dir_name().c_str());
    break;
  case PROP_FILE_INFO_EXTENSION:
    g_value_set_string(value, arrow_file_info->extension().c_str());
    break;
  case PROP_FILE_INFO_SIZE:
    g_value_set_int64(value, arrow_file_info->size());
    break;
  case PROP_FILE_INFO_MTIME:
    {
      const auto arrow_mtime = arrow_file_info->mtime();
      const auto mtime = arrow_mtime.time_since_epoch().count();
      g_value_set_int64(value, mtime);
    }
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_file_info_init(GArrowFileInfo *object)
{
  auto priv = GARROW_FILE_INFO_GET_PRIVATE(object);
  new(&priv->file_info) arrow::fs::FileInfo;
}

static void
garrow_file_info_class_init(GArrowFileInfoClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_file_info_finalize;
  gobject_class->set_property = garrow_file_info_set_property;
  gobject_class->get_property = garrow_file_info_get_property;

  auto info = arrow::fs::FileInfo();

  /**
   * GArrowFileInfo:type:
   *
   * The type of the entry.
   *
   * Since: 0.17.0
   */
  spec = g_param_spec_enum("type",
                           "Type",
                           "The type of the entry",
                           GARROW_TYPE_FILE_TYPE,
                           GARROW_FILE_TYPE_UNKNOWN,
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_FILE_INFO_TYPE, spec);

  /**
   * GArrowFileInfo:path:
   *
   * The full file path in the file system.
   *
   * Since: 0.17.0
   */
  spec = g_param_spec_string("path",
                             "Path",
                             "The full file path",
                             info.path().c_str(),
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_FILE_INFO_PATH, spec);

  /**
   * GArrowFileInfo:base-name:
   *
   * The file base name (component after the last directory separator).
   *
   * Since: 0.17.0
   */
  spec = g_param_spec_string("base-name",
                             "Base name",
                             "The file base name",
                             info.base_name().c_str(),
                             static_cast<GParamFlags>(G_PARAM_READABLE));
  g_object_class_install_property(gobject_class,
                                  PROP_FILE_INFO_BASE_NAME,
                                  spec);

  /**
   * GArrowFileInfo:dir-name:
   *
   * The directory base name (component before the file base name).
   *
   * Since: 0.17.0
   */
  spec = g_param_spec_string("dir-name",
                             "Directory name",
                             "The directory base name",
                             info.dir_name().c_str(),
                             static_cast<GParamFlags>(G_PARAM_READABLE));
  g_object_class_install_property(gobject_class,
                                  PROP_FILE_INFO_DIR_NAME,
                                  spec);

  /**
   * GArrowFileInfo:extension:
   *
   * The file extension (excluding the dot).
   *
   * Since: 0.17.0
   */
  spec = g_param_spec_string("extension",
                             "Extension",
                             "The file extension",
                             info.extension().c_str(),
                             static_cast<GParamFlags>(G_PARAM_READABLE));
  g_object_class_install_property(gobject_class,
                                  PROP_FILE_INFO_EXTENSION,
                                  spec);

  /**
   * GArrowFileInfo:size:
   *
   * The size in bytes, if available
   * Only regular files are guaranteed to have a size.
   *
   * Since: 0.17.0
   */
  spec = g_param_spec_int64("size",
                            "Size",
                            "The size in bytes",
                            arrow::fs::kNoSize,
                            INT64_MAX,
                            info.size(),
                            static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_FILE_INFO_SIZE, spec);

  /**
   * GArrowFileInfo:mtime:
   *
   * The time of last modification, if available.
   *
   * Since: 0.17.0
   */
  spec = g_param_spec_int64("mtime",
                            "Last modified time",
                            "The time of last modification",
                            arrow::fs::kNoTime.time_since_epoch().count(),
                            INT64_MAX,
                            info.mtime().time_since_epoch().count(),
                            static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_FILE_INFO_MTIME, spec);
}

/**
 * garrow_file_info_new:
 *
 * Returns: A newly created #GArrowFileInfo.
 *
 * Since: 0.17.0
 */
GArrowFileInfo *
garrow_file_info_new(void)
{
  return GARROW_FILE_INFO(g_object_new(GARROW_TYPE_FILE_INFO, NULL));
}

/**
 * garrow_file_info_equal:
 * @file_info: A #GArrowFileInfo.
 * @other_file_info: A #GArrowFileInfo to be compared.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 *
 * Since: 0.17.0
 */
gboolean
garrow_file_info_equal(GArrowFileInfo *file_info,
                       GArrowFileInfo *other_file_info)
{
  const auto arrow_file_info = garrow_file_info_get_raw(file_info);
  const auto arrow_other_file_info = garrow_file_info_get_raw(other_file_info);
  return arrow_file_info->Equals(*arrow_other_file_info);
}

/**
 * garrow_file_info_is_file:
 * @file_info: A #GArrowFileInfo.
 *
 * Returns: %TRUE if the entry is a file, %FALSE otherwise.
 *
 * Since: 0.17.0
 */
gboolean
garrow_file_info_is_file(GArrowFileInfo *file_info)
{
  const auto arrow_file_info = garrow_file_info_get_raw(file_info);
  return arrow_file_info->IsFile();
}

/**
 * garrow_file_info_is_dir
 * @file_info: A #GArrowFileInfo.
 *
 * Returns: %TRUE if the entry is a directory, %FALSE otherwise.
 *
 * Since: 0.17.0
 */
gboolean
garrow_file_info_is_dir(GArrowFileInfo *file_info)
{
  const auto arrow_file_info = garrow_file_info_get_raw(file_info);
  return arrow_file_info->IsDirectory();
}

/**
 * garrow_file_info_to_string:
 * @file_info: A #GArrowFileInfo.
 *
 * Returns: The string representation of the file statistics.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 0.17.0
 */
gchar *
garrow_file_info_to_string(GArrowFileInfo *file_info)
{
  const auto arrow_file_info = garrow_file_info_get_raw(file_info);
  const auto string = arrow_file_info->ToString();
  return g_strdup(string.c_str());
}

/* arrow::fs::FileSelector */

typedef struct GArrowFileSelectorPrivate_ {
  arrow::fs::FileSelector file_selector;
} GArrowFileSelectorPrivate;

enum {
  PROP_FILE_SELECTOR_BASE_DIR = 1,
  PROP_FILE_SELECTOR_ALLOW_NOT_FOUND,
  PROP_FILE_SELECTOR_RECURSIVE,
  PROP_FILE_SELECTOR_MAX_RECURSION
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowFileSelector, garrow_file_selector, G_TYPE_OBJECT)

#define GARROW_FILE_SELECTOR_GET_PRIVATE(obj)         \
  static_cast<GArrowFileSelectorPrivate *>(           \
     garrow_file_selector_get_instance_private(       \
       GARROW_FILE_SELECTOR(obj)))

static void
garrow_file_selector_finalize(GObject *object)
{
  auto priv = GARROW_FILE_SELECTOR_GET_PRIVATE(object);

  priv->file_selector.~FileSelector();

  G_OBJECT_CLASS(garrow_file_selector_parent_class)->finalize(object);
}

static void
garrow_file_selector_set_property(GObject *object,
                                  guint prop_id,
                                  const GValue *value,
                                  GParamSpec *pspec)
{
  auto priv = GARROW_FILE_SELECTOR_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FILE_SELECTOR_BASE_DIR:
    priv->file_selector.base_dir = g_value_get_string(value);
    break;
  case PROP_FILE_SELECTOR_ALLOW_NOT_FOUND:
    priv->file_selector.allow_not_found = g_value_get_boolean(value);
    break;
  case PROP_FILE_SELECTOR_RECURSIVE:
    priv->file_selector.recursive = g_value_get_boolean(value);
    break;
  case PROP_FILE_SELECTOR_MAX_RECURSION:
    priv->file_selector.max_recursion = g_value_get_int(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_file_selector_get_property(GObject *object,
                                  guint prop_id,
                                  GValue *value,
                                  GParamSpec *pspec)
{
  auto priv = GARROW_FILE_SELECTOR_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FILE_SELECTOR_BASE_DIR:
    g_value_set_string(value, priv->file_selector.base_dir.c_str());
    break;
  case PROP_FILE_SELECTOR_ALLOW_NOT_FOUND:
    g_value_set_boolean(value, priv->file_selector.allow_not_found);
    break;
  case PROP_FILE_SELECTOR_RECURSIVE:
    g_value_set_boolean(value, priv->file_selector.recursive);
    break;
  case PROP_FILE_SELECTOR_MAX_RECURSION:
    g_value_set_int(value, priv->file_selector.max_recursion);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_file_selector_init(GArrowFileSelector *object)
{
  auto priv = GARROW_FILE_SELECTOR_GET_PRIVATE(object);
  new(&priv->file_selector) arrow::fs::FileSelector;
}

static void
garrow_file_selector_class_init(GArrowFileSelectorClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = garrow_file_selector_finalize;
  gobject_class->set_property = garrow_file_selector_set_property;
  gobject_class->get_property = garrow_file_selector_get_property;

  auto file_selector = arrow::fs::FileSelector();

  /**
   * GArrowFileSelector:base-dir:
   *
   * The directory in which to select files.
   * If the path exists but doesn't point to a directory, this should
   * be an error.
   *
   * Since: 0.17.0
   */
  spec = g_param_spec_string("base-dir",
                             "Base dir",
                             "The directory in which to select files",
                             file_selector.base_dir.c_str(),
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_FILE_SELECTOR_BASE_DIR,
                                  spec);

  /**
   * GArrowFileSelector:allow-not-found:
   *
   * The behavior if `base_dir` isn't found in the file system.
   * If false, an error is returned.  If true, an empty selection is returned.
   *
   * Since: 0.17.0
   */
  spec = g_param_spec_boolean("allow-not-found",
                              "Allow not found",
                              "The behavior if `base_dir` isn't found in the file system",
                              file_selector.allow_not_found,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_FILE_SELECTOR_ALLOW_NOT_FOUND,
                                  spec);

  /**
   * GArrowFileSelector:recursive:
   *
   * Whether to recurse into subdirectories.
   *
   * Since: 0.17.0
   */
  spec = g_param_spec_boolean("recursive",
                              "Recursive",
                              "Whether to recurse into subdirectories",
                              file_selector.recursive,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_FILE_SELECTOR_RECURSIVE,
                                  spec);

  /**
   * GArrowFileSelector:max-recursion:
   *
   * The maximum number of subdirectories to recurse into.
   *
   * Since: 0.17.0
   */
  spec = g_param_spec_int("max-recursion",
                          "Max recursion",
                          "The maximum number of subdirectories to recurse into",
                          0,
                          INT32_MAX,
                          file_selector.max_recursion,
                          static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_FILE_SELECTOR_MAX_RECURSION,
                                  spec);
}

/* arrow::fs::FileSystem */

typedef struct GArrowFileSystemPrivate_ {
  std::shared_ptr<arrow::fs::FileSystem> file_system;
} GArrowFileSystemPrivate;

enum {
  PROP_FILE_SYSTEM = 1
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GArrowFileSystem,
                                    garrow_file_system,
                                    G_TYPE_OBJECT)

#define GARROW_FILE_SYSTEM_GET_PRIVATE(obj)         \
  static_cast<GArrowFileSystemPrivate *>(           \
     garrow_file_system_get_instance_private(       \
       GARROW_FILE_SYSTEM(obj)))

static void
garrow_file_system_finalize(GObject *object)
{
  auto priv = GARROW_FILE_SYSTEM_GET_PRIVATE(object);

  priv->file_system.~shared_ptr();

  G_OBJECT_CLASS(garrow_file_system_parent_class)->finalize(object);
}

static void
garrow_file_system_set_property(GObject *object,
                                guint prop_id,
                                const GValue *value,
                                GParamSpec *pspec)
{
  auto priv = GARROW_FILE_SYSTEM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FILE_SYSTEM:
    priv->file_system =
      *static_cast<std::shared_ptr<arrow::fs::FileSystem> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_file_system_init(GArrowFileSystem *object)
{
  auto priv = GARROW_FILE_SYSTEM_GET_PRIVATE(object);
  new(&priv->file_system) std::shared_ptr<arrow::fs::FileSystem>;
}

static void
garrow_file_system_class_init(GArrowFileSystemClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_file_system_finalize;
  gobject_class->set_property = garrow_file_system_set_property;

  spec = g_param_spec_pointer("file-system",
                              "FileSystem",
                              "The raw std::shared<arrow::fs::FileSystem> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FILE_SYSTEM, spec);
}

/**
 * garrow_file_system_create:
 * @uri: An URI to specify file system with options. If you only have an
 *   absolute path, g_filename_to_uri() will help you.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * This is a factory function to create a specific #GArrowFileSystem
 * object.
 *
 * Returns: (nullable) (transfer full): The newly created file system
 *   that is an object of a subclass of #GArrowFileSystem.
 *
 * Since: 3.0.0
 */
GArrowFileSystem *
garrow_file_system_create(const gchar *uri, GError **error)
{
  auto arrow_file_system_result = arrow::fs::FileSystemFromUri(uri);
  if (garrow::check(error,
                    arrow_file_system_result,
                    "[file-system][create]")) {
    auto arrow_file_system = *arrow_file_system_result;
    return garrow_file_system_new_raw(&arrow_file_system);
  } else {
    return NULL;
  }
}

/**
 * garrow_file_system_get_type_name:
 * @file_system: A #GArrowFileSystem.
 *
 * Returns: The name of file system type.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 0.17.0
 */
gchar *
garrow_file_system_get_type_name(GArrowFileSystem *file_system)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  const auto &type_name = arrow_file_system->type_name();
  return g_strndup(type_name.data(), type_name.size());
}

/**
 * garrow_file_system_get_file_info:
 * @file_system: A #GArrowFileSystem.
 * @path: The path of the target.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Get information for the given target.
 *
 * Any symlink is automatically dereferenced, recursively.
 * A non-existing or unreachable file returns an OK status and has
 * a #GArrowFileType of value %GARROW_FILE_TYPE_NOT_FOUND.
 * An error status indicates a truly exceptional condition
 * (low-level I/O error, etc.).
 *
 * Returns: (nullable) (transfer full): A #GArrowFileInfo.
 *
 * Since: 0.17.0
 */
GArrowFileInfo *
garrow_file_system_get_file_info(GArrowFileSystem *file_system,
                                 const gchar *path,
                                 GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto arrow_result = arrow_file_system->GetFileInfo(path);
  if (garrow::check(error, arrow_result, "[file-system][get-file-info]")) {
    const auto &arrow_file_info = *arrow_result;
    return garrow_file_info_new_raw(arrow_file_info);
  } else {
    return NULL;
  }
}

static inline GList *
garrow_file_infos_new(arrow::Result<std::vector<arrow::fs::FileInfo>>&& arrow_result,
                      GError **error,
                      const gchar *context)
{
  if (garrow::check(error, arrow_result, context)) {
    auto arrow_file_infos = *arrow_result;
    GList *file_infos = NULL;
    for (auto arrow_file_info : arrow_file_infos) {
      auto file_info = garrow_file_info_new_raw(arrow_file_info);
      file_infos = g_list_prepend(file_infos, file_info);
    }
    return g_list_reverse(file_infos);
  } else {
    return NULL;
  }
}

/**
 * garrow_file_system_get_file_infos_paths:
 * @file_system: A #GArrowFileSystem.
 * @paths: (array length=n_paths): The paths of the targets.
 * @n_paths: The number of items in @paths.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Get information same as garrow_file_system_get_file_info()
 * for the given many targets at once.
 *
 * Returns: (element-type GArrowFileInfo) (transfer full):
 *   A list of #GArrowFileInfo.
 *
 * Since: 0.17.0
 */
GList *
garrow_file_system_get_file_infos_paths(GArrowFileSystem *file_system,
                                        const gchar **paths,
                                        gsize n_paths,
                                        GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  std::vector<std::string> arrow_paths;
  for (gsize i = 0; i < n_paths; ++i) {
    arrow_paths.push_back(paths[i]);
  }
  return garrow_file_infos_new(arrow_file_system->GetFileInfo(arrow_paths),
                               error,
                               "[file-system][get-file-infos][paths]");
}

/**
 * garrow_file_system_get_file_infos_selector:
 * @file_system: A #GArrowFileSystem.
 * @file_selector: A #GArrowFileSelector.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Get information same as garrow_file_system_get_file_info()
 * according to a selector.
 *
 * The selector's base directory will not be part of the results,
 * even if it exists.
 *
 * Returns: (element-type GArrowFileInfo) (transfer full):
 *   A list of #GArrowFileInfo.
 *
 * Since: 0.17.0
 */
GList *
garrow_file_system_get_file_infos_selector(GArrowFileSystem *file_system,
                                           GArrowFileSelector *file_selector,
                                           GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  const auto &arrow_file_selector =
    GARROW_FILE_SELECTOR_GET_PRIVATE(file_selector)->file_selector;
  return garrow_file_infos_new(arrow_file_system->GetFileInfo(arrow_file_selector),
                               error,
                               "[file-system][get-file-infos][selector]");
}

/**
 * garrow_file_system_create_dir:
 * @file_system: A #GArrowFileSystem.
 * @path: The paths of the directory.
 * @recursive: Whether creating directory recursively or not.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Create a directory and subdirectories.
 * This function succeeds if the directory already exists.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Since: 0.17.0
 */
gboolean
garrow_file_system_create_dir(GArrowFileSystem *file_system,
                              const gchar *path,
                              gboolean recursive,
                              GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto status = arrow_file_system->CreateDir(path, recursive);
  return garrow::check(error, status, "[file-system][create-dir]");
}

/**
 * garrow_file_system_delete_dir:
 * @file_system: A #GArrowFileSystem.
 * @path: The paths of the directory.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Delete a directory and its contents, recursively.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Since: 0.17.0
 */
gboolean
garrow_file_system_delete_dir(GArrowFileSystem *file_system,
                              const gchar *path,
                              GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto status = arrow_file_system->DeleteDir(path);
  return garrow::check(error, status, "[file-system][delete-dir]");
}

/**
 * garrow_file_system_delete_dir_contents:
 * @file_system: A #GArrowFileSystem.
 * @path: The paths of the directory.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Delete a directory's contents, recursively. Like
 * garrow_file_system_delete_dir(), but doesn't delete the directory
 * itself. Passing an empty path (`""`) will wipe the entire file
 * system tree.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Since: 0.17.0
 */
gboolean
garrow_file_system_delete_dir_contents(GArrowFileSystem *file_system,
                                       const gchar *path,
                                       GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto status = arrow_file_system->DeleteDirContents(path);
  return garrow::check(error, status, "[file-system][delete-dir-contents]");
}

/**
 * garrow_file_system_delete_file:
 * @file_system: A #GArrowFileSystem.
 * @path: The paths of the file to be delete.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Delete a file.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Since: 0.17.0
 */
gboolean
garrow_file_system_delete_file(GArrowFileSystem *file_system,
                               const gchar *path,
                               GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto status = arrow_file_system->DeleteFile(path);
  return garrow::check(error, status, "[file-system][delete-file]");
}

/**
 * garrow_file_system_delete_files:
 * @file_system: A #GArrowFileSystem.
 * @paths: (array length=n_paths):
 *   The paths of the files to be delete.
 * @n_paths: The number of items in @paths.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Delete many files.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Since: 0.17.0
 */
gboolean
garrow_file_system_delete_files(GArrowFileSystem *file_system,
                                const gchar **paths,
                                gsize n_paths,
                                GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  std::vector<std::string> arrow_paths;
  arrow_paths.reserve(n_paths);
  for (gsize i = 0; i < n_paths; ++i) {
    arrow_paths.emplace_back(paths[i]);
  }
  auto status = arrow_file_system->DeleteFiles(arrow_paths);
  return garrow::check(error, status, "[file-system][delete-files]");
}

/**
 * garrow_file_system_move:
 * @file_system: A #GArrowFileSystem.
 * @src: The path of the source file.
 * @dest: The path of the destination.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Move / rename a file or a directory.
 * If the destination exists:
 * - if it is a non-empty directory, an error is returned
 * - otherwise, if it has the same type as the source, it is replaced
 * - otherwise, behavior is unspecified (implementation-dependent).
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Since: 0.17.0
 */
gboolean
garrow_file_system_move(GArrowFileSystem *file_system,
                        const gchar *src,
                        const gchar *dest,
                        GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto status = arrow_file_system->Move(src, dest);
  return garrow::check(error, status, "[file-system][move]");
}

/**
 * garrow_file_system_copy_file:
 * @file_system: A #GArrowFileSystem.
 * @src: The path of the source file.
 * @dest: The path of the destination.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Copy a file.
 * If the destination exists and is a directory, an error is returned.
 * Otherwise, it is replaced.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Since: 0.17.0
 */
gboolean
garrow_file_system_copy_file(GArrowFileSystem *file_system,
                             const gchar *src,
                             const gchar *dest,
                             GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto status = arrow_file_system->CopyFile(src, dest);
  return garrow::check(error, status, "[file-system][copy-file]");
}

/**
 * garrow_file_system_open_input_stream:
 * @file_system: A #GArrowFileSystem.
 * @path: The path of the input stream.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Open an input stream for sequential reading.
 *
 * Returns: (nullable) (transfer full): A newly created
 *   #GArrowInputStream.
 *
 * Since: 0.17.0
 */
GArrowInputStream *
garrow_file_system_open_input_stream(GArrowFileSystem *file_system,
                                     const gchar *path,
                                     GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto arrow_input_stream = arrow_file_system->OpenInputStream(path);
  if (garrow::check(error,
                    arrow_input_stream,
                    "[file-system][open-input-stream]")) {
    return garrow_input_stream_new_raw(&(*arrow_input_stream));
  } else {
    return NULL;
  }
}

/**
 * garrow_file_system_open_input_file:
 * @file_system: A #GArrowFileSystem.
 * @path: The path of the input file.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Open an input file for random access reading.
 *
 * Returns: (nullable) (transfer full): A newly created
 *   #GArrowSeekableInputStream.
 *
 * Since: 0.17.0
 */
GArrowSeekableInputStream *
garrow_file_system_open_input_file(GArrowFileSystem *file_system,
                                   const gchar *path,
                                   GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto arrow_random_access_file = arrow_file_system->OpenInputFile(path);
  if (garrow::check(error,
                    arrow_random_access_file,
                    "[file-system][open-input-file]")) {
    return garrow_seekable_input_stream_new_raw(&(*arrow_random_access_file));
  } else {
    return NULL;
  }
}

/**
 * garrow_file_system_open_output_stream:
 * @file_system: A #GArrowFileSystem.
 * @path: The path of the output stream.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Open an output stream for sequential writing.
 * If the target already exists, the existing data is truncated.
 *
 * Returns: (nullable) (transfer full): A newly created
 *   #GArrowOutputStream.
 *
 * Since: 0.17.0
 */
GArrowOutputStream *
garrow_file_system_open_output_stream(GArrowFileSystem *file_system,
                                      const gchar *path,
                                      GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto arrow_output_stream = arrow_file_system->OpenOutputStream(path);
  if (garrow::check(error,
                    arrow_output_stream,
                    "[file-system][open-output-stream]")) {
    return garrow_output_stream_new_raw(&(*arrow_output_stream));
  } else {
    return NULL;
  }
}

/**
 * garrow_file_system_open_append_stream:
 * @file_system: A #GArrowFileSystem.
 * @path: The path of the output stream.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Open an output stream for appending.
 * If the target doesn't exist, a new empty file is created.
 *
 * Returns: (nullable) (transfer full): A newly created #GArrowOutputStream
 *   for appending.
 *
 * Since: 0.17.0
 */
GArrowOutputStream *
garrow_file_system_open_append_stream(GArrowFileSystem *file_system,
                                      const gchar *path,
                                      GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto arrow_output_stream = arrow_file_system->OpenAppendStream(path);
  if (garrow::check(error,
                    arrow_output_stream,
                    "[file-system][open-append-stream]")) {
    return garrow_output_stream_new_raw(&(*arrow_output_stream));
  } else {
    return NULL;
  }
}

/* arrow::fs::SubTreeFileSystem */

typedef struct GArrowSubTreeFileSystemPrivate_ {
  GArrowFileSystem *base_file_system;
} GArrowSubTreeFileSystemPrivate;

enum {
  PROP_BASE_FILE_SYSTEM = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowSubTreeFileSystem,
                           garrow_sub_tree_file_system,
                           GARROW_TYPE_FILE_SYSTEM)

#define GARROW_SUB_TREE_FILE_SYSTEM_GET_PRIVATE(object) \
  static_cast<GArrowSubTreeFileSystemPrivate *>(        \
    garrow_sub_tree_file_system_get_instance_private(   \
      GARROW_SUB_TREE_FILE_SYSTEM(object)))

static void
garrow_sub_tree_file_system_dispose(GObject *object)
{
  auto priv = GARROW_SUB_TREE_FILE_SYSTEM_GET_PRIVATE(object);

  if (priv->base_file_system) {
    g_object_unref(priv->base_file_system);
    priv->base_file_system = NULL;
  }

  G_OBJECT_CLASS(garrow_sub_tree_file_system_parent_class)->dispose(object);
}

static void
garrow_sub_tree_file_system_set_property(GObject *object,
                                         guint prop_id,
                                         const GValue *value,
                                         GParamSpec *pspec)
{
  auto priv = GARROW_SUB_TREE_FILE_SYSTEM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_BASE_FILE_SYSTEM:
    priv->base_file_system = GARROW_FILE_SYSTEM(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_sub_tree_file_system_get_property(GObject *object,
                                         guint prop_id,
                                         GValue *value,
                                         GParamSpec *pspec)
{
  auto priv = GARROW_SUB_TREE_FILE_SYSTEM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_BASE_FILE_SYSTEM:
    g_value_set_object(value, priv->base_file_system);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_sub_tree_file_system_init(GArrowSubTreeFileSystem *file_system)
{
}

static void
garrow_sub_tree_file_system_class_init(GArrowSubTreeFileSystemClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->dispose      = garrow_sub_tree_file_system_dispose;
  gobject_class->set_property = garrow_sub_tree_file_system_set_property;
  gobject_class->get_property = garrow_sub_tree_file_system_get_property;

  GParamSpec *spec;
  spec = g_param_spec_object("base-file-system",
                             "Base file system",
                             "The base GArrowFileSystem",
                             GARROW_TYPE_FILE_SYSTEM,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_BASE_FILE_SYSTEM, spec);
}

/**
 * garrow_sub_tree_file_system_new:
 * @base_path: A base path of the sub tree file system.
 * @base_file_system: A #GArrowFileSystem as the base file system.
 *
 * Returns: (transfer full): A newly created #GArrowSubTreeFileSystem.
 *
 * Since: 0.17.0
 */
GArrowSubTreeFileSystem *
garrow_sub_tree_file_system_new(const gchar *base_path,
                                GArrowFileSystem *base_file_system)
{
  auto arrow_base_file_system = garrow_file_system_get_raw(base_file_system);
  auto arrow_sub_tree_file_system =
    std::static_pointer_cast<arrow::fs::FileSystem>(
      std::make_shared<arrow::fs::SubTreeFileSystem>(base_path,
                                                     arrow_base_file_system));
  return garrow_sub_tree_file_system_new_raw(&arrow_sub_tree_file_system,
                                             base_file_system);
}

/* arrow::fs::SlowFileSystem */

typedef struct GArrowSlowFileSystemPrivate_ {
  GArrowFileSystem *base_file_system;
} GArrowSlowFileSystemPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GArrowSlowFileSystem,
                           garrow_slow_file_system,
                           GARROW_TYPE_FILE_SYSTEM)

#define GARROW_SLOW_FILE_SYSTEM_GET_PRIVATE(object)     \
  static_cast<GArrowSlowFileSystemPrivate *>(           \
    garrow_slow_file_system_get_instance_private(       \
      GARROW_SLOW_FILE_SYSTEM(object)))

static void
garrow_slow_file_system_dispose(GObject *object)
{
  auto priv = GARROW_SLOW_FILE_SYSTEM_GET_PRIVATE(object);

  if (priv->base_file_system) {
    g_object_unref(priv->base_file_system);
    priv->base_file_system = NULL;
  }

  G_OBJECT_CLASS(garrow_slow_file_system_parent_class)->dispose(object);
}

static void
garrow_slow_file_system_set_property(GObject *object,
                                     guint prop_id,
                                     const GValue *value,
                                     GParamSpec *pspec)
{
  auto priv = GARROW_SLOW_FILE_SYSTEM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_BASE_FILE_SYSTEM:
    priv->base_file_system = GARROW_FILE_SYSTEM(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_slow_file_system_get_property(GObject *object,
                                     guint prop_id,
                                     GValue *value,
                                     GParamSpec *pspec)
{
  auto priv = GARROW_SLOW_FILE_SYSTEM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_BASE_FILE_SYSTEM:
    g_value_set_object(value, priv->base_file_system);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_slow_file_system_init(GArrowSlowFileSystem *file_system)
{
}

static void
garrow_slow_file_system_class_init(GArrowSlowFileSystemClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->dispose      = garrow_slow_file_system_dispose;
  gobject_class->set_property = garrow_slow_file_system_set_property;
  gobject_class->get_property = garrow_slow_file_system_get_property;

  GParamSpec *spec;
  spec = g_param_spec_object("base-file-system",
                             "Base file system",
                             "The base GArrowFileSystem",
                             GARROW_TYPE_FILE_SYSTEM,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_BASE_FILE_SYSTEM, spec);
}

/**
 * garrow_slow_file_system_new_average_latency:
 * @base_file_system: A #GArrowFileSystem as the base file system.
 * @average_latency: The average value of the latency.
 *
 * The latency is normally distributed with a standard deviation of
 * @average_latency * 0.1.
 *
 * The random seed is given by the default random device.
 *
 * Returns: (transfer full): A newly created #GArrowSlowFileSystem.
 *
 * Since: 0.17.0
 */
GArrowSlowFileSystem *
garrow_slow_file_system_new_average_latency(GArrowFileSystem *base_file_system,
                                            gdouble average_latency)
{
  auto arrow_base_file_system = garrow_file_system_get_raw(base_file_system);
  auto arrow_slow_file_system =
    std::static_pointer_cast<arrow::fs::FileSystem>(
      std::make_shared<arrow::fs::SlowFileSystem>(arrow_base_file_system,
                                                  average_latency));
  return garrow_slow_file_system_new_raw(&arrow_slow_file_system,
                                         base_file_system);
}

/**
 * garrow_slow_file_system_new_average_latency_and_seed:
 * @base_file_system: A #GArrowFileSystem as the base file system.
 * @average_latency: The average value of the latency.
 * @seed: A random seed.
 *
 * The latency is normally distributed with a standard deviation of
 * @average_latency * 0.1.
 *
 * Returns: (transfer full): A newly created #GArrowSlowFileSystem.
 *
 * Since: 0.17.0
 */
GArrowSlowFileSystem *
garrow_slow_file_system_new_average_latency_and_seed(GArrowFileSystem *base_file_system,
                                                     gdouble average_latency,
                                                     gint32 seed)
{
  auto arrow_base_file_system = garrow_file_system_get_raw(base_file_system);
  auto arrow_slow_file_system =
    std::static_pointer_cast<arrow::fs::FileSystem>(
      std::make_shared<arrow::fs::SlowFileSystem>(arrow_base_file_system,
                                                  average_latency,
                                                  seed));
  return garrow_slow_file_system_new_raw(&arrow_slow_file_system,
                                         base_file_system);
}


G_DEFINE_TYPE(GArrowMockFileSystem,
              garrow_mock_file_system,
              GARROW_TYPE_FILE_SYSTEM)

static void
garrow_mock_file_system_init(GArrowMockFileSystem *file_system)
{
}

static void
garrow_mock_file_system_class_init(GArrowMockFileSystemClass *klass)
{
}


G_DEFINE_TYPE(GArrowHDFSFileSystem,
              garrow_hdfs_file_system,
              GARROW_TYPE_FILE_SYSTEM)

static void
garrow_hdfs_file_system_init(GArrowHDFSFileSystem *file_system)
{
}

static void
garrow_hdfs_file_system_class_init(GArrowHDFSFileSystemClass *klass)
{
}


#ifndef ARROW_S3
namespace arrow {
  namespace fs {
    enum class S3LogLevel : int8_t { Off, Fatal, Error, Warn, Info, Debug, Trace };

    struct ARROW_EXPORT S3GlobalOptions {
      S3LogLevel log_level;
    };
  }
}
#endif

typedef struct GArrowS3GlobalOptionsPrivate_ {
  arrow::fs::S3GlobalOptions options;
} GArrowS3GlobalOptionsPrivate;

enum {
  PROP_S3_GLOBAL_OPTIONS_LOG_LEVEL = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowS3GlobalOptions,
                           garrow_s3_global_options,
                           G_TYPE_OBJECT)

#define GARROW_S3_GLOBAL_OPTIONS_GET_PRIVATE(object)    \
  static_cast<GArrowS3GlobalOptionsPrivate *>(          \
    garrow_s3_global_options_get_instance_private(      \
      GARROW_S3_GLOBAL_OPTIONS(object)))

static void
garrow_s3_global_options_finalize(GObject *object)
{
  auto priv = GARROW_S3_GLOBAL_OPTIONS_GET_PRIVATE(object);
  priv->options.~S3GlobalOptions();
  G_OBJECT_CLASS(garrow_s3_global_options_parent_class)->finalize(object);
}

static void
garrow_s3_global_options_set_property(GObject *object,
                                      guint prop_id,
                                      const GValue *value,
                                      GParamSpec *pspec)
{
#ifdef ARROW_S3
  auto arrow_options =
    garrow_s3_global_options_get_raw(GARROW_S3_GLOBAL_OPTIONS(object));

  switch (prop_id) {
  case PROP_S3_GLOBAL_OPTIONS_LOG_LEVEL:
    arrow_options->log_level =
      static_cast<arrow::fs::S3LogLevel>(g_value_get_enum(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
#else
  G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
#endif
}

static void
garrow_s3_global_options_get_property(GObject *object,
                                      guint prop_id,
                                      GValue *value,
                                      GParamSpec *pspec)
{
#ifdef ARROW_S3
  auto arrow_options =
    garrow_s3_global_options_get_raw(GARROW_S3_GLOBAL_OPTIONS(object));

  switch (prop_id) {
  case PROP_S3_GLOBAL_OPTIONS_LOG_LEVEL:
    g_value_set_enum(value,
                     static_cast<GArrowS3LogLevel>(arrow_options->log_level));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
#else
  G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
#endif
}

static void
garrow_s3_global_options_init(GArrowS3GlobalOptions *object)
{
  auto priv = GARROW_S3_GLOBAL_OPTIONS_GET_PRIVATE(object);
  new(&priv->options) arrow::fs::S3GlobalOptions;
}

static void
garrow_s3_global_options_class_init(GArrowS3GlobalOptionsClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_s3_global_options_finalize;
  gobject_class->set_property = garrow_s3_global_options_set_property;
  gobject_class->get_property = garrow_s3_global_options_get_property;

  /**
   * GArrowS3GlobalOptions:log-level:
   *
   * The log level of S3 APIs.
   *
   * Since: 7.0.0
   */
  spec = g_param_spec_enum("log-level",
                           "Log level",
                           "The log level of S3 APIs",
                           GARROW_TYPE_S3_LOG_LEVEL,
                           GARROW_S3_LOG_LEVEL_FATAL,
                           static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                    G_PARAM_CONSTRUCT));
  g_object_class_install_property(gobject_class,
                                  PROP_S3_GLOBAL_OPTIONS_LOG_LEVEL,
                                  spec);
}

/**
 * garrow_s3_global_options_new:
 *
 * Returns: A newly created #GArrowS3GlobalOptions.
 *
 * Since: 7.0.0
 */
GArrowS3GlobalOptions *
garrow_s3_global_options_new(void)
{
  return GARROW_S3_GLOBAL_OPTIONS(
    g_object_new(GARROW_TYPE_S3_GLOBAL_OPTIONS, NULL));
}


/**
 * garrow_s3_is_enabled:
 *
 * Returns: %TRUE if Apache Arrow C++ is built with S3 support, %FALSE
 *   otherwise.
 *
 * Since: 7.0.0
 */
gboolean
garrow_s3_is_enabled(void)
{
#ifdef ARROW_S3
  return TRUE;
#else
  return FALSE;
#endif
}

/**
 * garrow_s3_initialize:
 * @options: (nullable): Options to initialize the S3 APIs.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Normally, you don't need to call this function because the S3 APIs
 * are initialized with the default options automatically. If you want
 * to call this function, you must call this function before you use
 * any #GArrowS3FileSystem related APIs.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 7.0.0
 */
gboolean
garrow_s3_initialize(GArrowS3GlobalOptions *options,
                     GError **error)
{
#ifdef ARROW_S3
  auto arrow_options = garrow_s3_global_options_get_raw(options);
  return garrow::check(error,
                       arrow::fs::InitializeS3(*arrow_options),
                       "[s3][initialize]");
#else
  return garrow::check(error,
                       arrow::Status::NotImplemented(
                         "Apache Arrow C++ isn't built with S3 support"),
                       "[s3][initialize]");
#endif
}

/**
 * garrow_s3_finalize:
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Finalize the S3 APIs.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 7.0.0
 */
gboolean
garrow_s3_finalize(GError **error)
{
#ifdef ARROW_S3
  return garrow::check(error,
                       arrow::fs::FinalizeS3(),
                       "[s3][finalize]");
#else
  return garrow::check(error,
                       arrow::Status::NotImplemented(
                         "Apache Arrow C++ isn't built with S3 support"),
                       "[s3][initialize]");
#endif
}


G_DEFINE_TYPE(GArrowS3FileSystem,
              garrow_s3_file_system,
              GARROW_TYPE_FILE_SYSTEM)

static void
garrow_s3_file_system_init(GArrowS3FileSystem *file_system)
{
}

static void
garrow_s3_file_system_class_init(GArrowS3FileSystemClass *klass)
{
}


G_DEFINE_TYPE(GArrowGCSFileSystem,
              garrow_gcs_file_system,
              GARROW_TYPE_FILE_SYSTEM)

static void
garrow_gcs_file_system_init(GArrowGCSFileSystem *file_system)
{
}

static void
garrow_gcs_file_system_class_init(GArrowGCSFileSystemClass *klass)
{
}


G_END_DECLS

GArrowFileInfo *
garrow_file_info_new_raw(const arrow::fs::FileInfo &arrow_file_info)
{
  auto file_info = garrow_file_info_new();
  GARROW_FILE_INFO_GET_PRIVATE(file_info)->file_info = arrow_file_info;
  return file_info;
}

arrow::fs::FileInfo *
garrow_file_info_get_raw(GArrowFileInfo *file_info)
{
  auto priv = GARROW_FILE_INFO_GET_PRIVATE(file_info);
  return &(priv->file_info);
}

GArrowFileSystem *
garrow_file_system_new_raw(
  std::shared_ptr<arrow::fs::FileSystem> *arrow_file_system)
{
  const auto &type_name = (*arrow_file_system)->type_name();

  GType file_system_type = GARROW_TYPE_FILE_SYSTEM;
  if (type_name == "local") {
    file_system_type = GARROW_TYPE_LOCAL_FILE_SYSTEM;
  } else if (type_name == "hdfs") {
    file_system_type = GARROW_TYPE_HDFS_FILE_SYSTEM;
  } else if (type_name == "s3") {
    file_system_type = GARROW_TYPE_S3_FILE_SYSTEM;
  } else if (type_name == "gcs") {
    file_system_type = GARROW_TYPE_GCS_FILE_SYSTEM;
  } else if (type_name == "mock") {
    file_system_type = GARROW_TYPE_MOCK_FILE_SYSTEM;
  }

  return GARROW_FILE_SYSTEM(g_object_new(file_system_type,
                                         "file-system", arrow_file_system,
                                         NULL));
}

std::shared_ptr<arrow::fs::FileSystem>
garrow_file_system_get_raw(GArrowFileSystem *file_system)
{
  auto priv = GARROW_FILE_SYSTEM_GET_PRIVATE(file_system);
  return priv->file_system;
}

GArrowSubTreeFileSystem *
garrow_sub_tree_file_system_new_raw(
  std::shared_ptr<arrow::fs::FileSystem> *arrow_file_system,
  GArrowFileSystem *base_file_system)
{
  return GARROW_SUB_TREE_FILE_SYSTEM(
    g_object_new(GARROW_TYPE_SUB_TREE_FILE_SYSTEM,
                 "file-system", arrow_file_system,
                 "base-file-system", base_file_system,
                 NULL));
}

GArrowSlowFileSystem *
garrow_slow_file_system_new_raw(
  std::shared_ptr<arrow::fs::FileSystem> *arrow_file_system,
  GArrowFileSystem *base_file_system)
{
  return GARROW_SLOW_FILE_SYSTEM(
    g_object_new(GARROW_TYPE_SLOW_FILE_SYSTEM,
                 "file-system", arrow_file_system,
                 "base-file-system", base_file_system,
                 NULL));
}

#ifdef ARROW_S3
arrow::fs::S3GlobalOptions *
garrow_s3_global_options_get_raw(GArrowS3GlobalOptions *options)
{
  auto priv = GARROW_S3_GLOBAL_OPTIONS_GET_PRIVATE(options);
  return &(priv->options);
}
#endif
