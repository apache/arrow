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
#include <arrow-glib/output-stream.hpp>

G_BEGIN_DECLS

/**
 * SECTION: file-system
 * @section_id: file-system-classes
 * @title: File system classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowFileStats is a class for a stats of file system entry.
 *
 * #GArrowFileSelector is a class for a selector for filesystem APIs.
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
 * #GArrowLocalFileSystem is a class for an implementation of a file system
 * that accesses files on the local machine.
 */

/* arrow::fs::FileStats */

typedef struct GArrowFileStatsPrivate_ {
  arrow::fs::FileStats file_stats;
} GArrowFileStatsPrivate;

enum {
  PROP_FILE_STATS_TYPE = 1,
  PROP_FILE_STATS_PATH,
  PROP_FILE_STATS_BASE_NAME,
  PROP_FILE_STATS_DIR_NAME,
  PROP_FILE_STATS_EXTENSION,
  PROP_FILE_STATS_SIZE,
  PROP_FILE_STATS_MTIME,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowFileStats, garrow_file_stats, G_TYPE_OBJECT)

#define GARROW_FILE_STATS_GET_PRIVATE(obj)         \
  static_cast<GArrowFileStatsPrivate *>(           \
     garrow_file_stats_get_instance_private(       \
       GARROW_FILE_STATS(obj)))

static void
garrow_file_stats_set_property(GObject *object,
                               guint prop_id,
                               const GValue *value,
                               GParamSpec *pspec)
{
  auto priv = GARROW_FILE_STATS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FILE_STATS_TYPE:
    {
      auto arrow_file_type = static_cast<arrow::fs::FileType>(g_value_get_enum(value));
      priv->file_stats.set_type(arrow_file_type);
    }
    break;
  case PROP_FILE_STATS_PATH:
    priv->file_stats.set_path(g_value_get_string(value));
    break;
  case PROP_FILE_STATS_SIZE:
    priv->file_stats.set_size(g_value_get_int64(value));
    break;
  case PROP_FILE_STATS_MTIME:
    {
      const gint64 mtime = g_value_get_int64(value);
      const arrow::fs::TimePoint::duration duration(mtime);
      priv->file_stats.set_mtime(arrow::fs::TimePoint(duration));
    }
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_file_stats_get_property(GObject *object,
                               guint prop_id,
                               GValue *value,
                               GParamSpec *pspec)
{
  auto priv = GARROW_FILE_STATS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FILE_STATS_TYPE:
    {
      const auto arrow_file_type = priv->file_stats.type();
      const auto file_type = static_cast<GArrowFileType>(arrow_file_type);
      g_value_set_enum(value, file_type);
    }
    break;
  case PROP_FILE_STATS_PATH:
    g_value_set_string(value, priv->file_stats.path().c_str());
    break;
  case PROP_FILE_STATS_BASE_NAME:
    g_value_set_string(value, priv->file_stats.base_name().c_str());
    break;
  case PROP_FILE_STATS_DIR_NAME:
    g_value_set_string(value, priv->file_stats.dir_name().c_str());
    break;
  case PROP_FILE_STATS_EXTENSION:
    g_value_set_string(value, priv->file_stats.extension().c_str());
    break;
  case PROP_FILE_STATS_SIZE:
    g_value_set_int64(value, priv->file_stats.size());
    break;
  case PROP_FILE_STATS_MTIME:
    {
      const auto arrow_mtime = priv->file_stats.mtime();
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
garrow_file_stats_finalize(GObject *object)
{
  auto priv = GARROW_FILE_STATS_GET_PRIVATE(object);

  priv->file_stats.~FileStats();

  G_OBJECT_CLASS(garrow_file_stats_parent_class)->finalize(object);
}

static void
garrow_file_stats_init(GArrowFileStats *object)
{
  auto priv = GARROW_FILE_STATS_GET_PRIVATE(object);
  new(&priv->file_stats) arrow::fs::FileStats;
}

static void
garrow_file_stats_class_init(GArrowFileStatsClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_file_stats_finalize;
  gobject_class->set_property = garrow_file_stats_set_property;
  gobject_class->get_property = garrow_file_stats_get_property;

  /**
   * GArrowFileStats:type:
   *
   * The file type.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_enum("type",
                           "Type",
                           "The file type.",
                           GARROW_TYPE_FILE_TYPE,
                           GARROW_FILE_TYPE_UNKNOWN,
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_FILE_STATS_TYPE, spec);

  /**
   * GArrowFileStats:path:
   *
   * The full file path in the filesystem.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_string("path",
                             "Path",
                             "The full file path.",
                             "",
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_FILE_STATS_PATH, spec);

  /**
   * GArrowFileStats:base-name:
   *
   * The file base name (component after the last directory separator).
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_string("base-name",
                             "Base name",
                             "The file base name.",
                             "",
                             static_cast<GParamFlags>(G_PARAM_READABLE));
  g_object_class_install_property(gobject_class, PROP_FILE_STATS_BASE_NAME, spec);

  /**
   * GArrowFileStats:dir-name:
   *
   * The directory base name (component before the file base name).
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_string("dir-name",
                             "Dir name",
                             "The directory base name.",
                             "",
                             static_cast<GParamFlags>(G_PARAM_READABLE));
  g_object_class_install_property(gobject_class, PROP_FILE_STATS_DIR_NAME, spec);

  /**
   * GArrowFileStats:extension:
   *
   * The file extension (excluding the dot).
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_string("extension",
                             "Extension",
                             "The file extension.",
                             "",
                             static_cast<GParamFlags>(G_PARAM_READABLE));
  g_object_class_install_property(gobject_class, PROP_FILE_STATS_EXTENSION, spec);

  /**
   * GArrowFileStats:size:
   *
   * The size in bytes, if available
   * Only regular files are guaranteed to have a size.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_int64("size",
                            "Size",
                            "The size in bytes.",
                            -1,
                            INT64_MAX,
                            -1,
                            static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_FILE_STATS_SIZE, spec);

  /**
   * GArrowFileStats:mtime:
   *
   * The time of last modification, if available.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_int64("mtime",
                            "Mtime",
                            "The time of last modification",
                            -1,
                            INT64_MAX,
                            -1,
                            static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_FILE_STATS_MTIME, spec);
}

/**
 * garrow_file_stats_new:
 *
 * Returns: A newly created #GArrowFileStats.
 *
 * Since: 1.0.0
 */
GArrowFileStats *
garrow_file_stats_new(void)
{
  auto file_stats = GARROW_FILE_STATS(g_object_new(GARROW_TYPE_FILE_STATS, NULL));
  return file_stats;
}

/**
 * garrow_file_stats_equal:
 * @file_stats: A #GArrowFileStats.
 * @other_file_stats: A #GArrowFileStats to be compared.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 *
 * Since: 1.0.0
 */
gboolean
garrow_file_stats_equal(GArrowFileStats *file_stats,
                        GArrowFileStats *other_file_stats)
{
  const auto& arrow_file_stats =
    GARROW_FILE_STATS_GET_PRIVATE(file_stats)->file_stats;
  const auto& arrow_other_file_stats =
    GARROW_FILE_STATS_GET_PRIVATE(other_file_stats)->file_stats;
  return arrow_file_stats.Equals(arrow_other_file_stats);
}

gboolean
garrow_file_stats_is_file(GArrowFileStats *file_stats)
{
  const auto& arrow_file_stats =
    GARROW_FILE_STATS_GET_PRIVATE(file_stats)->file_stats;
  return arrow_file_stats.IsFile();
}

gboolean
garrow_file_stats_is_directory(GArrowFileStats *file_stats)
{
  const auto& arrow_file_stats =
    GARROW_FILE_STATS_GET_PRIVATE(file_stats)->file_stats;
  return arrow_file_stats.IsDirectory();
}

/* arrow::fs::FileSelector */

typedef struct GArrowFileSelectorPrivate_ {
  arrow::fs::FileSelector file_selector;
} GArrowFileSelectorPrivate;

enum {
  PROP_FILE_SELECTOR_BASE_DIR = 1,
  PROP_FILE_SELECTOR_ALLOW_NON_EXISTENT,
  PROP_FILE_SELECTOR_RECURSIVE,
  PROP_FILE_SELECTOR_MAX_RECURSION
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowFileSelector, garrow_file_selector, G_TYPE_OBJECT)

#define GARROW_FILE_SELECTOR_GET_PRIVATE(obj)         \
  static_cast<GArrowFileSelectorPrivate *>(           \
     garrow_file_selector_get_instance_private(       \
       GARROW_FILE_SELECTOR(obj)))

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
  case PROP_FILE_SELECTOR_ALLOW_NON_EXISTENT:
    priv->file_selector.allow_non_existent = g_value_get_boolean(value);
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
  case PROP_FILE_SELECTOR_ALLOW_NON_EXISTENT:
    g_value_set_boolean(value, priv->file_selector.allow_non_existent);
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
garrow_file_selector_finalize(GObject *object)
{
  auto priv = GARROW_FILE_SELECTOR_GET_PRIVATE(object);

  priv->file_selector.~FileSelector();

  G_OBJECT_CLASS(garrow_file_selector_parent_class)->finalize(object);
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
   * If the path exists but doesn't point to a directory, this should be an error.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_string("base-dir",
                             "Base dir",
                             "The directory in which to select files.",
                             file_selector.base_dir.c_str(),
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_FILE_SELECTOR_BASE_DIR, spec);

  /**
   * GArrowFileSelector:allow-non-existent:
   *
   * The behavior if `base_dir` doesn't exist in the filesystem.
   * If false, an error is returned.  If true, an empty selection is returned.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_boolean("allow-non-existent",
                              "Allow non existent",
                              "The behavior if `base_dir` doesn't exist in the filesystem.",
                              file_selector.allow_non_existent,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_FILE_SELECTOR_ALLOW_NON_EXISTENT, spec);

  /**
   * GArrowFileSelector:recursive:
   *
   * Whether to recurse into subdirectories.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_boolean("recursive",
                              "Recursive",
                              "Whether to recurse into subdirectories.",
                              file_selector.recursive,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_FILE_SELECTOR_RECURSIVE, spec);

  /**
   * GArrowFileSelector:max-recursion:
   *
   * The maximum number of subdirectories to recurse into.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_int("max-recursion",
                          "Max recursion",
                          "The maximum number of subdirectories to recurse into.",
                          0,
                          INT32_MAX,
                          file_selector.max_recursion,
                          static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_FILE_SELECTOR_MAX_RECURSION, spec);
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

  priv->file_system = nullptr;

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
}

static void
garrow_file_system_class_init(GArrowFileSystemClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_file_system_finalize;
  gobject_class->set_property = garrow_file_system_set_property;

  spec = g_param_spec_pointer("file_system",
                              "FileSystem",
                              "The raw std::shared<arrow::fs::FileSystem> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FILE_SYSTEM, spec);
}

/**
 * garrow_file_system_get_type_name:
 * @file_system: A #GArrowFileSystem.
 *
 * Returns: The name of file system type.
 *
 * Since: 1.0.0
 */
const gchar *
garrow_file_system_get_type_name(GArrowFileSystem *file_system)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  return arrow_file_system->type_name().c_str();
}

/**
 * garrow_file_system_get_target_stats_path:
 * @file_system: A #GArrowFileSystem.
 * @path: The path of the target.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Get statistics for the given target.
 *
 * Any symlink is automatically dereferenced, recursively.
 * A non-existing or unreachable file returns an OK status and has
 * a #GArrowFileType of value %GARROW_FILE_TYPE_NON_EXISTENT.
 * An error status indicates a truly exceptional condition
 * (low-level I/O error, etc.).
 *
 * Returns: (nullable) (transfer full): A #GArrowFileStats
 *
 * Since: 1.0.0
 */
GArrowFileStats *
garrow_file_system_get_target_stats_path(GArrowFileSystem *file_system,
                                         const gchar *path,
                                         GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto arrow_result = arrow_file_system->GetTargetStats(path);
  if (garrow::check(error, arrow_result, "[filesystem][get-target-stats]")) {
    const auto& arrow_file_stats = arrow_result.ValueOrDie();
    return garrow_file_stats_new_raw(arrow_file_stats);
  } else {
    return NULL;
  }
}

static inline GList *
garrow_file_stats_list_from_result(arrow::Result<std::vector<arrow::fs::FileStats>>&& arrow_result,
                                   GError **error,
                                   const gchar *context)
{
  if (garrow::check(error, arrow_result, context)) {
    auto arrow_file_stats_vector = arrow_result.ValueOrDie();
    GList *file_stats_list = NULL;
    for (auto arrow_file_stats : arrow_file_stats_vector) {
      auto file_stats = garrow_file_stats_new_raw(arrow_file_stats);
      file_stats_list = g_list_prepend(file_stats_list, file_stats);
    }
    return g_list_reverse(file_stats_list);
  } else {
    return NULL;
  }
}

/**
 * garrow_file_system_get_target_stats_paths:
 * @file_system: A #GArrowFileSystem.
 * @paths: The paths of the targets.
 * @n_paths: The number of items in @paths.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Get statistics same as garrow_file_system_get_target_stats_path()
 * for the given many targets at once.
 *
 * Returns: (element-type GArrowFileStats) (transfer full):
 *   A list of #GArrowFileStats
 *
 * Since: 1.0.0
 */
GList *
garrow_file_system_get_target_stats_paths(GArrowFileSystem *file_system,
                                          const gchar **paths,
                                          gsize n_paths,
                                          GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  std::vector<std::string> arrow_paths;
  for (gsize i = 0; i < n_paths; ++i) {
    arrow_paths.push_back(paths[i]);
  }
  return garrow_file_stats_list_from_result(arrow_file_system->GetTargetStats(arrow_paths),
                                            error, "[filesystem][get-target-stats-list]");
}

/**
 * garrow_file_system_get_target_stats_selector:
 * @file_system: A #GArrowFileSystem.
 * @file_selector: A #GArrowFileSelector.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Get statistics same as garrow_file_system_get_target_stats_path()
 * according to a selector.
 *
 * The selector's base directory will not be part of the results,
 * even if it exists.
 *
 * Returns: (element-type GArrowFileStats) (transfer full):
 *   A list of #GArrowFileStats
 *
 * Since: 1.0.0
 */
GList *
garrow_file_system_get_target_stats_selector(GArrowFileSystem *file_system,
                                             GArrowFileSelector *file_selector,
                                             GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  const auto& arrow_file_selector =
    GARROW_FILE_SELECTOR_GET_PRIVATE(file_selector)->file_selector;
  return garrow_file_stats_list_from_result(arrow_file_system->GetTargetStats(arrow_file_selector),
                                            error, "[filesystem][get-target-stats-list]");
}

/**
 * garrow_file_system_create_dir:
 * @file_system: A #GArrowFileSystem.
 * @path: The paths of the directory.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Create a directory and subdirectories.
 * This function succeeds if the directory already exists.
 *
 * Since: 1.0.0
 */
gboolean
garrow_file_system_create_dir(GArrowFileSystem *file_system,
                              const gchar *path,
                              gboolean recursive,
                              GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto status = arrow_file_system->CreateDir(path, recursive);
  return garrow::check(error, status, "[filesystem][create-dir]");
}

/**
 * garrow_file_system_delete_dir:
 * @file_system: A #GArrowFileSystem.
 * @path: The paths of the directory.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Delete a directory and its contents, recursively.
 *
 * Since: 1.0.0
 */
gboolean
garrow_file_system_delete_dir(GArrowFileSystem *file_system,
                              const gchar *path,
                              GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto status = arrow_file_system->DeleteDir(path);
  return garrow::check(error, status, "[filesystem][delete-dir]");
}

/**
 * garrow_file_system_delete_dir_contents:
 * @file_system: A #GArrowFileSystem.
 * @path: The paths of the directory.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Delete a directory's cooontents, recursively.
 * Like garrow_file_system_delete_dir, but doesn't delete the directory itself.
 * Passing an empty path ("") will wipe the entire filesystem tree.
 *
 * Since: 1.0.0
 */
gboolean
garrow_file_system_delete_dir_contents(GArrowFileSystem *file_system,
                                       const gchar *path,
                                       GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto status = arrow_file_system->DeleteDirContents(path);
  return garrow::check(error, status, "[filesystem][delete-dir-contents]");
}

/**
 * garrow_file_system_delete_file:
 * @file_system: A #GArrowFileSystem.
 * @path: The paths of the file to be delete.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Delete a file.
 *
 * Since: 1.0.0
 */
gboolean
garrow_file_system_delete_file(GArrowFileSystem *file_system,
                               const gchar *path,
                               GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto status = arrow_file_system->DeleteFile(path);
  return garrow::check(error, status, "[filesystem][delete-file]");
}

/**
 * garrow_file_system_delete_files:
 * @file_system: A #GArrowFileSystem.
 * @paths: The paths of the files to be delete.
 * @n_paths: The number of items in @paths.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Delete many files.
 *
 * Since: 1.0.0
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
  return garrow::check(error, status, "[filesystem][delete-files]");
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
 * Since: 1.0.0
 */
gboolean
garrow_file_system_move(GArrowFileSystem *file_system,
                        const gchar *src,
                        const gchar *dest,
                        GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto status = arrow_file_system->Move(src, dest);
  return garrow::check(error, status, "[filesystem][move]");
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
 * Since: 1.0.0
 */
gboolean
garrow_file_system_copy_file(GArrowFileSystem *file_system,
                             const gchar *src,
                             const gchar *dest,
                             GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto status = arrow_file_system->CopyFile(src, dest);
  return garrow::check(error, status, "[filesystem][copy-file]");
}

/**
 * garrow_file_system_open_input_stream:
 * @file_system: A #GArrowFileSystem.
 * @path: The path of the input stream.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Open an input stream for sequential reading.
 *
 * Returns: (nullable) (transfer full): A newly created #GArrowInputStream
 *   for appending.
 *
 * Since: 1.0.0
 */
GArrowInputStream *
garrow_file_system_open_input_stream(GArrowFileSystem *file_system,
                                     const gchar *path,
                                     GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto arrow_input_stream = arrow_file_system->OpenInputStream(path);
  if (garrow::check(error, arrow_input_stream, "[filesystem][open-input-stream]")) {
    return garrow_input_stream_new_raw(&(arrow_input_stream.ValueOrDie()));
  } else {
    return NULL;
  }
}

/* TODO: Need to implement the wrapper of arrow::io::RandomAccessFile
GArrowRandomAccessFile *
garrow_file_system_open_input_file(GArrowFileSystem *file_system,
                                   const gchar *path,
                                   GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto arrow_random_access_file = arrow_file_system->OpenInputFile(path);
  if (garrow::check(error, arrow_random_access_file, "[filesystem][open-input-file]")) {
    return garrow_random_access_file_new_raw(&(arrow_random_access_file.ValueOrDie()));
  } else {
    return NULL;
  }
}
*/

/**
 * garrow_file_system_open_output_stream:
 * @file_system: A #GArrowFileSystem.
 * @path: The path of the output stream.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Open an output stream for sequential writing.
 * If the target already exists, the existing data is truncated.
 *
 * Returns: (nullable) (transfer full): A newly created #GArrowOutputStream
 *   for appending.
 *
 * Since: 1.0.0
 */
GArrowOutputStream *
garrow_file_system_open_output_stream(GArrowFileSystem *file_system,
                                      const gchar *path,
                                      GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto arrow_output_stream = arrow_file_system->OpenOutputStream(path);
  if (garrow::check(error, arrow_output_stream, "[filesystem][open-append-stream]")) {
    return garrow_output_stream_new_raw(&(arrow_output_stream.ValueOrDie()));
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
 * Since: 1.0.0
 */
GArrowOutputStream *
garrow_file_system_open_append_stream(GArrowFileSystem *file_system,
                                      const gchar *path,
                                      GError **error)
{
  auto arrow_file_system = garrow_file_system_get_raw(file_system);
  auto arrow_output_stream = arrow_file_system->OpenAppendStream(path);
  if (garrow::check(error, arrow_output_stream, "[filesystem][open-append-stream]")) {
    return garrow_output_stream_new_raw(&(arrow_output_stream.ValueOrDie()));
  } else {
    return NULL;
  }
}

/* arrow::fs::SubTreeFileSystem */

G_DEFINE_TYPE(GArrowSubTreeFileSystem,
              garrow_sub_tree_file_system,
              GARROW_TYPE_FILE_SYSTEM)

static void
garrow_sub_tree_file_system_init(GArrowSubTreeFileSystem *file_system)
{
}

static void
garrow_sub_tree_file_system_class_init(GArrowSubTreeFileSystemClass *klass)
{
}

/**
 * garrow_sub_tree_file_system_new:
 * @base_path: A base path of the sub tree file system.
 * @base_file_system: A #GArrowFileSystem as the base file system.
 *
 * Returns: (transfer full): A newly created #GArrowSubTreeFileSystem.
 *
 * Since: 1.0.0
 */
GArrowSubTreeFileSystem *
garrow_sub_tree_file_system_new(const gchar *base_path,
                                GArrowFileSystem *base_file_system)
{
  auto arrow_base_file_system = garrow_file_system_get_raw(base_file_system);
  auto arrow_sub_tree_file_system =
    std::make_shared<arrow::fs::SubTreeFileSystem>(base_path, arrow_base_file_system);
  std::shared_ptr<arrow::fs::FileSystem> arrow_file_system = arrow_sub_tree_file_system;
  auto file_system = garrow_file_system_new_raw(&arrow_file_system,
                                                GARROW_TYPE_SUB_TREE_FILE_SYSTEM);
  return GARROW_SUB_TREE_FILE_SYSTEM(file_system);
}

/* arrow::fs::SlowFileSystem */

G_DEFINE_TYPE(GArrowSlowFileSystem,
              garrow_slow_file_system,
              GARROW_TYPE_FILE_SYSTEM)

static void
garrow_slow_file_system_init(GArrowSlowFileSystem *file_system)
{
}

static void
garrow_slow_file_system_class_init(GArrowSlowFileSystemClass *klass)
{
}

/**
 * garrow_slow_file_system_new_by_average_latency:
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
 * Since: 1.0.0
 */
GArrowSlowFileSystem *
garrow_slow_file_system_new_by_average_latency(GArrowFileSystem *base_file_system,
                                               gdouble average_latency)
{
  auto arrow_base_file_system = garrow_file_system_get_raw(base_file_system);
  auto arrow_slow_file_system =
    std::make_shared<arrow::fs::SlowFileSystem>(arrow_base_file_system, average_latency);
  std::shared_ptr<arrow::fs::FileSystem> arrow_file_system = arrow_slow_file_system;
  auto file_system = garrow_file_system_new_raw(&arrow_file_system,
                                                GARROW_TYPE_SLOW_FILE_SYSTEM);
  return GARROW_SLOW_FILE_SYSTEM(file_system);
}

/**
 * garrow_slow_file_system_new_by_average_latency_and_seed:
 * @base_file_system: A #GArrowFileSystem as the base file system.
 * @average_latency: The average value of the latency.
 * @seed: A random seed.
 *
 * The latency is normally distributed with a standard deviation of
 * @average_latency * 0.1.
 *
 * Returns: (transfer full): A newly created #GArrowSlowFileSystem.
 *
 * Since: 1.0.0
 */
GArrowSlowFileSystem *
garrow_slow_file_system_new_by_average_latency_and_seed(GArrowFileSystem *base_file_system,
                                                        gdouble average_latency,
                                                        gint32 seed)
{
  auto arrow_base_file_system = garrow_file_system_get_raw(base_file_system);
  auto arrow_slow_file_system =
    std::make_shared<arrow::fs::SlowFileSystem>(arrow_base_file_system, average_latency, seed);
  std::shared_ptr<arrow::fs::FileSystem> arrow_file_system = arrow_slow_file_system;
  auto file_system = garrow_file_system_new_raw(&arrow_file_system,
                                                GARROW_TYPE_SLOW_FILE_SYSTEM);
  return GARROW_SLOW_FILE_SYSTEM(file_system);
}

G_END_DECLS

GArrowFileStats *
garrow_file_stats_new_raw(const arrow::fs::FileStats& arrow_file_stats)
{
  auto file_stats = garrow_file_stats_new();
  GARROW_FILE_STATS_GET_PRIVATE(file_stats)->file_stats = arrow_file_stats;
  return file_stats;
}

GArrowFileSystem *
garrow_file_system_new_raw(std::shared_ptr<arrow::fs::FileSystem> *arrow_file_system,
                           GType type)
{
  auto file_system = GARROW_FILE_SYSTEM(g_object_new(type,
                                                     "file_system", arrow_file_system,
                                                     NULL));
  return file_system;
}

std::shared_ptr<arrow::fs::FileSystem>
garrow_file_system_get_raw(GArrowFileSystem *file_system)
{
  if (!file_system)
    return nullptr;

  auto priv = GARROW_FILE_SYSTEM_GET_PRIVATE(file_system);
  return priv->file_system;
}
