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

#pragma once

#include <glib-object.h>

#include <arrow-glib/input-stream.h>
#include <arrow-glib/output-stream.h>
#include <arrow-glib/version.h>

G_BEGIN_DECLS

/* arrow::fs::TimePoint */
typedef gint64 GArrowTimePoint;

/* arrow::fs::FileType */

/**
 * GArrowFileType
 * @GARROW_FILE_TYPE_NON_EXISTENT: Entry does not exist
 * @GARROW_FILE_TYPE_UNKNOWN: Entry exists but its type is unknown
 * @GARROW_FILE_TYPE_FILE: Entry is a regular file
 * @GARROW_FILE_TYPE_DIRECTORY: Entry is a directory
 *
 * They are corresponding to `arrow::fs::FileType` values.
 */
typedef enum {
  GARROW_FILE_TYPE_NON_EXISTENT,
  GARROW_FILE_TYPE_UNKNOWN,
  GARROW_FILE_TYPE_FILE,
  GARROW_FILE_TYPE_DIRECTORY
} GArrowFileType;


/* arrow::fs::FileStats */

#define GARROW_TYPE_FILE_STATS (garrow_file_stats_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFileStats,
                         garrow_file_stats,
                         GARROW,
                         FILE_STATS,
                         GObject)
struct _GArrowFileStatsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_1_0
GArrowFileStats *garrow_file_stats_new(void);

GARROW_AVAILABLE_IN_1_0
GArrowFileType garrow_file_stats_get_file_type(GArrowFileStats *file_stats);
GARROW_AVAILABLE_IN_1_0
void garrow_file_stats_set_file_type(GArrowFileStats *file_stats,
                                     GArrowFileType file_type);
GARROW_AVAILABLE_IN_1_0
gboolean garrow_file_stats_is_file(GArrowFileStats *file_stats);
GARROW_AVAILABLE_IN_1_0
gboolean garrow_file_stats_is_directory(GArrowFileStats *file_stats);

GARROW_AVAILABLE_IN_1_0
const gchar *garrow_file_stats_get_path(GArrowFileStats *file_stats);
GARROW_AVAILABLE_IN_1_0
void garrow_file_stats_set_path(GArrowFileStats *file_stats,
                                const gchar *path);

GARROW_AVAILABLE_IN_1_0
gchar *garrow_file_stats_get_base_name(GArrowFileStats *file_stats);
GARROW_AVAILABLE_IN_1_0
gchar *garrow_file_stats_get_dir_name(GArrowFileStats *file_stats);
GARROW_AVAILABLE_IN_1_0
gchar *garrow_file_stats_get_extension(GArrowFileStats *file_stats);

GARROW_AVAILABLE_IN_1_0
gint64 garrow_file_stats_get_size(GArrowFileStats *file_stats);
GARROW_AVAILABLE_IN_1_0
void garrow_file_stats_set_size(GArrowFileStats *file_stats,
                                gint64 size);

GARROW_AVAILABLE_IN_1_0
GArrowTimePoint garrow_file_stats_get_mtime(GArrowFileStats *file_stats);

GARROW_AVAILABLE_IN_1_0
void garrow_file_stats_set_mtime(GArrowFileStats *file_stats,
                                 GArrowTimePoint mtime);

/* arrow::fs::FileSelector */

#define GARROW_TYPE_FILE_SELECTOR (garrow_file_selector_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFileSelector,
                         garrow_file_selector,
                         GARROW,
                         FILE_SELECTOR,
                         GObject)
struct _GArrowFileSelectorClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_1_0
const gchar *garrow_file_selector_get_base_dir(GArrowFileSelector *file_selector);
GARROW_AVAILABLE_IN_1_0
void         garrow_file_selector_set_base_dir(GArrowFileSelector *file_selector,
                                               const gchar *base_dir);

GARROW_AVAILABLE_IN_1_0
gboolean garrow_file_selector_get_allow_non_existent(GArrowFileSelector *file_selector);
GARROW_AVAILABLE_IN_1_0
void     garrow_file_selector_set_allow_non_existent(GArrowFileSelector *file_selector,
                                                     gboolean allow_non_existent);

GARROW_AVAILABLE_IN_1_0
gboolean garrow_file_selector_get_recursive(GArrowFileSelector *file_selector);
GARROW_AVAILABLE_IN_1_0
void     garrow_file_selector_set_recursive(GArrowFileSelector *file_selector,
                                            gboolean recursive);

GARROW_AVAILABLE_IN_1_0
gint32 garrow_file_selector_get_max_recursion(GArrowFileSelector *file_selector);
GARROW_AVAILABLE_IN_1_0
void   garrow_file_selector_set_max_recursion(GArrowFileSelector *file_selector,
                                              gint32 max_recursion);

/* arrow::fs::FileSystem */

#define GARROW_TYPE_FILE_SYSTEM (garrow_file_system_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFileSystem,
                         garrow_file_system,
                         GARROW,
                         FILE_SYSTEM,
                         GObject)
struct _GArrowFileSystemClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_1_0
const gchar *garrow_file_system_get_type_name(GArrowFileSystem *file_system);

GARROW_AVAILABLE_IN_1_0
GArrowFileStats *garrow_file_system_get_target_stats(GArrowFileSystem *file_system,
                                                     const gchar *path,
                                                     GError **error);

GARROW_AVAILABLE_IN_1_0
GList *garrow_file_system_get_target_stats_list(GArrowFileSystem *file_system,
                                                const gchar **paths,
                                                gsize n_paths,
                                                GError **error);

GARROW_AVAILABLE_IN_1_0
GList *garrow_file_system_get_target_stats_list_by_selector(GArrowFileSystem *file_system,
                                                            GArrowFileSelector *file_selector,
                                                            GError **error);

GARROW_AVAILABLE_IN_1_0
void garrow_file_system_create_dir(GArrowFileSystem *file_system,
                                   const gchar *path,
                                   gboolean recursive,
                                   GError **error);

GARROW_AVAILABLE_IN_1_0
void garrow_file_system_delete_dir(GArrowFileSystem *file_system,
                                   const gchar *path,
                                   GError **error);

GARROW_AVAILABLE_IN_1_0
void garrow_file_system_delete_dir_contents(GArrowFileSystem *file_system,
                                            const gchar *path,
                                            GError **error);

GARROW_AVAILABLE_IN_1_0
void garrow_file_system_delete_file(GArrowFileSystem *file_system,
                                    const gchar *path,
                                    GError **error);

GARROW_AVAILABLE_IN_1_0
void garrow_file_system_delete_files(GArrowFileSystem *file_system,
                                     const gchar **paths,
                                     gsize n_paths,
                                     GError **error);

GARROW_AVAILABLE_IN_1_0
void garrow_file_system_move(GArrowFileSystem *file_system,
                             const gchar *src,
                             const gchar *dest,
                             GError **error);

GARROW_AVAILABLE_IN_1_0
void garrow_file_system_copy_file(GArrowFileSystem *file_system,
                                  const gchar *src,
                                  const gchar *dest,
                                  GError **error);

GARROW_AVAILABLE_IN_1_0
GArrowInputStream *
garrow_file_system_open_input_stream(GArrowFileSystem *file_system,
                                     const gchar *path,
                                     GError **error);

/* TODO:
GARROW_AVAILABLE_IN_1_0
GArrowRandomAccessFile *
garrow_file_system_open_input_file(GArrowFileSystem *file_system,
                                   const gchar *path,
                                   GError **error);
*/

GARROW_AVAILABLE_IN_1_0
GArrowOutputStream *
garrow_file_system_open_output_stream(GArrowFileSystem *file_system,
                                      const gchar *path,
                                      GError **error);

GARROW_AVAILABLE_IN_1_0
GArrowOutputStream *
garrow_file_system_open_append_stream(GArrowFileSystem *file_system,
                                      const gchar *path,
                                      GError **error);

/* arrow::fs::SubTreeFileSystem */

#define GARROW_TYPE_SUB_TREE_FILE_SYSTEM (garrow_sub_tree_file_system_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowSubTreeFileSystem,
                         garrow_sub_tree_file_system,
                         GARROW,
                         SUB_TREE_FILE_SYSTEM,
                         GArrowFileSystem)
struct _GArrowSubTreeFileSystemClass
{
  GArrowFileSystemClass parent_class;
};

GARROW_AVAILABLE_IN_1_0
GArrowSubTreeFileSystem *
garrow_sub_tree_file_system_new(const gchar *base_path,
                                GArrowFileSystem *base_file_system);

/* TODO: arrow::fs::SlowFileSystem */

#define GARROW_TYPE_SLOW_FILE_SYSTEM (garrow_slow_file_system_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowSlowFileSystem,
                         garrow_slow_file_system,
                         GARROW,
                         SLOW_FILE_SYSTEM,
                         GArrowFileSystem)
struct _GArrowSlowFileSystemClass
{
  GArrowFileSystemClass parent_class;
};

/* TODO: GArrowLatencyGenerator
GARROW_AVAILABLE_IN_1_0
GArrowSlowFileSystem *
garrow_slow_file_system_new(GArrowFileSystem *base_file_system,
                            GArrowLatencyGenerator *latencies);
*/

GARROW_AVAILABLE_IN_1_0
GArrowSlowFileSystem *
garrow_slow_file_system_new_by_average_latency(GArrowFileSystem *base_file_system,
                                               gdouble average_latency);

GARROW_AVAILABLE_IN_1_0
GArrowSlowFileSystem *
garrow_slow_file_system_new_by_average_latency_and_seed(GArrowFileSystem *base_file_system,
                                                        gdouble average_latency,
                                                        gint32 seed);

G_END_DECLS
