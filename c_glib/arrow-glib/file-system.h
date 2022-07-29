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
 * @GARROW_FILE_TYPE_NOT_FOUND: Entry is not found
 * @GARROW_FILE_TYPE_UNKNOWN: Entry exists but its type is unknown
 * @GARROW_FILE_TYPE_FILE: Entry is a regular file
 * @GARROW_FILE_TYPE_DIR: Entry is a directory
 *
 * They are corresponding to `arrow::fs::FileType` values.
 *
 * Since: 1.0.0
 */
typedef enum {
  GARROW_FILE_TYPE_NOT_FOUND,
  GARROW_FILE_TYPE_UNKNOWN,
  GARROW_FILE_TYPE_FILE,
  GARROW_FILE_TYPE_DIR
} GArrowFileType;


/* arrow::fs::FileInfo */

#define GARROW_TYPE_FILE_INFO (garrow_file_info_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFileInfo,
                         garrow_file_info,
                         GARROW,
                         FILE_INFO,
                         GObject)
struct _GArrowFileInfoClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_0_17
GArrowFileInfo *garrow_file_info_new(void);

GARROW_AVAILABLE_IN_0_17
gboolean garrow_file_info_equal(GArrowFileInfo *file_info,
                                GArrowFileInfo *other_file_info);

GARROW_AVAILABLE_IN_0_17
gboolean garrow_file_info_is_file(GArrowFileInfo *file_info);
GARROW_AVAILABLE_IN_0_17
gboolean garrow_file_info_is_dir(GArrowFileInfo *file_info);
GARROW_AVAILABLE_IN_0_17
gchar *garrow_file_info_to_string(GArrowFileInfo *file_info);

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

GARROW_AVAILABLE_IN_3_0
GArrowFileSystem *
garrow_file_system_create(const gchar *uri,
                          GError **error);

GARROW_AVAILABLE_IN_0_17
gchar *garrow_file_system_get_type_name(GArrowFileSystem *file_system);

GARROW_AVAILABLE_IN_0_17
GArrowFileInfo *
garrow_file_system_get_file_info(GArrowFileSystem *file_system,
                                 const gchar *path,
                                 GError **error);

GARROW_AVAILABLE_IN_0_17
GList *garrow_file_system_get_file_infos_paths(GArrowFileSystem *file_system,
                                               const gchar **paths,
                                               gsize n_paths,
                                               GError **error);

GARROW_AVAILABLE_IN_0_17
GList *
garrow_file_system_get_file_infos_selector(GArrowFileSystem *file_system,
                                           GArrowFileSelector *file_selector,
                                           GError **error);

GARROW_AVAILABLE_IN_0_17
gboolean garrow_file_system_create_dir(GArrowFileSystem *file_system,
                                       const gchar *path,
                                       gboolean recursive,
                                       GError **error);

GARROW_AVAILABLE_IN_0_17
gboolean garrow_file_system_delete_dir(GArrowFileSystem *file_system,
                                       const gchar *path,
                                       GError **error);

GARROW_AVAILABLE_IN_0_17
gboolean garrow_file_system_delete_dir_contents(GArrowFileSystem *file_system,
                                                const gchar *path,
                                                GError **error);

GARROW_AVAILABLE_IN_0_17
gboolean garrow_file_system_delete_file(GArrowFileSystem *file_system,
                                        const gchar *path,
                                        GError **error);

GARROW_AVAILABLE_IN_0_17
gboolean garrow_file_system_delete_files(GArrowFileSystem *file_system,
                                        const gchar **paths,
                                        gsize n_paths,
                                        GError **error);

GARROW_AVAILABLE_IN_0_17
gboolean garrow_file_system_move(GArrowFileSystem *file_system,
                                 const gchar *src,
                                 const gchar *dest,
                                 GError **error);

GARROW_AVAILABLE_IN_0_17
gboolean garrow_file_system_copy_file(GArrowFileSystem *file_system,
                                      const gchar *src,
                                      const gchar *dest,
                                      GError **error);

GARROW_AVAILABLE_IN_0_17
GArrowInputStream *
garrow_file_system_open_input_stream(GArrowFileSystem *file_system,
                                     const gchar *path,
                                     GError **error);

GARROW_AVAILABLE_IN_0_17
GArrowSeekableInputStream *
garrow_file_system_open_input_file(GArrowFileSystem *file_system,
                                   const gchar *path,
                                   GError **error);

GARROW_AVAILABLE_IN_0_17
GArrowOutputStream *
garrow_file_system_open_output_stream(GArrowFileSystem *file_system,
                                      const gchar *path,
                                      GError **error);

GARROW_AVAILABLE_IN_0_17
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

GARROW_AVAILABLE_IN_0_17
GArrowSubTreeFileSystem *
garrow_sub_tree_file_system_new(const gchar *base_path,
                                GArrowFileSystem *base_file_system);

/* arrow::fs::SlowFileSystem */

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
GARROW_AVAILABLE_IN_0_17
GArrowSlowFileSystem *
garrow_slow_file_system_new(GArrowFileSystem *base_file_system,
                            GArrowLatencyGenerator *latencies);
*/

GARROW_AVAILABLE_IN_0_17
GArrowSlowFileSystem *
garrow_slow_file_system_new_average_latency(GArrowFileSystem *base_file_system,
                                            gdouble average_latency);

GARROW_AVAILABLE_IN_0_17
GArrowSlowFileSystem *
garrow_slow_file_system_new_average_latency_and_seed(GArrowFileSystem *base_file_system,
                                                     gdouble average_latency,
                                                     gint32 seed);



#define GARROW_TYPE_MOCK_FILE_SYSTEM (garrow_mock_file_system_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowMockFileSystem,
                         garrow_mock_file_system,
                         GARROW,
                         MOCK_FILE_SYSTEM,
                         GArrowFileSystem)
struct _GArrowMockFileSystemClass
{
  GArrowFileSystemClass parent_class;
};


#define GARROW_TYPE_HDFS_FILE_SYSTEM (garrow_hdfs_file_system_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowHDFSFileSystem,
                         garrow_hdfs_file_system,
                         GARROW,
                         HDFS_FILE_SYSTEM,
                         GArrowFileSystem)
struct _GArrowHDFSFileSystemClass
{
  GArrowFileSystemClass parent_class;
};


/**
 * GArrowS3LogLevel:
 * @GARROW_S3_LOG_LEVEL_OFF: Off.
 * @GARROW_S3_LOG_LEVEL_FATAL: Fatal. This is the default.
 * @GARROW_S3_LOG_LEVEL_ERROR: Error.
 * @GARROW_S3_LOG_LEVEL_WARN: Warn.
 * @GARROW_S3_LOG_LEVEL_INFO: Info.
 * @GARROW_S3_LOG_LEVEL_DEBUG: Debug.
 * @GARROW_S3_LOG_LEVEL_TRACE: Trace.
 *
 * They are corresponding to `arrow::fs::S3LogLevel` values.
 *
 * Since: 7.0.0
 */
typedef enum {
  GARROW_S3_LOG_LEVEL_OFF,
  GARROW_S3_LOG_LEVEL_FATAL,
  GARROW_S3_LOG_LEVEL_ERROR,
  GARROW_S3_LOG_LEVEL_WARN,
  GARROW_S3_LOG_LEVEL_INFO,
  GARROW_S3_LOG_LEVEL_DEBUG,
  GARROW_S3_LOG_LEVEL_TRACE,
} GArrowS3LogLevel;


#define GARROW_TYPE_S3_GLOBAL_OPTIONS (garrow_s3_global_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowS3GlobalOptions,
                         garrow_s3_global_options,
                         GARROW,
                         S3_GLOBAL_OPTIONS,
                         GObject)
struct _GArrowS3GlobalOptionsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_7_0
GArrowS3GlobalOptions *
garrow_s3_global_options_new(void);


GARROW_AVAILABLE_IN_7_0
gboolean
garrow_s3_is_enabled(void);
GARROW_AVAILABLE_IN_7_0
gboolean
garrow_s3_initialize(GArrowS3GlobalOptions *options,
                     GError **error);
GARROW_AVAILABLE_IN_7_0
gboolean
garrow_s3_finalize(GError **error);


#define GARROW_TYPE_S3_FILE_SYSTEM (garrow_s3_file_system_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowS3FileSystem,
                         garrow_s3_file_system,
                         GARROW,
                         S3_FILE_SYSTEM,
                         GArrowFileSystem)
struct _GArrowS3FileSystemClass
{
  GArrowFileSystemClass parent_class;
};


#define GARROW_TYPE_GCS_FILE_SYSTEM (garrow_gcs_file_system_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowGCSFileSystem,
                         garrow_gcs_file_system,
                         GARROW,
                         GCS_FILE_SYSTEM,
                         GArrowFileSystem)
struct _GArrowGCSFileSystemClass
{
  GArrowFileSystemClass parent_class;
};


G_END_DECLS
