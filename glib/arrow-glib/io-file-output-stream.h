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

G_BEGIN_DECLS

#define GARROW_IO_TYPE_FILE_OUTPUT_STREAM       \
  (garrow_io_file_output_stream_get_type())
#define GARROW_IO_FILE_OUTPUT_STREAM(obj)                               \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                                    \
                              GARROW_IO_TYPE_FILE_OUTPUT_STREAM,        \
                              GArrowIOFileOutputStream))
#define GARROW_IO_FILE_OUTPUT_STREAM_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_IO_TYPE_FILE_OUTPUT_STREAM,   \
                           GArrowIOFileOutputStreamClass))
#define GARROW_IO_IS_FILE_OUTPUT_STREAM(obj)                            \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                                    \
                              GARROW_IO_TYPE_FILE_OUTPUT_STREAM))
#define GARROW_IO_IS_FILE_OUTPUT_STREAM_CLASS(klass)            \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_IO_TYPE_FILE_OUTPUT_STREAM))
#define GARROW_IO_FILE_OUTPUT_STREAM_GET_CLASS(obj)             \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_IO_TYPE_FILE_OUTPUT_STREAM, \
                             GArrowIOFileOutputStreamClass))

typedef struct _GArrowIOFileOutputStream         GArrowIOFileOutputStream;
typedef struct _GArrowIOFileOutputStreamClass    GArrowIOFileOutputStreamClass;

/**
 * GArrowIOFileOutputStream:
 *
 * It wraps `arrow::io::FileOutputStream`.
 */
struct _GArrowIOFileOutputStream
{
  /*< private >*/
  GObject parent_instance;
};

struct _GArrowIOFileOutputStreamClass
{
  GObjectClass parent_class;
};

GType garrow_io_file_output_stream_get_type(void) G_GNUC_CONST;

GArrowIOFileOutputStream *garrow_io_file_output_stream_open(const gchar *path,
                                                            gboolean append,
                                                            GError **error);

G_END_DECLS
