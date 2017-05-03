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

#include <arrow-glib/buffer.h>

G_BEGIN_DECLS

#define GARROW_TYPE_OUTPUT_STREAM               \
  (garrow_output_stream_get_type())
#define GARROW_OUTPUT_STREAM(obj)                               \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_OUTPUT_STREAM,        \
                              GArrowOutputStream))
#define GARROW_OUTPUT_STREAM_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_OUTPUT_STREAM,   \
                           GArrowOutputStreamClass))
#define GARROW_IS_OUTPUT_STREAM(obj)                            \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_OUTPUT_STREAM))
#define GARROW_IS_OUTPUT_STREAM_CLASS(klass)            \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_OUTPUT_STREAM))
#define GARROW_OUTPUT_STREAM_GET_CLASS(obj)             \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_OUTPUT_STREAM, \
                             GArrowOutputStreamClass))

typedef struct _GArrowOutputStream          GArrowOutputStream;
#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowOutputStreamClass     GArrowOutputStreamClass;
#endif

/**
 * GArrowOutputStream:
 *
 * It wraps `arrow::io::OutputStream`.
 */
struct _GArrowOutputStream
{
  /*< private >*/
  GObject parent_instance;
};

#ifndef __GTK_DOC_IGNORE__
struct _GArrowOutputStreamClass
{
  GObjectClass parent_class;
};
#endif

GType garrow_output_stream_get_type(void) G_GNUC_CONST;


#define GARROW_TYPE_FILE_OUTPUT_STREAM          \
  (garrow_file_output_stream_get_type())
#define GARROW_FILE_OUTPUT_STREAM(obj)                          \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_FILE_OUTPUT_STREAM,   \
                              GArrowFileOutputStream))
#define GARROW_FILE_OUTPUT_STREAM_CLASS(klass)                  \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_FILE_OUTPUT_STREAM,      \
                           GArrowFileOutputStreamClass))
#define GARROW_IS_FILE_OUTPUT_STREAM(obj)                       \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_FILE_OUTPUT_STREAM))
#define GARROW_IS_FILE_OUTPUT_STREAM_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_FILE_OUTPUT_STREAM))
#define GARROW_FILE_OUTPUT_STREAM_GET_CLASS(obj)                \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_FILE_OUTPUT_STREAM,    \
                             GArrowFileOutputStreamClass))

typedef struct _GArrowFileOutputStream         GArrowFileOutputStream;
#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowFileOutputStreamClass    GArrowFileOutputStreamClass;
#endif

/**
 * GArrowFileOutputStream:
 *
 * It wraps `arrow::io::FileOutputStream`.
 */
struct _GArrowFileOutputStream
{
  /*< private >*/
  GArrowOutputStream parent_instance;
};

#ifndef __GTK_DOC_IGNORE__
struct _GArrowFileOutputStreamClass
{
  GArrowOutputStreamClass parent_class;
};
#endif

GType garrow_file_output_stream_get_type(void) G_GNUC_CONST;

GArrowFileOutputStream *garrow_file_output_stream_new(const gchar *path,
                                                      gboolean append,
                                                      GError **error);


#define GARROW_TYPE_BUFFER_OUTPUT_STREAM        \
  (garrow_buffer_output_stream_get_type())
#define GARROW_BUFFER_OUTPUT_STREAM(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_BUFFER_OUTPUT_STREAM, \
                              GArrowBufferOutputStream))
#define GARROW_BUFFER_OUTPUT_STREAM_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_BUFFER_OUTPUT_STREAM,    \
                           GArrowBufferOutputStreamClass))
#define GARROW_IS_BUFFER_OUTPUT_STREAM(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                                    \
                              GARROW_TYPE_BUFFER_OUTPUT_STREAM))
#define GARROW_IS_BUFFER_OUTPUT_STREAM_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_BUFFER_OUTPUT_STREAM))
#define GARROW_BUFFER_OUTPUT_STREAM_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_BUFFER_OUTPUT_STREAM,  \
                             GArrowBufferOutputStreamClass))

typedef struct _GArrowBufferOutputStream         GArrowBufferOutputStream;
#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowBufferOutputStreamClass    GArrowBufferOutputStreamClass;
#endif

/**
 * GArrowBufferOutputStream:
 *
 * It wraps `arrow::io::BufferOutputStream`.
 */
struct _GArrowBufferOutputStream
{
  /*< private >*/
  GArrowOutputStream parent_instance;
};

#ifndef __GTK_DOC_IGNORE__
struct _GArrowBufferOutputStreamClass
{
  GArrowOutputStreamClass parent_class;
};
#endif

GType garrow_buffer_output_stream_get_type(void) G_GNUC_CONST;

GArrowBufferOutputStream *garrow_buffer_output_stream_new(GArrowResizableBuffer *buffer);

G_END_DECLS
