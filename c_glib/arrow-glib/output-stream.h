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

#include <gio/gio.h>

#include <arrow-glib/buffer.h>
#include <arrow-glib/tensor.h>

G_BEGIN_DECLS

#define GARROW_TYPE_OUTPUT_STREAM (garrow_output_stream_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowOutputStream,
                         garrow_output_stream,
                         GARROW,
                         OUTPUT_STREAM,
                         GObject)
struct _GArrowOutputStreamClass
{
  GObjectClass parent_class;
};

gboolean garrow_output_stream_align(GArrowOutputStream *stream,
                                    gint32 alignment,
                                    GError **error);
gint64 garrow_output_stream_write_tensor(GArrowOutputStream *stream,
                                         GArrowTensor *tensor,
                                         GError **error);


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


#define GARROW_TYPE_GIO_OUTPUT_STREAM           \
  (garrow_gio_output_stream_get_type())
#define GARROW_GIO_OUTPUT_STREAM(obj)                           \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_GIO_OUTPUT_STREAM,    \
                              GArrowGIOOutputStream))
#define GARROW_GIO_OUTPUT_STREAM_CLASS(klass)                   \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_GIO_OUTPUT_STREAM,       \
                           GArrowGIOOutputStreamClass))
#define GARROW_IS_GIO_OUTPUT_STREAM(obj)                        \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_GIO_OUTPUT_STREAM))
#define GARROW_IS_GIO_OUTPUT_STREAM_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_GIO_OUTPUT_STREAM))
#define GARROW_GIO_OUTPUT_STREAM_GET_CLASS(obj)                 \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_GIO_OUTPUT_STREAM,     \
                             GArrowGIOOutputStreamClass))

typedef struct _GArrowGIOOutputStream         GArrowGIOOutputStream;
#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowGIOOutputStreamClass    GArrowGIOOutputStreamClass;
#endif

/**
 * GArrowGIOOutputStream:
 *
 * It's an output stream for `GOutputStream`.
 */
struct _GArrowGIOOutputStream
{
  /*< private >*/
  GArrowOutputStream parent_instance;
};

#ifndef __GTK_DOC_IGNORE__
struct _GArrowGIOOutputStreamClass
{
  GArrowOutputStreamClass parent_class;
};
#endif

GType garrow_gio_output_stream_get_type(void) G_GNUC_CONST;

GArrowGIOOutputStream *garrow_gio_output_stream_new(GOutputStream *gio_output_stream);
GOutputStream *garrow_gio_output_stream_get_raw(GArrowGIOOutputStream *output_stream);

G_END_DECLS
