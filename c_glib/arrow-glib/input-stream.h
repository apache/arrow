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

#define GARROW_TYPE_INPUT_STREAM                \
  (garrow_input_stream_get_type())
#define GARROW_INPUT_STREAM(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_INPUT_STREAM, \
                              GArrowInputStream))
#define GARROW_INPUT_STREAM_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_INPUT_STREAM,    \
                           GArrowInputStreamClass))
#define GARROW_IS_INPUT_STREAM(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_INPUT_STREAM))
#define GARROW_IS_INPUT_STREAM_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_INPUT_STREAM))
#define GARROW_INPUT_STREAM_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_INPUT_STREAM,  \
                             GArrowInputStreamClass))

typedef struct _GArrowInputStream         GArrowInputStream;
#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowInputStreamClass    GArrowInputStreamClass;
#endif

/**
 * GArrowInputStream:
 *
 * It wraps `arrow::io::InputStream`.
 */
struct _GArrowInputStream
{
  /*< private >*/
  GObject parent_instance;
};

#ifndef __GTK_DOC_IGNORE__
struct _GArrowInputStreamClass
{
  GObjectClass parent_class;
};
#endif

GType garrow_input_stream_get_type(void) G_GNUC_CONST;


#define GARROW_TYPE_SEEKABLE_INPUT_STREAM       \
  (garrow_seekable_input_stream_get_type())
#define GARROW_SEEKABLE_INPUT_STREAM(obj)                               \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                                    \
                              GARROW_TYPE_SEEKABLE_INPUT_STREAM,        \
                              GArrowSeekableInputStream))
#define GARROW_SEEKABLE_INPUT_STREAM_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_SEEKABLE_INPUT_STREAM,   \
                           GArrowSeekableInputStreamClass))
#define GARROW_IS_SEEKABLE_INPUT_STREAM(obj)                            \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                                    \
                              GARROW_TYPE_SEEKABLE_INPUT_STREAM))
#define GARROW_IS_SEEKABLE_INPUT_STREAM_CLASS(klass)            \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_SEEKABLE_INPUT_STREAM))
#define GARROW_SEEKABLE_INPUT_STREAM_GET_CLASS(obj)             \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_SEEKABLE_INPUT_STREAM, \
                             GArrowSeekableInputStreamClass))

typedef struct _GArrowSeekableInputStream         GArrowSeekableInputStream;
#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowSeekableInputStreamClass    GArrowSeekableInputStreamClass;
#endif

/**
 * GArrowSeekableInputStream:
 *
 * It wraps `arrow::io::RandomAccessFile`.
 */
struct _GArrowSeekableInputStream
{
  /*< private >*/
  GArrowInputStream parent_instance;
};

#ifndef __GTK_DOC_IGNORE__
struct _GArrowSeekableInputStreamClass
{
  GArrowInputStreamClass parent_class;
};
#endif

GType garrow_seekable_input_stream_get_type(void) G_GNUC_CONST;

guint64 garrow_seekable_input_stream_get_size(GArrowSeekableInputStream *input_stream,
                                              GError **error);
gboolean garrow_seekable_input_stream_get_support_zero_copy(GArrowSeekableInputStream *input_stream);
GArrowBuffer *garrow_seekable_input_stream_read_at(GArrowSeekableInputStream *input_stream,
                                                   gint64 position,
                                                   gint64 n_bytes,
                                                   GError **error);
GArrowTensor *garrow_seekable_input_stream_read_tensor(GArrowSeekableInputStream *input_stream,
                                                       gint64 position,
                                                       GError **error);


#define GARROW_TYPE_BUFFER_INPUT_STREAM         \
  (garrow_buffer_input_stream_get_type())
#define GARROW_BUFFER_INPUT_STREAM(obj)                         \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_BUFFER_INPUT_STREAM,  \
                              GArrowBufferInputStream))
#define GARROW_BUFFER_INPUT_STREAM_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_BUFFER_INPUT_STREAM,     \
                           GArrowBufferInputStreamClass))
#define GARROW_IS_BUFFER_INPUT_STREAM(obj)                      \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_BUFFER_INPUT_STREAM))
#define GARROW_IS_BUFFER_INPUT_STREAM_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_BUFFER_INPUT_STREAM))
#define GARROW_BUFFER_INPUT_STREAM_GET_CLASS(obj)               \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_BUFFER_INPUT_STREAM,   \
                             GArrowBufferInputStreamClass))

typedef struct _GArrowBufferInputStream         GArrowBufferInputStream;
#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowBufferInputStreamClass    GArrowBufferInputStreamClass;
#endif

/**
 * GArrowBufferInputStream:
 *
 * It wraps `arrow::io::BufferReader`.
 */
struct _GArrowBufferInputStream
{
  /*< private >*/
  GArrowSeekableInputStream parent_instance;
};

#ifndef __GTK_DOC_IGNORE__
struct _GArrowBufferInputStreamClass
{
  GArrowSeekableInputStreamClass parent_class;
};
#endif

GType garrow_buffer_input_stream_get_type(void) G_GNUC_CONST;

GArrowBufferInputStream *garrow_buffer_input_stream_new(GArrowBuffer *buffer);

GArrowBuffer *garrow_buffer_input_stream_get_buffer(GArrowBufferInputStream *input_stream);


#define GARROW_TYPE_MEMORY_MAPPED_INPUT_STREAM          \
  (garrow_memory_mapped_input_stream_get_type())
#define GARROW_MEMORY_MAPPED_INPUT_STREAM(obj)                          \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                                    \
                              GARROW_TYPE_MEMORY_MAPPED_INPUT_STREAM,   \
                              GArrowMemoryMappedInputStream))
#define GARROW_MEMORY_MAPPED_INPUT_STREAM_CLASS(klass)                  \
  (G_TYPE_CHECK_CLASS_CAST((klass),                                     \
                           GARROW_TYPE_MEMORY_MAPPED_INPUT_STREAM,      \
                           GArrowMemoryMappedInputStreamClass))
#define GARROW_IS_MEMORY_MAPPED_INPUT_STREAM(obj)                       \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                                    \
                              GARROW_TYPE_MEMORY_MAPPED_INPUT_STREAM))
#define GARROW_IS_MEMORY_MAPPED_INPUT_STREAM_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                                     \
                           GARROW_TYPE_MEMORY_MAPPED_INPUT_STREAM))
#define GARROW_MEMORY_MAPPED_INPUT_STREAM_GET_CLASS(obj)                \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                                     \
                             GARROW_TYPE_MEMORY_MAPPED_INPUT_STREAM,    \
                             GArrowMemoryMappedInputStreamClass))

typedef struct _GArrowMemoryMappedInputStream         GArrowMemoryMappedInputStream;
#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowMemoryMappedInputStreamClass    GArrowMemoryMappedInputStreamClass;
#endif

/**
 * GArrowMemoryMappedInputStream:
 *
 * It wraps `arrow::io::MemoryMappedFile`.
 */
struct _GArrowMemoryMappedInputStream
{
  /*< private >*/
  GArrowSeekableInputStream parent_instance;
};

#ifndef __GTK_DOC_IGNORE__
struct _GArrowMemoryMappedInputStreamClass
{
  GArrowSeekableInputStreamClass parent_class;
};
#endif

GType garrow_memory_mapped_input_stream_get_type(void) G_GNUC_CONST;

GArrowMemoryMappedInputStream *garrow_memory_mapped_input_stream_new(const gchar *path,
                                                                     GError **error);


#define GARROW_TYPE_GIO_INPUT_STREAM            \
  (garrow_gio_input_stream_get_type())
#define GARROW_GIO_INPUT_STREAM(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_GIO_INPUT_STREAM,     \
                              GArrowGIOInputStream))
#define GARROW_GIO_INPUT_STREAM_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_GIO_INPUT_STREAM,        \
                           GArrowGIOInputStreamClass))
#define GARROW_IS_GIO_INPUT_STREAM(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_GIO_INPUT_STREAM))
#define GARROW_IS_GIO_INPUT_STREAM_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_GIO_INPUT_STREAM))
#define GARROW_GIO_INPUT_STREAM_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_GIO_INPUT_STREAM,      \
                             GArrowGIOInputStreamClass))

typedef struct _GArrowGIOInputStream         GArrowGIOInputStream;
#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowGIOInputStreamClass    GArrowGIOInputStreamClass;
#endif

/**
 * GArrowGIOInputStream:
 *
 * It's an input stream for `GInputStream`.
 */
struct _GArrowGIOInputStream
{
  /*< private >*/
  GArrowSeekableInputStream parent_instance;
};

#ifndef __GTK_DOC_IGNORE__
struct _GArrowGIOInputStreamClass
{
  GArrowSeekableInputStreamClass parent_class;
};
#endif

GType garrow_gio_input_stream_get_type(void) G_GNUC_CONST;

GArrowGIOInputStream *garrow_gio_input_stream_new(GInputStream *gio_input_stream);
GInputStream *garrow_gio_input_stream_get_gio_input_stream(GArrowGIOInputStream *input_stream);

G_END_DECLS
