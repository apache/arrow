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
#include <arrow-glib/codec.h>
#include <arrow-glib/ipc-options.h>
#include <arrow-glib/record-batch.h>
#include <arrow-glib/tensor.h>

G_BEGIN_DECLS

#define GARROW_TYPE_INPUT_STREAM (garrow_input_stream_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowInputStream,
                         garrow_input_stream,
                         GARROW,
                         INPUT_STREAM,
                         GInputStream)
struct _GArrowInputStreamClass
{
  GInputStreamClass parent_class;
};

gboolean garrow_input_stream_advance(GArrowInputStream *input_stream,
                                     gint64 n_bytes,
                                     GError **error);
gboolean garrow_input_stream_align(GArrowInputStream *input_stream,
                                   gint32 alignment,
                                   GError **error);
GArrowTensor *garrow_input_stream_read_tensor(GArrowInputStream *input_stream,
                                              GError **error);
GARROW_AVAILABLE_IN_1_0
GArrowRecordBatch *
garrow_input_stream_read_record_batch(GArrowInputStream *input_stream,
                                      GArrowSchema *schema,
                                      GArrowReadOptions *options,
                                      GError **error);

#define GARROW_TYPE_SEEKABLE_INPUT_STREAM       \
  (garrow_seekable_input_stream_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowSeekableInputStream,
                         garrow_seekable_input_stream,
                         GARROW,
                         SEEKABLE_INPUT_STREAM,
                         GArrowInputStream)
struct _GArrowSeekableInputStreamClass
{
  GArrowInputStreamClass parent_class;
};

guint64 garrow_seekable_input_stream_get_size(GArrowSeekableInputStream *input_stream,
                                              GError **error);
gboolean garrow_seekable_input_stream_get_support_zero_copy(GArrowSeekableInputStream *input_stream);
GArrowBuffer *
garrow_seekable_input_stream_read_at(GArrowSeekableInputStream *input_stream,
                                     gint64 position,
                                     gint64 n_bytes,
                                     GError **error);
GARROW_AVAILABLE_IN_0_15
GBytes *
garrow_seekable_input_stream_read_at_bytes(GArrowSeekableInputStream *input_stream,
                                           gint64 position,
                                           gint64 n_bytes,
                                           GError **error);
GARROW_AVAILABLE_IN_0_12
GBytes *garrow_seekable_input_stream_peek(GArrowSeekableInputStream *input_stream,
                                          gint64 n_bytes,
                                          GError **error);


#define GARROW_TYPE_BUFFER_INPUT_STREAM         \
  (garrow_buffer_input_stream_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowBufferInputStream,
                         garrow_buffer_input_stream,
                         GARROW,
                         BUFFER_INPUT_STREAM,
                         GArrowSeekableInputStream)
struct _GArrowBufferInputStreamClass
{
  GArrowSeekableInputStreamClass parent_class;
};

GArrowBufferInputStream *garrow_buffer_input_stream_new(GArrowBuffer *buffer);

GArrowBuffer *garrow_buffer_input_stream_get_buffer(GArrowBufferInputStream *input_stream);


#define GARROW_TYPE_FILE_INPUT_STREAM (garrow_file_input_stream_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFileInputStream,
                         garrow_file_input_stream,
                         GARROW,
                         FILE_INPUT_STREAM,
                         GArrowSeekableInputStream)
struct _GArrowFileInputStreamClass
{
  GArrowSeekableInputStreamClass parent_class;
};

GArrowFileInputStream *
garrow_file_input_stream_new(const gchar *path,
                             GError **error);
GArrowFileInputStream *
garrow_file_input_stream_new_file_descriptor(gint file_descriptor,
                                             GError **error);
gint
garrow_file_input_stream_get_file_descriptor(GArrowFileInputStream *stream);


#define GARROW_TYPE_MEMORY_MAPPED_INPUT_STREAM          \
  (garrow_memory_mapped_input_stream_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowMemoryMappedInputStream,
                         garrow_memory_mapped_input_stream,
                         GARROW,
                         MEMORY_MAPPED_INPUT_STREAM,
                         GArrowSeekableInputStream)
struct _GArrowMemoryMappedInputStreamClass
{
  GArrowSeekableInputStreamClass parent_class;
};

GArrowMemoryMappedInputStream *
garrow_memory_mapped_input_stream_new(const gchar *path,
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
#ifndef GARROW_DISABLE_DEPRECATED
G_GNUC_DEPRECATED
GInputStream *
garrow_gio_input_stream_get_raw(GArrowGIOInputStream *input_stream);
#endif

#define GARROW_TYPE_COMPRESSED_INPUT_STREAM     \
  (garrow_compressed_input_stream_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowCompressedInputStream,
                         garrow_compressed_input_stream,
                         GARROW,
                         COMPRESSED_INPUT_STREAM,
                         GArrowInputStream)
struct _GArrowCompressedInputStreamClass
{
  GArrowInputStreamClass parent_class;
};

GArrowCompressedInputStream *
garrow_compressed_input_stream_new(GArrowCodec *codec,
                                   GArrowInputStream *raw,
                                   GError **error);

G_END_DECLS
