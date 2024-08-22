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

#define GARROW_TYPE_OUTPUT_STREAM (garrow_output_stream_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowOutputStream, garrow_output_stream, GARROW, OUTPUT_STREAM, GObject)
struct _GArrowOutputStreamClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
gboolean
garrow_output_stream_align(GArrowOutputStream *stream, gint32 alignment, GError **error);

GARROW_AVAILABLE_IN_ALL
gint64
garrow_output_stream_write_tensor(GArrowOutputStream *stream,
                                  GArrowTensor *tensor,
                                  GError **error);
GARROW_AVAILABLE_IN_1_0
gint64
garrow_output_stream_write_record_batch(GArrowOutputStream *stream,
                                        GArrowRecordBatch *record_batch,
                                        GArrowWriteOptions *options,
                                        GError **error);

#define GARROW_TYPE_FILE_OUTPUT_STREAM (garrow_file_output_stream_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowFileOutputStream,
                         garrow_file_output_stream,
                         GARROW,
                         FILE_OUTPUT_STREAM,
                         GArrowOutputStream)
struct _GArrowFileOutputStreamClass
{
  GArrowOutputStreamClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowFileOutputStream *
garrow_file_output_stream_new(const gchar *path, gboolean append, GError **error);

#define GARROW_TYPE_BUFFER_OUTPUT_STREAM (garrow_buffer_output_stream_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowBufferOutputStream,
                         garrow_buffer_output_stream,
                         GARROW,
                         BUFFER_OUTPUT_STREAM,
                         GArrowOutputStream)
struct _GArrowBufferOutputStreamClass
{
  GArrowOutputStreamClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowBufferOutputStream *
garrow_buffer_output_stream_new(GArrowResizableBuffer *buffer);

#define GARROW_TYPE_GIO_OUTPUT_STREAM (garrow_gio_output_stream_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowGIOOutputStream,
                         garrow_gio_output_stream,
                         GARROW,
                         GIO_OUTPUT_STREAM,
                         GArrowOutputStream)
struct _GArrowGIOOutputStreamClass
{
  GArrowOutputStreamClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowGIOOutputStream *
garrow_gio_output_stream_new(GOutputStream *gio_output_stream);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_AVAILABLE_IN_ALL
G_GNUC_DEPRECATED
GOutputStream *
garrow_gio_output_stream_get_raw(GArrowGIOOutputStream *output_stream);
#endif

#define GARROW_TYPE_COMPRESSED_OUTPUT_STREAM (garrow_compressed_output_stream_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowCompressedOutputStream,
                         garrow_compressed_output_stream,
                         GARROW,
                         COMPRESSED_OUTPUT_STREAM,
                         GArrowOutputStream)
struct _GArrowCompressedOutputStreamClass
{
  GArrowOutputStreamClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowCompressedOutputStream *
garrow_compressed_output_stream_new(GArrowCodec *codec,
                                    GArrowOutputStream *raw,
                                    GError **error);

G_END_DECLS
