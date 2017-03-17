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

#include <arrow-glib/array.h>
#include <arrow-glib/record-batch.h>
#include <arrow-glib/schema.h>

#include <arrow-glib/io-output-stream.h>

G_BEGIN_DECLS

#define GARROW_IPC_TYPE_STREAM_WRITER           \
  (garrow_ipc_stream_writer_get_type())
#define GARROW_IPC_STREAM_WRITER(obj)                           \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_IPC_TYPE_STREAM_WRITER,    \
                              GArrowIPCStreamWriter))
#define GARROW_IPC_STREAM_WRITER_CLASS(klass)                   \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_IPC_TYPE_STREAM_WRITER,       \
                           GArrowIPCStreamWriterClass))
#define GARROW_IPC_IS_STREAM_WRITER(obj)                        \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_IPC_TYPE_STREAM_WRITER))
#define GARROW_IPC_IS_STREAM_WRITER_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_IPC_TYPE_STREAM_WRITER))
#define GARROW_IPC_STREAM_WRITER_GET_CLASS(obj)                 \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_IPC_TYPE_STREAM_WRITER,     \
                             GArrowIPCStreamWriterClass))

typedef struct _GArrowIPCStreamWriter         GArrowIPCStreamWriter;
typedef struct _GArrowIPCStreamWriterClass    GArrowIPCStreamWriterClass;

/**
 * GArrowIPCStreamWriter:
 *
 * It wraps `arrow::ipc::StreamWriter`.
 */
struct _GArrowIPCStreamWriter
{
  /*< private >*/
  GObject parent_instance;
};

struct _GArrowIPCStreamWriterClass
{
  GObjectClass parent_class;
};

GType garrow_ipc_stream_writer_get_type(void) G_GNUC_CONST;

GArrowIPCStreamWriter *garrow_ipc_stream_writer_open(GArrowIOOutputStream *sink,
                                                     GArrowSchema *schema,
                                                     GError **error);

gboolean garrow_ipc_stream_writer_write_record_batch(GArrowIPCStreamWriter *stream_writer,
                                                     GArrowRecordBatch *record_batch,
                                                     GError **error);
gboolean garrow_ipc_stream_writer_close(GArrowIPCStreamWriter *stream_writer,
                                        GError **error);

G_END_DECLS
