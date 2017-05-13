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

#include <arrow-glib/output-stream.h>

G_BEGIN_DECLS

#define GARROW_TYPE_STREAM_WRITER               \
  (garrow_stream_writer_get_type())
#define GARROW_STREAM_WRITER(obj)                               \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_STREAM_WRITER,        \
                              GArrowStreamWriter))
#define GARROW_STREAM_WRITER_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_STREAM_WRITER,   \
                           GArrowStreamWriterClass))
#define GARROW_IS_STREAM_WRITER(obj)                            \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_STREAM_WRITER))
#define GARROW_IS_STREAM_WRITER_CLASS(klass)            \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_STREAM_WRITER))
#define GARROW_STREAM_WRITER_GET_CLASS(obj)             \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_STREAM_WRITER, \
                             GArrowStreamWriterClass))

typedef struct _GArrowStreamWriter         GArrowStreamWriter;
#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowStreamWriterClass    GArrowStreamWriterClass;
#endif

/**
 * GArrowStreamWriter:
 *
 * It wraps `arrow::ipc::RecordBatchStreamWriter`.
 */
struct _GArrowStreamWriter
{
  /*< private >*/
  GObject parent_instance;
};

#ifndef __GTK_DOC_IGNORE__
struct _GArrowStreamWriterClass
{
  GObjectClass parent_class;
};
#endif

GType garrow_stream_writer_get_type(void) G_GNUC_CONST;

GArrowStreamWriter *garrow_stream_writer_new(GArrowOutputStream *sink,
                                             GArrowSchema *schema,
                                             GError **error);

gboolean garrow_stream_writer_write_record_batch(GArrowStreamWriter *stream_writer,
                                                 GArrowRecordBatch *record_batch,
                                                 GError **error);
gboolean garrow_stream_writer_close(GArrowStreamWriter *stream_writer,
                                    GError **error);


#define GARROW_TYPE_FILE_WRITER                 \
  (garrow_file_writer_get_type())
#define GARROW_FILE_WRITER(obj)                         \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_FILE_WRITER,  \
                              GArrowFileWriter))
#define GARROW_FILE_WRITER_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_FILE_WRITER,     \
                           GArrowFileWriterClass))
#define GARROW_IS_FILE_WRITER(obj)                      \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                    \
                              GARROW_TYPE_FILE_WRITER))
#define GARROW_IS_FILE_WRITER_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_FILE_WRITER))
#define GARROW_FILE_WRITER_GET_CLASS(obj)               \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_FILE_WRITER,   \
                             GArrowFileWriterClass))

typedef struct _GArrowFileWriter         GArrowFileWriter;
#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowFileWriterClass    GArrowFileWriterClass;
#endif

/**
 * GArrowFileWriter:
 *
 * It wraps `arrow::ipc::FileWriter`.
 */
struct _GArrowFileWriter
{
  /*< private >*/
  GArrowStreamWriter parent_instance;
};

#ifndef __GTK_DOC_IGNORE__
struct _GArrowFileWriterClass
{
  GArrowStreamWriterClass parent_class;
};
#endif

GType garrow_file_writer_get_type(void) G_GNUC_CONST;

GArrowFileWriter *garrow_file_writer_new(GArrowOutputStream *sink,
                                         GArrowSchema *schema,
                                         GError **error);

G_END_DECLS
