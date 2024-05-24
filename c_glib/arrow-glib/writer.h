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

#define GARROW_TYPE_RECORD_BATCH_WRITER (garrow_record_batch_writer_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowRecordBatchWriter,
                         garrow_record_batch_writer,
                         GARROW,
                         RECORD_BATCH_WRITER,
                         GObject)
struct _GArrowRecordBatchWriterClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
gboolean
garrow_record_batch_writer_write_record_batch(GArrowRecordBatchWriter *writer,
                                              GArrowRecordBatch *record_batch,
                                              GError **error);
GARROW_AVAILABLE_IN_ALL
gboolean
garrow_record_batch_writer_write_table(GArrowRecordBatchWriter *writer,
                                       GArrowTable *table,
                                       GError **error);
GARROW_AVAILABLE_IN_ALL
gboolean
garrow_record_batch_writer_close(GArrowRecordBatchWriter *writer, GError **error);

#define GARROW_TYPE_RECORD_BATCH_STREAM_WRITER                                           \
  (garrow_record_batch_stream_writer_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowRecordBatchStreamWriter,
                         garrow_record_batch_stream_writer,
                         GARROW,
                         RECORD_BATCH_STREAM_WRITER,
                         GArrowRecordBatchWriter)
struct _GArrowRecordBatchStreamWriterClass
{
  GArrowRecordBatchWriterClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowRecordBatchStreamWriter *
garrow_record_batch_stream_writer_new(GArrowOutputStream *sink,
                                      GArrowSchema *schema,
                                      GError **error);

#define GARROW_TYPE_RECORD_BATCH_FILE_WRITER (garrow_record_batch_file_writer_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowRecordBatchFileWriter,
                         garrow_record_batch_file_writer,
                         GARROW,
                         RECORD_BATCH_FILE_WRITER,
                         GArrowRecordBatchStreamWriter)
struct _GArrowRecordBatchFileWriterClass
{
  GArrowRecordBatchStreamWriterClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowRecordBatchFileWriter *
garrow_record_batch_file_writer_new(GArrowOutputStream *sink,
                                    GArrowSchema *schema,
                                    GError **error);

G_END_DECLS
