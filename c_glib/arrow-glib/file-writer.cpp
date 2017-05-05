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

#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include <arrow/ipc/api.h>

#include <arrow-glib/array.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/schema.hpp>

#include <arrow-glib/output-stream.hpp>

#include <arrow-glib/stream-writer.hpp>
#include <arrow-glib/file-writer.hpp>

G_BEGIN_DECLS

/**
 * SECTION: file-writer
 * @short_description: File writer class
 *
 * #GArrowFileWriter is a class for sending data by file based IPC.
 */

G_DEFINE_TYPE(GArrowFileWriter,
              garrow_file_writer,
              GARROW_TYPE_STREAM_WRITER);

static void
garrow_file_writer_init(GArrowFileWriter *object)
{
}

static void
garrow_file_writer_class_init(GArrowFileWriterClass *klass)
{
}

/**
 * garrow_file_writer_new:
 * @sink: The output of the writer.
 * @schema: The schema of the writer.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowFileWriter or %NULL on
 *   error.
 */
GArrowFileWriter *
garrow_file_writer_new(GArrowOutputStream *sink,
                       GArrowSchema *schema,
                       GError **error)
{
  std::shared_ptr<arrow::ipc::FileWriter> arrow_file_writer;
  auto status =
    arrow::ipc::FileWriter::Open(garrow_output_stream_get_raw(sink).get(),
                                 garrow_schema_get_raw(schema),
                                 &arrow_file_writer);
  if (garrow_error_check(error, status, "[ipc][file-writer][open]")) {
    return garrow_file_writer_new_raw(&arrow_file_writer);
  } else {
    return NULL;
  }
}

/**
 * garrow_file_writer_write_record_batch:
 * @file_writer: A #GArrowFileWriter.
 * @record_batch: The record batch to be written.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 */
gboolean
garrow_file_writer_write_record_batch(GArrowFileWriter *file_writer,
                                          GArrowRecordBatch *record_batch,
                                          GError **error)
{
  auto arrow_file_writer =
    garrow_file_writer_get_raw(file_writer);
  auto arrow_record_batch =
    garrow_record_batch_get_raw(record_batch);
  auto arrow_record_batch_raw =
    arrow_record_batch.get();

  auto status = arrow_file_writer->WriteRecordBatch(*arrow_record_batch_raw);
  return garrow_error_check(error,
                            status,
                            "[ipc][file-writer][write-record-batch]");
}

/**
 * garrow_file_writer_close:
 * @file_writer: A #GArrowFileWriter.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 */
gboolean
garrow_file_writer_close(GArrowFileWriter *file_writer,
                             GError **error)
{
  auto arrow_file_writer =
    garrow_file_writer_get_raw(file_writer);

  auto status = arrow_file_writer->Close();
  return garrow_error_check(error, status, "[ipc][file-writer][close]");
}

G_END_DECLS

GArrowFileWriter *
garrow_file_writer_new_raw(std::shared_ptr<arrow::ipc::FileWriter> *arrow_file_writer)
{
  auto file_writer =
    GARROW_FILE_WRITER(g_object_new(GARROW_TYPE_FILE_WRITER,
                                        "stream-writer", arrow_file_writer,
                                        NULL));
  return file_writer;
}

arrow::ipc::FileWriter *
garrow_file_writer_get_raw(GArrowFileWriter *file_writer)
{
  auto arrow_stream_writer =
    garrow_stream_writer_get_raw(GARROW_STREAM_WRITER(file_writer));
  auto arrow_file_writer_raw =
    dynamic_cast<arrow::ipc::FileWriter *>(arrow_stream_writer.get());
  return arrow_file_writer_raw;
}
