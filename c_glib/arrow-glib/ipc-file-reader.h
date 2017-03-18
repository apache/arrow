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

#include <arrow-glib/record-batch.h>
#include <arrow-glib/schema.h>

#include <arrow-glib/io-random-access-file.h>

#include <arrow-glib/ipc-metadata-version.h>

G_BEGIN_DECLS

#define GARROW_IPC_TYPE_FILE_READER      \
  (garrow_ipc_file_reader_get_type())
#define GARROW_IPC_FILE_READER(obj)                             \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_IPC_TYPE_FILE_READER,      \
                              GArrowIPCFileReader))
#define GARROW_IPC_FILE_READER_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_IPC_TYPE_FILE_READER, \
                           GArrowIPCFileReaderClass))
#define GARROW_IPC_IS_FILE_READER(obj)                          \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_IPC_TYPE_FILE_READER))
#define GARROW_IPC_IS_FILE_READER_CLASS(klass)                  \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_IPC_TYPE_FILE_READER))
#define GARROW_IPC_FILE_READER_GET_CLASS(obj)                   \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_IPC_TYPE_FILE_READER,       \
                             GArrowIPCFileReaderClass))

typedef struct _GArrowIPCFileReader         GArrowIPCFileReader;
typedef struct _GArrowIPCFileReaderClass    GArrowIPCFileReaderClass;

/**
 * GArrowIPCFileReader:
 *
 * It wraps `arrow::ipc::FileReader`.
 */
struct _GArrowIPCFileReader
{
  /*< private >*/
  GObject parent_instance;
};

struct _GArrowIPCFileReaderClass
{
  GObjectClass parent_class;
};

GType garrow_ipc_file_reader_get_type(void) G_GNUC_CONST;

GArrowIPCFileReader *garrow_ipc_file_reader_open(GArrowIORandomAccessFile *file,
                                                 GError **error);

GArrowSchema *garrow_ipc_file_reader_get_schema(GArrowIPCFileReader *file_reader);
guint garrow_ipc_file_reader_get_n_record_batches(GArrowIPCFileReader *file_reader);
GArrowIPCMetadataVersion garrow_ipc_file_reader_get_version(GArrowIPCFileReader *file_reader);
GArrowRecordBatch *garrow_ipc_file_reader_get_record_batch(GArrowIPCFileReader *file_reader,
                                                           guint i,
                                                           GError **error);

G_END_DECLS
