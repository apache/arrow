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

#include <arrow-glib/error.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/schema.hpp>

#include <arrow-glib/io-readable-file.hpp>

#include <arrow-glib/ipc-file-reader.hpp>
#include <arrow-glib/ipc-metadata-version.hpp>

G_BEGIN_DECLS

/**
 * SECTION: ipc-file-reader
 * @short_description: File reader class
 *
 * #GArrowIPCFileReader is a class for receiving data by file based IPC.
 */

typedef struct GArrowIPCFileReaderPrivate_ {
  std::shared_ptr<arrow::ipc::FileReader> file_reader;
} GArrowIPCFileReaderPrivate;

enum {
  PROP_0,
  PROP_FILE_READER
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowIPCFileReader,
                           garrow_ipc_file_reader,
                           G_TYPE_OBJECT);

#define GARROW_IPC_FILE_READER_GET_PRIVATE(obj)                         \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                                   \
                               GARROW_IPC_TYPE_FILE_READER,             \
                               GArrowIPCFileReaderPrivate))

static void
garrow_ipc_file_reader_finalize(GObject *object)
{
  GArrowIPCFileReaderPrivate *priv;

  priv = GARROW_IPC_FILE_READER_GET_PRIVATE(object);

  priv->file_reader = nullptr;

  G_OBJECT_CLASS(garrow_ipc_file_reader_parent_class)->finalize(object);
}

static void
garrow_ipc_file_reader_set_property(GObject *object,
                                    guint prop_id,
                                    const GValue *value,
                                    GParamSpec *pspec)
{
  GArrowIPCFileReaderPrivate *priv;

  priv = GARROW_IPC_FILE_READER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FILE_READER:
    priv->file_reader =
      *static_cast<std::shared_ptr<arrow::ipc::FileReader> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_ipc_file_reader_get_property(GObject *object,
                                          guint prop_id,
                                          GValue *value,
                                          GParamSpec *pspec)
{
  switch (prop_id) {
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_ipc_file_reader_init(GArrowIPCFileReader *object)
{
}

static void
garrow_ipc_file_reader_class_init(GArrowIPCFileReaderClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_ipc_file_reader_finalize;
  gobject_class->set_property = garrow_ipc_file_reader_set_property;
  gobject_class->get_property = garrow_ipc_file_reader_get_property;

  spec = g_param_spec_pointer("file-reader",
                              "ipc::FileReader",
                              "The raw std::shared<arrow::ipc::FileReader> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FILE_READER, spec);
}

/**
 * garrow_ipc_file_reader_open:
 * @file: The file to be read.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): A newly opened
 *   #GArrowIPCFileReader or %NULL on error.
 */
GArrowIPCFileReader *
garrow_ipc_file_reader_open(GArrowIOReadableFile *file,
                            GError **error)
{
  std::shared_ptr<arrow::ipc::FileReader> arrow_file_reader;
  auto status =
    arrow::ipc::FileReader::Open(garrow_io_readable_file_get_raw(file),
                                 &arrow_file_reader);
  if (status.ok()) {
    return garrow_ipc_file_reader_new_raw(&arrow_file_reader);
  } else {
    garrow_error_set(error, status, "[ipc][file-reader][open]");
    return NULL;
  }
}

/**
 * garrow_ipc_file_reader_get_schema:
 * @file_reader: A #GArrowIPCFileReader.
 *
 * Returns: (transfer full): The schema in the file.
 */
GArrowSchema *
garrow_ipc_file_reader_get_schema(GArrowIPCFileReader *file_reader)
{
  auto arrow_file_reader =
    garrow_ipc_file_reader_get_raw(file_reader);
  auto arrow_schema = arrow_file_reader->schema();
  return garrow_schema_new_raw(&arrow_schema);
}

/**
 * garrow_ipc_file_reader_get_n_record_batches:
 * @file_reader: A #GArrowIPCFileReader.
 *
 * Returns: The number of record batches in the file.
 */
guint
garrow_ipc_file_reader_get_n_record_batches(GArrowIPCFileReader *file_reader)
{
  auto arrow_file_reader =
    garrow_ipc_file_reader_get_raw(file_reader);
  return arrow_file_reader->num_record_batches();
}

/**
 * garrow_ipc_file_reader_get_version:
 * @file_reader: A #GArrowIPCFileReader.
 *
 * Returns: The format version in the file.
 */
GArrowIPCMetadataVersion
garrow_ipc_file_reader_get_version(GArrowIPCFileReader *file_reader)
{
  auto arrow_file_reader =
    garrow_ipc_file_reader_get_raw(file_reader);
  auto arrow_version = arrow_file_reader->version();
  return garrow_ipc_metadata_version_from_raw(arrow_version);
}

/**
 * garrow_ipc_file_reader_get_record_batch:
 * @file_reader: A #GArrowIPCFileReader.
 * @i: The index of the target record batch.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   The i-th record batch in the file or %NULL on error.
 */
GArrowRecordBatch *
garrow_ipc_file_reader_get_record_batch(GArrowIPCFileReader *file_reader,
                                        guint i,
                                        GError **error)
{
  auto arrow_file_reader =
    garrow_ipc_file_reader_get_raw(file_reader);
  std::shared_ptr<arrow::RecordBatch> arrow_record_batch;
  auto status = arrow_file_reader->GetRecordBatch(i, &arrow_record_batch);

  if (status.ok()) {
    return garrow_record_batch_new_raw(&arrow_record_batch);
  } else {
    garrow_error_set(error, status, "[ipc][file-reader][get-record-batch]");
    return NULL;
  }
}

G_END_DECLS

GArrowIPCFileReader *
garrow_ipc_file_reader_new_raw(std::shared_ptr<arrow::ipc::FileReader> *arrow_file_reader)
{
  auto file_reader =
    GARROW_IPC_FILE_READER(g_object_new(GARROW_IPC_TYPE_FILE_READER,
                                        "file-reader", arrow_file_reader,
                                        NULL));
  return file_reader;
}

std::shared_ptr<arrow::ipc::FileReader>
garrow_ipc_file_reader_get_raw(GArrowIPCFileReader *file_reader)
{
  GArrowIPCFileReaderPrivate *priv;

  priv = GARROW_IPC_FILE_READER_GET_PRIVATE(file_reader);
  return priv->file_reader;
}
