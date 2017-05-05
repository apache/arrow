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

#include <arrow-glib/input-stream.h>

#include <arrow-glib/metadata-version.h>

G_BEGIN_DECLS

#define GARROW_TYPE_STREAM_READER           \
  (garrow_stream_reader_get_type())
#define GARROW_STREAM_READER(obj)                           \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_STREAM_READER,    \
                              GArrowStreamReader))
#define GARROW_STREAM_READER_CLASS(klass)                   \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_STREAM_READER,       \
                           GArrowStreamReaderClass))
#define GARROW_IS_STREAM_READER(obj)                        \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_STREAM_READER))
#define GARROW_IS_STREAM_READER_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_STREAM_READER))
#define GARROW_STREAM_READER_GET_CLASS(obj)                 \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_STREAM_READER,     \
                             GArrowStreamReaderClass))

typedef struct _GArrowStreamReader         GArrowStreamReader;
typedef struct _GArrowStreamReaderClass    GArrowStreamReaderClass;

/**
 * GArrowStreamReader:
 *
 * It wraps `arrow::ipc::StreamReader`.
 */
struct _GArrowStreamReader
{
  /*< private >*/
  GObject parent_instance;
};

struct _GArrowStreamReaderClass
{
  GObjectClass parent_class;
};

GType garrow_stream_reader_get_type(void) G_GNUC_CONST;

GArrowStreamReader *garrow_stream_reader_new(GArrowInputStream *stream,
                                             GError **error);

GArrowSchema *garrow_stream_reader_get_schema(GArrowStreamReader *stream_reader);
GArrowRecordBatch *garrow_stream_reader_get_next_record_batch(GArrowStreamReader *stream_reader,
                                                                  GError **error);

G_END_DECLS
