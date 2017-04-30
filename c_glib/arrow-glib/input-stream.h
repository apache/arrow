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

#include <arrow-glib/buffer.h>

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


#define GARROW_TYPE_BUFFER_READER               \
  (garrow_buffer_reader_get_type())
#define GARROW_BUFFER_READER(obj)                               \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_BUFFER_READER,        \
                              GArrowBufferReader))
#define GARROW_BUFFER_READER_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_BUFFER_READER,   \
                           GArrowBufferReaderClass))
#define GARROW_IS_BUFFER_READER(obj)                            \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_BUFFER_READER))
#define GARROW_IS_BUFFER_READER_CLASS(klass)            \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_BUFFER_READER))
#define GARROW_BUFFER_READER_GET_CLASS(obj)             \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_BUFFER_READER, \
                             GArrowBufferReaderClass))

typedef struct _GArrowBufferReader         GArrowBufferReader;
#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowBufferReaderClass    GArrowBufferReaderClass;
#endif

/**
 * GArrowBufferReader:
 *
 * It wraps `arrow::io::BufferReader`.
 */
struct _GArrowBufferReader
{
  /*< private >*/
  GArrowInputStream parent_instance;
};

#ifndef __GTK_DOC_IGNORE__
struct _GArrowBufferReaderClass
{
  GArrowInputStreamClass parent_class;
};
#endif

GType garrow_buffer_reader_get_type(void) G_GNUC_CONST;

GArrowBufferReader *garrow_buffer_reader_new(GArrowBuffer *buffer);

GArrowBuffer *garrow_buffer_reader_get_buffer(GArrowBufferReader *buffer_reader);

G_END_DECLS
