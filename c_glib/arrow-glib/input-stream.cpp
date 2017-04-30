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

#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>

#include <arrow-glib/buffer.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/file.hpp>
#include <arrow-glib/input-stream.hpp>
#include <arrow-glib/random-access-file.hpp>
#include <arrow-glib/readable.hpp>

G_BEGIN_DECLS

/**
 * SECTION: input-stream
 * @section_id: input-stream-classes
 * @title: Input stream classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowInputStream is a base class for input stream.
 *
 * #GArrowBufferReader is a class for buffer input stream.
 */

typedef struct GArrowInputStreamPrivate_ {
  std::shared_ptr<arrow::io::InputStream> input_stream;
} GArrowInputStreamPrivate;

enum {
  PROP_0,
  PROP_INPUT_STREAM
};

static std::shared_ptr<arrow::io::FileInterface>
garrow_input_stream_get_raw_file_interface(GArrowFile *file)
{
  auto input_stream = GARROW_INPUT_STREAM(file);
  auto arrow_input_stream =
    garrow_input_stream_get_raw(input_stream);
  return arrow_input_stream;
}

static void
garrow_input_stream_file_interface_init(GArrowFileInterface *iface)
{
  iface->get_raw = garrow_input_stream_get_raw_file_interface;
}

static std::shared_ptr<arrow::io::Readable>
garrow_input_stream_get_raw_readable_interface(GArrowReadable *readable)
{
  auto input_stream = GARROW_INPUT_STREAM(readable);
  auto arrow_input_stream = garrow_input_stream_get_raw(input_stream);
  return arrow_input_stream;
}

static void
garrow_input_stream_readable_interface_init(GArrowReadableInterface *iface)
{
  iface->get_raw = garrow_input_stream_get_raw_readable_interface;
}

G_DEFINE_TYPE_WITH_CODE(GArrowInputStream,
                        garrow_input_stream,
                        G_TYPE_OBJECT,
                        G_ADD_PRIVATE(GArrowInputStream)
                        G_IMPLEMENT_INTERFACE(GARROW_TYPE_FILE,
                                              garrow_input_stream_file_interface_init)
                        G_IMPLEMENT_INTERFACE(GARROW_TYPE_READABLE,
                                              garrow_input_stream_readable_interface_init));

#define GARROW_INPUT_STREAM_GET_PRIVATE(obj)                    \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                           \
                               GARROW_TYPE_INPUT_STREAM,        \
                               GArrowInputStreamPrivate))

static void
garrow_input_stream_finalize(GObject *object)
{
  GArrowInputStreamPrivate *priv;

  priv = GARROW_INPUT_STREAM_GET_PRIVATE(object);

  priv->input_stream = nullptr;

  G_OBJECT_CLASS(garrow_input_stream_parent_class)->finalize(object);
}

static void
garrow_input_stream_set_property(GObject *object,
                                 guint prop_id,
                                 const GValue *value,
                                 GParamSpec *pspec)
{
  GArrowInputStreamPrivate *priv;

  priv = GARROW_INPUT_STREAM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_INPUT_STREAM:
    priv->input_stream =
      *static_cast<std::shared_ptr<arrow::io::InputStream> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_input_stream_get_property(GObject *object,
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
garrow_input_stream_init(GArrowInputStream *object)
{
}

static void
garrow_input_stream_class_init(GArrowInputStreamClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_input_stream_finalize;
  gobject_class->set_property = garrow_input_stream_set_property;
  gobject_class->get_property = garrow_input_stream_get_property;

  spec = g_param_spec_pointer("input-stream",
                              "Input stream",
                              "The raw std::shared<arrow::io::InputStream> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_INPUT_STREAM, spec);
}


static std::shared_ptr<arrow::io::RandomAccessFile>
garrow_buffer_reader_get_raw_random_access_file_interface(GArrowRandomAccessFile *random_access_file)
{
  auto input_stream = GARROW_INPUT_STREAM(random_access_file);
  auto arrow_input_stream = garrow_input_stream_get_raw(input_stream);
  auto arrow_buffer_reader =
    std::static_pointer_cast<arrow::io::BufferReader>(arrow_input_stream);
  return arrow_buffer_reader;
}

static void
garrow_buffer_reader_random_access_file_interface_init(GArrowRandomAccessFileInterface *iface)
{
  iface->get_raw = garrow_buffer_reader_get_raw_random_access_file_interface;
}

G_DEFINE_TYPE_WITH_CODE(GArrowBufferReader,               \
                        garrow_buffer_reader,             \
                        GARROW_TYPE_INPUT_STREAM,
                        G_IMPLEMENT_INTERFACE(GARROW_TYPE_RANDOM_ACCESS_FILE,
                                              garrow_buffer_reader_random_access_file_interface_init));

static void
garrow_buffer_reader_init(GArrowBufferReader *object)
{
}

static void
garrow_buffer_reader_class_init(GArrowBufferReaderClass *klass)
{
}

/**
 * garrow_buffer_reader_new:
 * @buffer: The buffer to be read.
 *
 * Returns: A newly created #GArrowBufferReader.
 */
GArrowBufferReader *
garrow_buffer_reader_new(GArrowBuffer *buffer)
{
  auto arrow_buffer = garrow_buffer_get_raw(buffer);
  auto arrow_buffer_reader =
    std::make_shared<arrow::io::BufferReader>(arrow_buffer);
  return garrow_buffer_reader_new_raw(&arrow_buffer_reader);
}

/**
 * garrow_buffer_reader_get_buffer:
 * @buffer_reader: A #GArrowBufferReader.
 *
 * Returns: (transfer full): The data of the array as #GArrowBuffer.
 */
GArrowBuffer *
garrow_buffer_reader_get_buffer(GArrowBufferReader *buffer_reader)
{
  auto arrow_input_stream =
    garrow_input_stream_get_raw(GARROW_INPUT_STREAM(buffer_reader));
  auto arrow_buffer_reader =
    std::static_pointer_cast<arrow::io::BufferReader>(arrow_input_stream);
  auto arrow_buffer = arrow_buffer_reader->buffer();
  return garrow_buffer_new_raw(&arrow_buffer);
}


G_END_DECLS

GArrowInputStream *
garrow_input_stream_new_raw(std::shared_ptr<arrow::io::InputStream> *arrow_input_stream)
{
  auto input_stream =
    GARROW_INPUT_STREAM(g_object_new(GARROW_TYPE_INPUT_STREAM,
                                     "input-stream", arrow_input_stream,
                                     NULL));
  return input_stream;
}

std::shared_ptr<arrow::io::InputStream>
garrow_input_stream_get_raw(GArrowInputStream *input_stream)
{
  GArrowInputStreamPrivate *priv;

  priv = GARROW_INPUT_STREAM_GET_PRIVATE(input_stream);
  return priv->input_stream;
}


GArrowBufferReader *
garrow_buffer_reader_new_raw(std::shared_ptr<arrow::io::BufferReader> *arrow_buffer_reader)
{
  auto buffer_reader =
    GARROW_BUFFER_READER(g_object_new(GARROW_TYPE_BUFFER_READER,
                                      "input-stream", arrow_buffer_reader,
                                      NULL));
  return buffer_reader;
}
