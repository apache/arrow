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

#include <arrow/api.h>
#include <arrow/io/memory.h>

#include <arrow-glib/buffer.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/file.hpp>
#include <arrow-glib/output-stream.hpp>
#include <arrow-glib/writeable.hpp>

G_BEGIN_DECLS

/**
 * SECTION: output-stream
 * @section_id: output-stream-classes
 * @title: Output stream classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowOutputStream is an interface for stream output. Stream
 * output is file based and writeable
 *
 * #GArrowFileOutputStream is a class for file output stream.
 *
 * #GArrowBufferOutputStream is a class for buffer output stream.
 */

typedef struct GArrowOutputStreamPrivate_ {
  std::shared_ptr<arrow::io::OutputStream> output_stream;
} GArrowOutputStreamPrivate;

enum {
  PROP_0,
  PROP_OUTPUT_STREAM
};

static std::shared_ptr<arrow::io::FileInterface>
garrow_output_stream_get_raw_file_interface(GArrowFile *file)
{
  auto output_stream = GARROW_OUTPUT_STREAM(file);
  auto arrow_output_stream = garrow_output_stream_get_raw(output_stream);
  return arrow_output_stream;
}

static void
garrow_output_stream_file_interface_init(GArrowFileInterface *iface)
{
  iface->get_raw = garrow_output_stream_get_raw_file_interface;
}

static std::shared_ptr<arrow::io::Writeable>
garrow_output_stream_get_raw_writeable_interface(GArrowWriteable *writeable)
{
  auto output_stream = GARROW_OUTPUT_STREAM(writeable);
  auto arrow_output_stream = garrow_output_stream_get_raw(output_stream);
  return arrow_output_stream;
}

static void
garrow_output_stream_writeable_interface_init(GArrowWriteableInterface *iface)
{
  iface->get_raw = garrow_output_stream_get_raw_writeable_interface;
}

G_DEFINE_TYPE_WITH_CODE(GArrowOutputStream,
                        garrow_output_stream,
                        G_TYPE_OBJECT,
                        G_ADD_PRIVATE(GArrowOutputStream)
                        G_IMPLEMENT_INTERFACE(GARROW_TYPE_FILE,
                                              garrow_output_stream_file_interface_init)
                        G_IMPLEMENT_INTERFACE(GARROW_TYPE_WRITEABLE,
                                              garrow_output_stream_writeable_interface_init));

#define GARROW_OUTPUT_STREAM_GET_PRIVATE(obj)                   \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                           \
                               GARROW_TYPE_OUTPUT_STREAM,       \
                               GArrowOutputStreamPrivate))

static void
garrow_output_stream_finalize(GObject *object)
{
  GArrowOutputStreamPrivate *priv;

  priv = GARROW_OUTPUT_STREAM_GET_PRIVATE(object);

  priv->output_stream = nullptr;

  G_OBJECT_CLASS(garrow_output_stream_parent_class)->finalize(object);
}

static void
garrow_output_stream_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  GArrowOutputStreamPrivate *priv;

  priv = GARROW_OUTPUT_STREAM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_OUTPUT_STREAM:
    priv->output_stream =
      *static_cast<std::shared_ptr<arrow::io::OutputStream> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_output_stream_get_property(GObject *object,
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
garrow_output_stream_init(GArrowOutputStream *object)
{
}

static void
garrow_output_stream_class_init(GArrowOutputStreamClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_output_stream_finalize;
  gobject_class->set_property = garrow_output_stream_set_property;
  gobject_class->get_property = garrow_output_stream_get_property;

  spec = g_param_spec_pointer("output-stream",
                              "io::OutputStream",
                              "The raw std::shared<arrow::io::OutputStream> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_OUTPUT_STREAM, spec);
}


G_DEFINE_TYPE(GArrowFileOutputStream,
              garrow_file_output_stream,
              GARROW_TYPE_OUTPUT_STREAM);

static void
garrow_file_output_stream_init(GArrowFileOutputStream *file_output_stream)
{
}

static void
garrow_file_output_stream_class_init(GArrowFileOutputStreamClass *klass)
{
}

/**
 * garrow_file_output_stream_open:
 * @path: The path of the file output stream.
 * @append: Whether the path is opened as append mode or recreate mode.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): A newly opened
 *   #GArrowFileOutputStream or %NULL on error.
 */
GArrowFileOutputStream *
garrow_file_output_stream_open(const gchar *path,
                               gboolean append,
                               GError **error)
{
  std::shared_ptr<arrow::io::FileOutputStream> arrow_file_output_stream;
  auto status =
    arrow::io::FileOutputStream::Open(std::string(path),
                                      append,
                                      &arrow_file_output_stream);
  if (status.ok()) {
    return garrow_file_output_stream_new_raw(&arrow_file_output_stream);
  } else {
    std::string context("[io][file-output-stream][open]: <");
    context += path;
    context += ">";
    garrow_error_check(error, status, context.c_str());
    return NULL;
  }
}


G_DEFINE_TYPE(GArrowBufferOutputStream,
              garrow_buffer_output_stream,
              GARROW_TYPE_OUTPUT_STREAM);

static void
garrow_buffer_output_stream_init(GArrowBufferOutputStream *buffer_output_stream)
{
}

static void
garrow_buffer_output_stream_class_init(GArrowBufferOutputStreamClass *klass)
{
}

/**
 * garrow_buffer_output_stream_new:
 * @buffer: The resizable buffer to be output.
 *
 * Returns: (transfer full): A newly created #GArrowBufferOutputStream.
 */
GArrowBufferOutputStream *
garrow_buffer_output_stream_new(GArrowResizableBuffer *buffer)
{
  auto arrow_buffer = garrow_buffer_get_raw(GARROW_BUFFER(buffer));
  auto arrow_resizable_buffer =
    std::static_pointer_cast<arrow::ResizableBuffer>(arrow_buffer);
  auto arrow_buffer_output_stream =
    std::make_shared<arrow::io::BufferOutputStream>(arrow_resizable_buffer);
  return garrow_buffer_output_stream_new_raw(&arrow_buffer_output_stream);
}
G_END_DECLS

GArrowOutputStream *
garrow_output_stream_new_raw(std::shared_ptr<arrow::io::OutputStream> *arrow_output_stream)
{
  auto output_stream =
    GARROW_OUTPUT_STREAM(g_object_new(GARROW_TYPE_OUTPUT_STREAM,
                                      "output-stream", arrow_output_stream,
                                      NULL));
  return output_stream;
}

std::shared_ptr<arrow::io::OutputStream>
garrow_output_stream_get_raw(GArrowOutputStream *output_stream)
{
  GArrowOutputStreamPrivate *priv;

  priv = GARROW_OUTPUT_STREAM_GET_PRIVATE(output_stream);
  return priv->output_stream;
}


GArrowFileOutputStream *
garrow_file_output_stream_new_raw(std::shared_ptr<arrow::io::FileOutputStream> *arrow_file_output_stream)
{
  auto file_output_stream =
    GARROW_FILE_OUTPUT_STREAM(g_object_new(GARROW_TYPE_FILE_OUTPUT_STREAM,
                                           "output-stream", arrow_file_output_stream,
                                           NULL));
  return file_output_stream;
}

GArrowBufferOutputStream *
garrow_buffer_output_stream_new_raw(std::shared_ptr<arrow::io::BufferOutputStream> *arrow_buffer_output_stream)
{
  auto buffer_output_stream =
    GARROW_BUFFER_OUTPUT_STREAM(g_object_new(GARROW_TYPE_BUFFER_OUTPUT_STREAM,
                                             "output-stream", arrow_buffer_output_stream,
                                             NULL));
  return buffer_output_stream;
}
