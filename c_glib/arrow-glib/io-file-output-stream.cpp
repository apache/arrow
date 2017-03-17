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

#include <arrow/io/file.h>

#include <arrow-glib/error.hpp>
#include <arrow-glib/io-file.hpp>
#include <arrow-glib/io-file-output-stream.hpp>
#include <arrow-glib/io-output-stream.hpp>
#include <arrow-glib/io-writeable.hpp>

G_BEGIN_DECLS

/**
 * SECTION: io-file-output-stream
 * @short_description: A file output stream.
 *
 * The #GArrowIOFileOutputStream is a class for file output stream.
 */

typedef struct GArrowIOFileOutputStreamPrivate_ {
  std::shared_ptr<arrow::io::FileOutputStream> file_output_stream;
} GArrowIOFileOutputStreamPrivate;

enum {
  PROP_0,
  PROP_FILE_OUTPUT_STREAM
};

static std::shared_ptr<arrow::io::FileInterface>
garrow_io_file_output_stream_get_raw_file_interface(GArrowIOFile *file)
{
  auto file_output_stream = GARROW_IO_FILE_OUTPUT_STREAM(file);
  auto arrow_file_output_stream =
    garrow_io_file_output_stream_get_raw(file_output_stream);
  return arrow_file_output_stream;
}

static void
garrow_io_file_interface_init(GArrowIOFileInterface *iface)
{
  iface->get_raw = garrow_io_file_output_stream_get_raw_file_interface;
}

static std::shared_ptr<arrow::io::Writeable>
garrow_io_file_output_stream_get_raw_writeable_interface(GArrowIOWriteable *writeable)
{
  auto file_output_stream = GARROW_IO_FILE_OUTPUT_STREAM(writeable);
  auto arrow_file_output_stream =
    garrow_io_file_output_stream_get_raw(file_output_stream);
  return arrow_file_output_stream;
}

static void
garrow_io_writeable_interface_init(GArrowIOWriteableInterface *iface)
{
  iface->get_raw = garrow_io_file_output_stream_get_raw_writeable_interface;
}

static std::shared_ptr<arrow::io::OutputStream>
garrow_io_file_output_stream_get_raw_output_stream_interface(GArrowIOOutputStream *output_stream)
{
  auto file_output_stream = GARROW_IO_FILE_OUTPUT_STREAM(output_stream);
  auto arrow_file_output_stream =
    garrow_io_file_output_stream_get_raw(file_output_stream);
  return arrow_file_output_stream;
}

static void
garrow_io_output_stream_interface_init(GArrowIOOutputStreamInterface *iface)
{
  iface->get_raw = garrow_io_file_output_stream_get_raw_output_stream_interface;
}

G_DEFINE_TYPE_WITH_CODE(GArrowIOFileOutputStream,
                        garrow_io_file_output_stream,
                        G_TYPE_OBJECT,
                        G_ADD_PRIVATE(GArrowIOFileOutputStream)
                        G_IMPLEMENT_INTERFACE(GARROW_IO_TYPE_FILE,
                                              garrow_io_file_interface_init)
                        G_IMPLEMENT_INTERFACE(GARROW_IO_TYPE_WRITEABLE,
                                              garrow_io_writeable_interface_init)
                        G_IMPLEMENT_INTERFACE(GARROW_IO_TYPE_OUTPUT_STREAM,
                                              garrow_io_output_stream_interface_init));

#define GARROW_IO_FILE_OUTPUT_STREAM_GET_PRIVATE(obj)                   \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                                   \
                               GARROW_IO_TYPE_FILE_OUTPUT_STREAM,       \
                               GArrowIOFileOutputStreamPrivate))

static void
garrow_io_file_output_stream_finalize(GObject *object)
{
  GArrowIOFileOutputStreamPrivate *priv;

  priv = GARROW_IO_FILE_OUTPUT_STREAM_GET_PRIVATE(object);

  priv->file_output_stream = nullptr;

  G_OBJECT_CLASS(garrow_io_file_output_stream_parent_class)->finalize(object);
}

static void
garrow_io_file_output_stream_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  GArrowIOFileOutputStreamPrivate *priv;

  priv = GARROW_IO_FILE_OUTPUT_STREAM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FILE_OUTPUT_STREAM:
    priv->file_output_stream =
      *static_cast<std::shared_ptr<arrow::io::FileOutputStream> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_io_file_output_stream_get_property(GObject *object,
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
garrow_io_file_output_stream_init(GArrowIOFileOutputStream *object)
{
}

static void
garrow_io_file_output_stream_class_init(GArrowIOFileOutputStreamClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_io_file_output_stream_finalize;
  gobject_class->set_property = garrow_io_file_output_stream_set_property;
  gobject_class->get_property = garrow_io_file_output_stream_get_property;

  spec = g_param_spec_pointer("file-output-stream",
                              "io::FileOutputStream",
                              "The raw std::shared<arrow::io::FileOutputStream> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FILE_OUTPUT_STREAM, spec);
}

/**
 * garrow_io_file_output_stream_open:
 * @path: The path of the file output stream.
 * @append: Whether the path is opened as append mode or recreate mode.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): A newly opened
 *   #GArrowIOFileOutputStream or %NULL on error.
 */
GArrowIOFileOutputStream *
garrow_io_file_output_stream_open(const gchar *path,
                                  gboolean append,
                                  GError **error)
{
  std::shared_ptr<arrow::io::FileOutputStream> arrow_file_output_stream;
  auto status =
    arrow::io::FileOutputStream::Open(std::string(path),
                                      append,
                                      &arrow_file_output_stream);
  if (status.ok()) {
    return garrow_io_file_output_stream_new_raw(&arrow_file_output_stream);
  } else {
    std::string context("[io][file-output-stream][open]: <");
    context += path;
    context += ">";
    garrow_error_set(error, status, context.c_str());
    return NULL;
  }
}

G_END_DECLS

GArrowIOFileOutputStream *
garrow_io_file_output_stream_new_raw(std::shared_ptr<arrow::io::FileOutputStream> *arrow_file_output_stream)
{
  auto file_output_stream =
    GARROW_IO_FILE_OUTPUT_STREAM(g_object_new(GARROW_IO_TYPE_FILE_OUTPUT_STREAM,
                                              "file-output-stream", arrow_file_output_stream,
                                              NULL));
  return file_output_stream;
}

std::shared_ptr<arrow::io::FileOutputStream>
garrow_io_file_output_stream_get_raw(GArrowIOFileOutputStream *file_output_stream)
{
  GArrowIOFileOutputStreamPrivate *priv;

  priv = GARROW_IO_FILE_OUTPUT_STREAM_GET_PRIVATE(file_output_stream);
  return priv->file_output_stream;
}
