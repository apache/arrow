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
#include <arrow-glib/io-file-mode.hpp>
#include <arrow-glib/io-input-stream.hpp>
#include <arrow-glib/io-memory-mapped-file.hpp>
#include <arrow-glib/io-readable.hpp>
#include <arrow-glib/io-random-access-file.hpp>
#include <arrow-glib/io-writeable.hpp>
#include <arrow-glib/io-writeable-file.hpp>

G_BEGIN_DECLS

/**
 * SECTION: io-memory-mapped-file
 * @short_description: Memory mapped file class
 *
 * #GArrowIOMemoryMappedFile is a class for memory mapped file. It's
 * readable and writeable. It supports zero copy.
 */

typedef struct GArrowIOMemoryMappedFilePrivate_ {
  std::shared_ptr<arrow::io::MemoryMappedFile> memory_mapped_file;
} GArrowIOMemoryMappedFilePrivate;

enum {
  PROP_0,
  PROP_MEMORY_MAPPED_FILE
};

static std::shared_ptr<arrow::io::FileInterface>
garrow_io_memory_mapped_file_get_raw_file_interface(GArrowIOFile *file)
{
  auto memory_mapped_file = GARROW_IO_MEMORY_MAPPED_FILE(file);
  auto arrow_memory_mapped_file =
    garrow_io_memory_mapped_file_get_raw(memory_mapped_file);
  return arrow_memory_mapped_file;
}

static void
garrow_io_file_interface_init(GArrowIOFileInterface *iface)
{
  iface->get_raw = garrow_io_memory_mapped_file_get_raw_file_interface;
}

static std::shared_ptr<arrow::io::Readable>
garrow_io_memory_mapped_file_get_raw_readable_interface(GArrowIOReadable *readable)
{
  auto memory_mapped_file = GARROW_IO_MEMORY_MAPPED_FILE(readable);
  auto arrow_memory_mapped_file =
    garrow_io_memory_mapped_file_get_raw(memory_mapped_file);
  return arrow_memory_mapped_file;
}

static void
garrow_io_readable_interface_init(GArrowIOReadableInterface *iface)
{
  iface->get_raw = garrow_io_memory_mapped_file_get_raw_readable_interface;
}

static std::shared_ptr<arrow::io::InputStream>
garrow_io_memory_mapped_file_get_raw_input_stream_interface(GArrowIOInputStream *input_stream)
{
  auto memory_mapped_file = GARROW_IO_MEMORY_MAPPED_FILE(input_stream);
  auto arrow_memory_mapped_file =
    garrow_io_memory_mapped_file_get_raw(memory_mapped_file);
  return arrow_memory_mapped_file;
}

static void
garrow_io_input_stream_interface_init(GArrowIOInputStreamInterface *iface)
{
  iface->get_raw = garrow_io_memory_mapped_file_get_raw_input_stream_interface;
}

static std::shared_ptr<arrow::io::RandomAccessFile>
garrow_io_memory_mapped_file_get_raw_random_access_file_interface(GArrowIORandomAccessFile *file)
{
  auto memory_mapped_file = GARROW_IO_MEMORY_MAPPED_FILE(file);
  auto arrow_memory_mapped_file =
    garrow_io_memory_mapped_file_get_raw(memory_mapped_file);
  return arrow_memory_mapped_file;
}

static void
garrow_io_random_access_file_interface_init(GArrowIORandomAccessFileInterface *iface)
{
  iface->get_raw = garrow_io_memory_mapped_file_get_raw_random_access_file_interface;
}

static std::shared_ptr<arrow::io::Writeable>
garrow_io_memory_mapped_file_get_raw_writeable_interface(GArrowIOWriteable *writeable)
{
  auto memory_mapped_file = GARROW_IO_MEMORY_MAPPED_FILE(writeable);
  auto arrow_memory_mapped_file =
    garrow_io_memory_mapped_file_get_raw(memory_mapped_file);
  return arrow_memory_mapped_file;
}

static void
garrow_io_writeable_interface_init(GArrowIOWriteableInterface *iface)
{
  iface->get_raw = garrow_io_memory_mapped_file_get_raw_writeable_interface;
}

static std::shared_ptr<arrow::io::WriteableFileInterface>
garrow_io_memory_mapped_file_get_raw_writeable_file_interface(GArrowIOWriteableFile *file)
{
  auto memory_mapped_file = GARROW_IO_MEMORY_MAPPED_FILE(file);
  auto arrow_memory_mapped_file =
    garrow_io_memory_mapped_file_get_raw(memory_mapped_file);
  return arrow_memory_mapped_file;
}

static void
garrow_io_writeable_file_interface_init(GArrowIOWriteableFileInterface *iface)
{
  iface->get_raw = garrow_io_memory_mapped_file_get_raw_writeable_file_interface;
}

G_DEFINE_TYPE_WITH_CODE(GArrowIOMemoryMappedFile,
                        garrow_io_memory_mapped_file,
                        G_TYPE_OBJECT,
                        G_ADD_PRIVATE(GArrowIOMemoryMappedFile)
                        G_IMPLEMENT_INTERFACE(GARROW_IO_TYPE_FILE,
                                              garrow_io_file_interface_init)
                        G_IMPLEMENT_INTERFACE(GARROW_IO_TYPE_READABLE,
                                              garrow_io_readable_interface_init)
                        G_IMPLEMENT_INTERFACE(GARROW_IO_TYPE_INPUT_STREAM,
                                              garrow_io_input_stream_interface_init)
                        G_IMPLEMENT_INTERFACE(GARROW_IO_TYPE_RANDOM_ACCESS_FILE,
                                              garrow_io_random_access_file_interface_init)
                        G_IMPLEMENT_INTERFACE(GARROW_IO_TYPE_WRITEABLE,
                                              garrow_io_writeable_interface_init)
                        G_IMPLEMENT_INTERFACE(GARROW_IO_TYPE_WRITEABLE_FILE,
                                              garrow_io_writeable_file_interface_init));

#define GARROW_IO_MEMORY_MAPPED_FILE_GET_PRIVATE(obj)                   \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                                   \
                               GARROW_IO_TYPE_MEMORY_MAPPED_FILE,       \
                               GArrowIOMemoryMappedFilePrivate))

static void
garrow_io_memory_mapped_file_finalize(GObject *object)
{
  GArrowIOMemoryMappedFilePrivate *priv;

  priv = GARROW_IO_MEMORY_MAPPED_FILE_GET_PRIVATE(object);

  priv->memory_mapped_file = nullptr;

  G_OBJECT_CLASS(garrow_io_memory_mapped_file_parent_class)->finalize(object);
}

static void
garrow_io_memory_mapped_file_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  GArrowIOMemoryMappedFilePrivate *priv;

  priv = GARROW_IO_MEMORY_MAPPED_FILE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_MEMORY_MAPPED_FILE:
    priv->memory_mapped_file =
      *static_cast<std::shared_ptr<arrow::io::MemoryMappedFile> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_io_memory_mapped_file_get_property(GObject *object,
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
garrow_io_memory_mapped_file_init(GArrowIOMemoryMappedFile *object)
{
}

static void
garrow_io_memory_mapped_file_class_init(GArrowIOMemoryMappedFileClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_io_memory_mapped_file_finalize;
  gobject_class->set_property = garrow_io_memory_mapped_file_set_property;
  gobject_class->get_property = garrow_io_memory_mapped_file_get_property;

  spec = g_param_spec_pointer("memory-mapped-file",
                              "io::MemoryMappedFile",
                              "The raw std::shared<arrow::io::MemoryMappedFile> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_MEMORY_MAPPED_FILE, spec);
}

/**
 * garrow_io_memory_mapped_file_open:
 * @path: The path of the memory mapped file.
 * @mode: The mode of the memory mapped file.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): A newly opened
 *   #GArrowIOMemoryMappedFile or %NULL on error.
 */
GArrowIOMemoryMappedFile *
garrow_io_memory_mapped_file_open(const gchar *path,
                                  GArrowIOFileMode mode,
                                  GError **error)
{
  std::shared_ptr<arrow::io::MemoryMappedFile> arrow_memory_mapped_file;
  auto status =
    arrow::io::MemoryMappedFile::Open(std::string(path),
                                      garrow_io_file_mode_to_raw(mode),
                                      &arrow_memory_mapped_file);
  if (status.ok()) {
    return garrow_io_memory_mapped_file_new_raw(&arrow_memory_mapped_file);
  } else {
    std::string context("[io][memory-mapped-file][open]: <");
    context += path;
    context += ">";
    garrow_error_set(error, status, context.c_str());
    return NULL;
  }
}

G_END_DECLS

GArrowIOMemoryMappedFile *
garrow_io_memory_mapped_file_new_raw(std::shared_ptr<arrow::io::MemoryMappedFile> *arrow_memory_mapped_file)
{
  auto memory_mapped_file =
    GARROW_IO_MEMORY_MAPPED_FILE(g_object_new(GARROW_IO_TYPE_MEMORY_MAPPED_FILE,
                                              "memory-mapped-file", arrow_memory_mapped_file,
                                              NULL));
  return memory_mapped_file;
}

std::shared_ptr<arrow::io::MemoryMappedFile>
garrow_io_memory_mapped_file_get_raw(GArrowIOMemoryMappedFile *memory_mapped_file)
{
  GArrowIOMemoryMappedFilePrivate *priv;

  priv = GARROW_IO_MEMORY_MAPPED_FILE_GET_PRIVATE(memory_mapped_file);
  return priv->memory_mapped_file;
}
