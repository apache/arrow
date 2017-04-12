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
#include <arrow-glib/file.hpp>
#include <arrow-glib/file-mode.hpp>
#include <arrow-glib/input-stream.hpp>
#include <arrow-glib/memory-mapped-file.hpp>
#include <arrow-glib/readable.hpp>
#include <arrow-glib/random-access-file.hpp>
#include <arrow-glib/writeable.hpp>
#include <arrow-glib/writeable-file.hpp>

G_BEGIN_DECLS

/**
 * SECTION: memory-mapped-file
 * @short_description: Memory mapped file class
 *
 * #GArrowMemoryMappedFile is a class for memory mapped file. It's
 * readable and writeable. It supports zero copy.
 */

typedef struct GArrowMemoryMappedFilePrivate_ {
  std::shared_ptr<arrow::io::MemoryMappedFile> memory_mapped_file;
} GArrowMemoryMappedFilePrivate;

enum {
  PROP_0,
  PROP_MEMORY_MAPPED_FILE
};

static std::shared_ptr<arrow::io::FileInterface>
garrow_memory_mapped_file_get_raw_file_interface(GArrowFile *file)
{
  auto memory_mapped_file = GARROW_MEMORY_MAPPED_FILE(file);
  auto arrow_memory_mapped_file =
    garrow_memory_mapped_file_get_raw(memory_mapped_file);
  return arrow_memory_mapped_file;
}

static void
garrow_file_interface_init(GArrowFileInterface *iface)
{
  iface->get_raw = garrow_memory_mapped_file_get_raw_file_interface;
}

static std::shared_ptr<arrow::io::Readable>
garrow_memory_mapped_file_get_raw_readable_interface(GArrowReadable *readable)
{
  auto memory_mapped_file = GARROW_MEMORY_MAPPED_FILE(readable);
  auto arrow_memory_mapped_file =
    garrow_memory_mapped_file_get_raw(memory_mapped_file);
  return arrow_memory_mapped_file;
}

static void
garrow_readable_interface_init(GArrowReadableInterface *iface)
{
  iface->get_raw = garrow_memory_mapped_file_get_raw_readable_interface;
}

static std::shared_ptr<arrow::io::InputStream>
garrow_memory_mapped_file_get_raw_input_stream_interface(GArrowInputStream *input_stream)
{
  auto memory_mapped_file = GARROW_MEMORY_MAPPED_FILE(input_stream);
  auto arrow_memory_mapped_file =
    garrow_memory_mapped_file_get_raw(memory_mapped_file);
  return arrow_memory_mapped_file;
}

static void
garrow_input_stream_interface_init(GArrowInputStreamInterface *iface)
{
  iface->get_raw = garrow_memory_mapped_file_get_raw_input_stream_interface;
}

static std::shared_ptr<arrow::io::RandomAccessFile>
garrow_memory_mapped_file_get_raw_random_access_file_interface(GArrowRandomAccessFile *file)
{
  auto memory_mapped_file = GARROW_MEMORY_MAPPED_FILE(file);
  auto arrow_memory_mapped_file =
    garrow_memory_mapped_file_get_raw(memory_mapped_file);
  return arrow_memory_mapped_file;
}

static void
garrow_random_access_file_interface_init(GArrowRandomAccessFileInterface *iface)
{
  iface->get_raw = garrow_memory_mapped_file_get_raw_random_access_file_interface;
}

static std::shared_ptr<arrow::io::Writeable>
garrow_memory_mapped_file_get_raw_writeable_interface(GArrowWriteable *writeable)
{
  auto memory_mapped_file = GARROW_MEMORY_MAPPED_FILE(writeable);
  auto arrow_memory_mapped_file =
    garrow_memory_mapped_file_get_raw(memory_mapped_file);
  return arrow_memory_mapped_file;
}

static void
garrow_writeable_interface_init(GArrowWriteableInterface *iface)
{
  iface->get_raw = garrow_memory_mapped_file_get_raw_writeable_interface;
}

static std::shared_ptr<arrow::io::WriteableFile>
garrow_memory_mapped_file_get_raw_writeable_file_interface(GArrowWriteableFile *file)
{
  auto memory_mapped_file = GARROW_MEMORY_MAPPED_FILE(file);
  auto arrow_memory_mapped_file =
    garrow_memory_mapped_file_get_raw(memory_mapped_file);
  return arrow_memory_mapped_file;
}

static void
garrow_writeable_file_interface_init(GArrowWriteableFileInterface *iface)
{
  iface->get_raw = garrow_memory_mapped_file_get_raw_writeable_file_interface;
}

G_DEFINE_TYPE_WITH_CODE(GArrowMemoryMappedFile,
                        garrow_memory_mapped_file,
                        G_TYPE_OBJECT,
                        G_ADD_PRIVATE(GArrowMemoryMappedFile)
                        G_IMPLEMENT_INTERFACE(GARROW_TYPE_FILE,
                                              garrow_file_interface_init)
                        G_IMPLEMENT_INTERFACE(GARROW_TYPE_READABLE,
                                              garrow_readable_interface_init)
                        G_IMPLEMENT_INTERFACE(GARROW_TYPE_INPUT_STREAM,
                                              garrow_input_stream_interface_init)
                        G_IMPLEMENT_INTERFACE(GARROW_TYPE_RANDOM_ACCESS_FILE,
                                              garrow_random_access_file_interface_init)
                        G_IMPLEMENT_INTERFACE(GARROW_TYPE_WRITEABLE,
                                              garrow_writeable_interface_init)
                        G_IMPLEMENT_INTERFACE(GARROW_TYPE_WRITEABLE_FILE,
                                              garrow_writeable_file_interface_init));

#define GARROW_MEMORY_MAPPED_FILE_GET_PRIVATE(obj)                   \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                                   \
                               GARROW_TYPE_MEMORY_MAPPED_FILE,       \
                               GArrowMemoryMappedFilePrivate))

static void
garrow_memory_mapped_file_finalize(GObject *object)
{
  GArrowMemoryMappedFilePrivate *priv;

  priv = GARROW_MEMORY_MAPPED_FILE_GET_PRIVATE(object);

  priv->memory_mapped_file = nullptr;

  G_OBJECT_CLASS(garrow_memory_mapped_file_parent_class)->finalize(object);
}

static void
garrow_memory_mapped_file_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  GArrowMemoryMappedFilePrivate *priv;

  priv = GARROW_MEMORY_MAPPED_FILE_GET_PRIVATE(object);

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
garrow_memory_mapped_file_get_property(GObject *object,
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
garrow_memory_mapped_file_init(GArrowMemoryMappedFile *object)
{
}

static void
garrow_memory_mapped_file_class_init(GArrowMemoryMappedFileClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_memory_mapped_file_finalize;
  gobject_class->set_property = garrow_memory_mapped_file_set_property;
  gobject_class->get_property = garrow_memory_mapped_file_get_property;

  spec = g_param_spec_pointer("memory-mapped-file",
                              "io::MemoryMappedFile",
                              "The raw std::shared<arrow::io::MemoryMappedFile> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_MEMORY_MAPPED_FILE, spec);
}

/**
 * garrow_memory_mapped_file_open:
 * @path: The path of the memory mapped file.
 * @mode: The mode of the memory mapped file.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): A newly opened
 *   #GArrowMemoryMappedFile or %NULL on error.
 */
GArrowMemoryMappedFile *
garrow_memory_mapped_file_open(const gchar *path,
                                  GArrowFileMode mode,
                                  GError **error)
{
  std::shared_ptr<arrow::io::MemoryMappedFile> arrow_memory_mapped_file;
  auto status =
    arrow::io::MemoryMappedFile::Open(std::string(path),
                                      garrow_file_mode_to_raw(mode),
                                      &arrow_memory_mapped_file);
  if (status.ok()) {
    return garrow_memory_mapped_file_new_raw(&arrow_memory_mapped_file);
  } else {
    std::string context("[io][memory-mapped-file][open]: <");
    context += path;
    context += ">";
    garrow_error_set(error, status, context.c_str());
    return NULL;
  }
}

G_END_DECLS

GArrowMemoryMappedFile *
garrow_memory_mapped_file_new_raw(std::shared_ptr<arrow::io::MemoryMappedFile> *arrow_memory_mapped_file)
{
  auto memory_mapped_file =
    GARROW_MEMORY_MAPPED_FILE(g_object_new(GARROW_TYPE_MEMORY_MAPPED_FILE,
                                              "memory-mapped-file", arrow_memory_mapped_file,
                                              NULL));
  return memory_mapped_file;
}

std::shared_ptr<arrow::io::MemoryMappedFile>
garrow_memory_mapped_file_get_raw(GArrowMemoryMappedFile *memory_mapped_file)
{
  GArrowMemoryMappedFilePrivate *priv;

  priv = GARROW_MEMORY_MAPPED_FILE_GET_PRIVATE(memory_mapped_file);
  return priv->memory_mapped_file;
}
