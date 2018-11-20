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
#include <arrow/ipc/reader.h>

#include <arrow-glib/buffer.hpp>
#include <arrow-glib/codec.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/file.hpp>
#include <arrow-glib/input-stream.hpp>
#include <arrow-glib/readable.hpp>
#include <arrow-glib/tensor.hpp>

G_BEGIN_DECLS

/**
 * SECTION: input-stream
 * @section_id: input-stream-classes
 * @title: Input stream classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowInputStream is a base class for input stream.
 *
 * #GArrowSeekableInputStream is a base class for input stream that
 * supports random access.
 *
 * #GArrowBufferInputStream is a class to read data on buffer.
 *
 * #GArrowMemoryMappedInputStream is a class to read data in file by
 * mapping the file on memory. It supports zero copy.
 *
 * #GArrowGIOInputStream is a class for `GInputStream` based input
 * stream.
 *
 * #GArrowCompressedInputStream is a class to read data from
 * compressed input stream.
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

/**
 * garrow_input_stream_advance:
 * @input_stream: A #GArrowInputStream.
 * @n_bytes: The number of bytes to be advanced.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 0.11.0
 */
gboolean
garrow_input_stream_advance(GArrowInputStream *input_stream,
                            gint64 n_bytes,
                            GError **error)
{
  auto arrow_input_stream = garrow_input_stream_get_raw(input_stream);
  auto status = arrow_input_stream->Advance(n_bytes);
  return garrow_error_check(error, status, "[input-stream][advance]");
}

/**
 * garrow_input_stream_align:
 * @input_stream: A #GArrowInputStream.
 * @alignment: The byte multiple for the metadata prefix, usually 8
 *   or 64, to ensure the body starts on a multiple of that alignment.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 0.11.0
 */
gboolean
garrow_input_stream_align(GArrowInputStream *input_stream,
                          gint32 alignment,
                          GError **error)
{
  auto arrow_input_stream = garrow_input_stream_get_raw(input_stream);
  auto status = arrow::ipc::AlignStream(arrow_input_stream.get(),
                                        alignment);
  return garrow_error_check(error, status, "[input-stream][align]");
}

/**
 * garrow_input_stream_read_tensor:
 * @input_stream: A #GArrowInputStream.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable):
 *   #GArrowTensor on success, %NULL on error.
 *
 * Since: 0.11.0
 */
GArrowTensor *
garrow_input_stream_read_tensor(GArrowInputStream *input_stream,
                                GError **error)
{
  auto arrow_input_stream = garrow_input_stream_get_raw(input_stream);

  std::shared_ptr<arrow::Tensor> arrow_tensor;
  auto status = arrow::ipc::ReadTensor(arrow_input_stream.get(),
                                       &arrow_tensor);
  if (garrow_error_check(error, status, "[input-stream][read-tensor]")) {
    return garrow_tensor_new_raw(&arrow_tensor);
  } else {
    return NULL;
  }
}


G_DEFINE_TYPE(GArrowSeekableInputStream,                \
              garrow_seekable_input_stream,             \
              GARROW_TYPE_INPUT_STREAM);

static void
garrow_seekable_input_stream_init(GArrowSeekableInputStream *object)
{
}

static void
garrow_seekable_input_stream_class_init(GArrowSeekableInputStreamClass *klass)
{
}

/**
 * garrow_seekable_input_stream_get_size:
 * @input_stream: A #GArrowSeekableInputStream.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The size of the file.
 */
guint64
garrow_seekable_input_stream_get_size(GArrowSeekableInputStream *input_stream,
                                      GError **error)
{
  auto arrow_random_access_file =
    garrow_seekable_input_stream_get_raw(input_stream);
  int64_t size;
  auto status = arrow_random_access_file->GetSize(&size);
  if (garrow_error_check(error, status, "[seekable-input-stream][get-size]")) {
    return size;
  } else {
    return 0;
  }
}

/**
 * garrow_seekable_input_stream_get_support_zero_copy:
 * @input_stream: A #GArrowSeekableInputStream.
 *
 * Returns: Whether zero copy read is supported or not.
 */
gboolean
garrow_seekable_input_stream_get_support_zero_copy(GArrowSeekableInputStream *input_stream)
{
  auto arrow_random_access_file =
    garrow_seekable_input_stream_get_raw(input_stream);
  return arrow_random_access_file->supports_zero_copy();
}

/**
 * garrow_seekable_input_stream_read_at:
 * @input_stream: A #GArrowSeekableInputStream.
 * @position: The read start position.
 * @n_bytes: The number of bytes to be read.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable): #GArrowBuffer that has read
 *   data on success, %NULL if there was an error.
 */
GArrowBuffer *
garrow_seekable_input_stream_read_at(GArrowSeekableInputStream *input_stream,
                                     gint64 position,
                                     gint64 n_bytes,
                                     GError **error)
{
  auto arrow_random_access_file =
    garrow_seekable_input_stream_get_raw(input_stream);

  std::shared_ptr<arrow::Buffer> arrow_buffer;
  auto status = arrow_random_access_file->ReadAt(position,
                                                 n_bytes,
                                                 &arrow_buffer);
  if (garrow_error_check(error, status, "[seekable-input-stream][read-at]")) {
    return garrow_buffer_new_raw(&arrow_buffer);
  } else {
    return NULL;
  }
}


typedef struct GArrowBufferInputStreamPrivate_ {
  GArrowBuffer *buffer;
} GArrowBufferInputStreamPrivate;

enum {
  PROP_0_,
  PROP_BUFFER
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowBufferInputStream,                     \
                           garrow_buffer_input_stream,                  \
                           GARROW_TYPE_SEEKABLE_INPUT_STREAM);

#define GARROW_BUFFER_INPUT_STREAM_GET_PRIVATE(obj)                     \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                                   \
                               GARROW_TYPE_BUFFER_INPUT_STREAM,         \
                               GArrowBufferInputStreamPrivate))

static void
garrow_buffer_input_stream_dispose(GObject *object)
{
  auto priv = GARROW_BUFFER_INPUT_STREAM_GET_PRIVATE(object);

  if (priv->buffer) {
    g_object_unref(priv->buffer);
    priv->buffer = nullptr;
  }

  G_OBJECT_CLASS(garrow_buffer_input_stream_parent_class)->dispose(object);
}

static void
garrow_buffer_input_stream_set_property(GObject *object,
                                        guint prop_id,
                                        const GValue *value,
                                        GParamSpec *pspec)
{
  auto priv = GARROW_BUFFER_INPUT_STREAM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_BUFFER:
    priv->buffer = GARROW_BUFFER(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_buffer_input_stream_get_property(GObject *object,
                                        guint prop_id,
                                        GValue *value,
                                        GParamSpec *pspec)
{
  auto priv = GARROW_BUFFER_INPUT_STREAM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_BUFFER:
    g_value_set_object(value, priv->buffer);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_buffer_input_stream_init(GArrowBufferInputStream *object)
{
}

static void
garrow_buffer_input_stream_class_init(GArrowBufferInputStreamClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_buffer_input_stream_dispose;
  gobject_class->set_property = garrow_buffer_input_stream_set_property;
  gobject_class->get_property = garrow_buffer_input_stream_get_property;

  spec = g_param_spec_object("buffer",
                             "Buffer",
                             "The data",
                             GARROW_TYPE_BUFFER,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_BUFFER, spec);
}

/**
 * garrow_buffer_input_stream_new:
 * @buffer: The buffer to be read.
 *
 * Returns: A newly created #GArrowBufferInputStream.
 */
GArrowBufferInputStream *
garrow_buffer_input_stream_new(GArrowBuffer *buffer)
{
  auto arrow_buffer = garrow_buffer_get_raw(buffer);
  auto arrow_buffer_reader =
    std::make_shared<arrow::io::BufferReader>(arrow_buffer);
  return garrow_buffer_input_stream_new_raw_buffer(&arrow_buffer_reader, buffer);
}

/**
 * garrow_buffer_input_stream_get_buffer:
 * @input_stream: A #GArrowBufferInputStream.
 *
 * Returns: (transfer full): The data of the stream as #GArrowBuffer.
 */
GArrowBuffer *
garrow_buffer_input_stream_get_buffer(GArrowBufferInputStream *input_stream)
{
  auto priv = GARROW_BUFFER_INPUT_STREAM_GET_PRIVATE(input_stream);
  if (priv->buffer) {
    g_object_ref(priv->buffer);
    return priv->buffer;
  }

  auto arrow_buffer_reader = garrow_buffer_input_stream_get_raw(input_stream);
  auto arrow_buffer = arrow_buffer_reader->buffer();
  return garrow_buffer_new_raw(&arrow_buffer);
}


G_DEFINE_TYPE(GArrowMemoryMappedInputStream,            \
              garrow_memory_mapped_input_stream,        \
              GARROW_TYPE_SEEKABLE_INPUT_STREAM);

static void
garrow_memory_mapped_input_stream_init(GArrowMemoryMappedInputStream *object)
{
}

static void
garrow_memory_mapped_input_stream_class_init(GArrowMemoryMappedInputStreamClass *klass)
{
}

/**
 * garrow_memory_mapped_input_stream_new:
 * @path: The path of the file to be mapped on memory.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowMemoryMappedInputStream
 *   or %NULL on error.
 */
GArrowMemoryMappedInputStream *
garrow_memory_mapped_input_stream_new(const gchar *path,
                                      GError **error)
{
  std::shared_ptr<arrow::io::MemoryMappedFile> arrow_memory_mapped_file;
  auto status =
    arrow::io::MemoryMappedFile::Open(std::string(path),
                                      arrow::io::FileMode::READ,
                                      &arrow_memory_mapped_file);
  if (status.ok()) {
    return garrow_memory_mapped_input_stream_new_raw(&arrow_memory_mapped_file);
  } else {
    std::string context("[memory-mapped-input-stream][open]: <");
    context += path;
    context += ">";
    garrow_error_check(error, status, context.c_str());
    return NULL;
  }
}


G_END_DECLS

namespace garrow {
  class GIOInputStream : public arrow::io::RandomAccessFile {
  public:
    GIOInputStream(GInputStream *input_stream) :
      input_stream_(input_stream) {
      g_object_ref(input_stream_);
    }

    ~GIOInputStream() {
      g_object_unref(input_stream_);
    }

    GInputStream *get_input_stream() {
      return input_stream_;
    }

    bool closed() const override {
      return static_cast<bool>(g_input_stream_is_closed(input_stream_));
    }

    arrow::Status Close() override {
      GError *error = NULL;
      if (g_input_stream_close(input_stream_, NULL, &error)) {
        return arrow::Status::OK();
      } else {
        return garrow_error_to_status(error,
                                      arrow::StatusCode::IOError,
                                      "[gio-input-stream][close]");
      }
    }

    arrow::Status Tell(int64_t *position) const override {
      if (!G_IS_SEEKABLE(input_stream_)) {
        std::string message("[gio-input-stream][tell] "
                            "not seekable input stream: <");
        message += G_OBJECT_CLASS_NAME(G_OBJECT_GET_CLASS(input_stream_));
        message += ">";
        return arrow::Status::NotImplemented(message);
      }

      *position = g_seekable_tell(G_SEEKABLE(input_stream_));
      return arrow::Status::OK();
    }

    arrow::Status Read(int64_t n_bytes,
                       int64_t *n_read_bytes,
                       void *out) override {
      GError *error = NULL;
      *n_read_bytes = g_input_stream_read(input_stream_,
                                          out,
                                          n_bytes,
                                          NULL,
                                          &error);
      if (*n_read_bytes == -1) {
        return garrow_error_to_status(error,
                                      arrow::StatusCode::IOError,
                                      "[gio-input-stream][read]");
      } else {
        return arrow::Status::OK();
      }
    }

    arrow::Status ReadAt(int64_t position, int64_t n_bytes,
			 int64_t *n_read_bytes, void* out) override {
	return arrow::io::RandomAccessFile::ReadAt(
	    position, n_bytes, n_read_bytes, out);
    }

    arrow::Status ReadAt(int64_t position, int64_t n_bytes,
			 std::shared_ptr<arrow::Buffer>* out) override {
	return arrow::io::RandomAccessFile::ReadAt(position, n_bytes, out);
    }

    arrow::Status Read(int64_t n_bytes,
                       std::shared_ptr<arrow::Buffer> *out) override {
      arrow::MemoryPool *pool = arrow::default_memory_pool();
      std::shared_ptr<arrow::ResizableBuffer> buffer;
      RETURN_NOT_OK(AllocateResizableBuffer(pool, n_bytes, &buffer));

      GError *error = NULL;
      auto n_read_bytes = g_input_stream_read(input_stream_,
                                              buffer->mutable_data(),
                                              n_bytes,
                                              NULL,
                                              &error);
      if (n_read_bytes == -1) {
        return garrow_error_to_status(error,
                                      arrow::StatusCode::IOError,
                                      "[gio-input-stream][read][buffer]");
      } else {
        if (n_read_bytes < n_bytes) {
          RETURN_NOT_OK(buffer->Resize(n_read_bytes));
        }
        *out = buffer;
        return arrow::Status::OK();
      }
    }

    arrow::Status Seek(int64_t position) override {
      if (!G_IS_SEEKABLE(input_stream_)) {
        std::string message("[gio-input-stream][seek] "
                            "not seekable input stream: <");
        message += G_OBJECT_CLASS_NAME(G_OBJECT_GET_CLASS(input_stream_));
        message += ">";
        return arrow::Status::NotImplemented(message);
      }

      GError *error = NULL;
      if (g_seekable_seek(G_SEEKABLE(input_stream_),
                          position,
                          G_SEEK_SET,
                          NULL,
                          &error)) {
        return arrow::Status::OK();
      } else {
        return garrow_error_to_status(error,
                                      arrow::StatusCode::IOError,
                                      "[gio-input-stream][seek]");
      }
    }

    arrow::Status GetSize(int64_t *size) override {
      if (!G_IS_SEEKABLE(input_stream_)) {
        std::string message("[gio-input-stream][size] "
                            "not seekable input stream: <");
        message += G_OBJECT_CLASS_NAME(G_OBJECT_GET_CLASS(input_stream_));
        message += ">";
        return arrow::Status::NotImplemented(message);
      }

      auto current_position = g_seekable_tell(G_SEEKABLE(input_stream_));
      GError *error = NULL;
      if (!g_seekable_seek(G_SEEKABLE(input_stream_),
                           0,
                           G_SEEK_END,
                           NULL,
                           &error)) {
        return garrow_error_to_status(error,
                                      arrow::StatusCode::IOError,
                                      "[gio-input-stream][size][seek]");
      }
      *size = g_seekable_tell(G_SEEKABLE(input_stream_));
      if (!g_seekable_seek(G_SEEKABLE(input_stream_),
                           current_position,
                           G_SEEK_SET,
                           NULL,
                           &error)) {
        return garrow_error_to_status(error,
                                      arrow::StatusCode::IOError,
                                      "[gio-input-stream][size][seek][restore]");
      }
      return arrow::Status::OK();
    }

    bool supports_zero_copy() const override {
      return false;
    }

  private:
    GInputStream *input_stream_;
  };
};

G_BEGIN_DECLS

G_DEFINE_TYPE(GArrowGIOInputStream,                \
              garrow_gio_input_stream,             \
              GARROW_TYPE_SEEKABLE_INPUT_STREAM);

static void
garrow_gio_input_stream_init(GArrowGIOInputStream *object)
{
}

static void
garrow_gio_input_stream_class_init(GArrowGIOInputStreamClass *klass)
{
}

/**
 * garrow_gio_input_stream_new:
 * @gio_input_stream: The stream to be read.
 *
 * Returns: (transfer full): A newly created #GArrowGIOInputStream.
 *
 * Since: 0.5.0
 */
GArrowGIOInputStream *
garrow_gio_input_stream_new(GInputStream *gio_input_stream)
{
  auto arrow_input_stream =
    std::make_shared<garrow::GIOInputStream>(gio_input_stream);
  auto object = g_object_new(GARROW_TYPE_GIO_INPUT_STREAM,
                             "input-stream", &arrow_input_stream,
                             NULL);
  auto input_stream = GARROW_GIO_INPUT_STREAM(object);
  return input_stream;
}

/**
 * garrow_gio_input_stream_get_raw:
 * @input_stream: A #GArrowGIOInputStream.
 *
 * Returns: (transfer none): The wrapped #GInputStream.
 *
 * Since: 0.5.0
 */
GInputStream *
garrow_gio_input_stream_get_raw(GArrowGIOInputStream *input_stream)
{
  auto arrow_input_stream =
    garrow_input_stream_get_raw(GARROW_INPUT_STREAM(input_stream));
  auto arrow_gio_input_stream =
    std::static_pointer_cast<garrow::GIOInputStream>(arrow_input_stream);
  auto gio_input_stream = arrow_gio_input_stream->get_input_stream();
  return gio_input_stream;
}

typedef struct GArrowCompressedInputStreamPrivate_ {
  GArrowCodec *codec;
  GArrowInputStream *raw;
} GArrowCompressedInputStreamPrivate;

enum {
  PROP_CODEC = 1,
  PROP_RAW
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowCompressedInputStream,
                           garrow_compressed_input_stream,
                           GARROW_TYPE_INPUT_STREAM)

#define GARROW_COMPRESSED_INPUT_STREAM_GET_PRIVATE(object)      \
  static_cast<GArrowCompressedInputStreamPrivate *>(            \
    garrow_compressed_input_stream_get_instance_private(        \
      GARROW_COMPRESSED_INPUT_STREAM(object)))

static void
garrow_compressed_input_stream_dispose(GObject *object)
{
  auto priv = GARROW_COMPRESSED_INPUT_STREAM_GET_PRIVATE(object);

  if (priv->codec) {
    g_object_unref(priv->codec);
    priv->codec = NULL;
  }

  if (priv->raw) {
    g_object_unref(priv->raw);
    priv->raw = NULL;
  }

  G_OBJECT_CLASS(garrow_compressed_input_stream_parent_class)->dispose(object);
}

static void
garrow_compressed_input_stream_set_property(GObject *object,
                                            guint prop_id,
                                            const GValue *value,
                                            GParamSpec *pspec)
{
  auto priv = GARROW_COMPRESSED_INPUT_STREAM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_CODEC:
    priv->codec = GARROW_CODEC(g_value_dup_object(value));
    break;
  case PROP_RAW:
    priv->raw = GARROW_INPUT_STREAM(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_compressed_input_stream_get_property(GObject *object,
                                            guint prop_id,
                                            GValue *value,
                                            GParamSpec *pspec)
{
  auto priv = GARROW_COMPRESSED_INPUT_STREAM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_CODEC:
    g_value_set_object(value, priv->codec);
    break;
  case PROP_RAW:
    g_value_set_object(value, priv->raw);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_compressed_input_stream_init(GArrowCompressedInputStream *object)
{
}

static void
garrow_compressed_input_stream_class_init(GArrowCompressedInputStreamClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_compressed_input_stream_dispose;
  gobject_class->set_property = garrow_compressed_input_stream_set_property;
  gobject_class->get_property = garrow_compressed_input_stream_get_property;

  spec = g_param_spec_object("codec",
                             "Codec",
                             "The codec for the stream",
                             GARROW_TYPE_CODEC,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_CODEC, spec);

  spec = g_param_spec_object("raw",
                             "Raw",
                             "The underlying raw input stream",
                             GARROW_TYPE_INPUT_STREAM,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_RAW, spec);
}

/**
 * garrow_compressed_input_stream_new:
 * @codec: A #GArrowCodec for compressed data in the @raw.
 * @raw: A #GArrowInputStream that contains compressed data.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: A newly created #GArrowCompressedInputStream.
 *
 * Since: 0.12.0
 */
GArrowCompressedInputStream *
garrow_compressed_input_stream_new(GArrowCodec *codec,
                                   GArrowInputStream *raw,
                                   GError **error)
{
  auto arrow_codec = garrow_codec_get_raw(codec);
  auto arrow_raw = garrow_input_stream_get_raw(raw);
  std::shared_ptr<arrow::io::CompressedInputStream> arrow_stream;
  auto status = arrow::io::CompressedInputStream::Make(arrow_codec,
                                                       arrow_raw,
                                                       &arrow_stream);
  if (garrow_error_check(error, status, "[compressed-input-stream][new]")) {
    return garrow_compressed_input_stream_new_raw(&arrow_stream,
                                                  codec,
                                                  raw);
  } else {
    return NULL;
  }
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

std::shared_ptr<arrow::io::RandomAccessFile>
garrow_seekable_input_stream_get_raw(GArrowSeekableInputStream *seekable_input_stream)
{
  auto arrow_input_stream =
    garrow_input_stream_get_raw(GARROW_INPUT_STREAM(seekable_input_stream));
  auto arrow_random_access_file =
    std::static_pointer_cast<arrow::io::RandomAccessFile>(arrow_input_stream);
  return arrow_random_access_file;
}

GArrowBufferInputStream *
garrow_buffer_input_stream_new_raw(std::shared_ptr<arrow::io::BufferReader> *arrow_buffer_reader)
{
  return garrow_buffer_input_stream_new_raw_buffer(arrow_buffer_reader, nullptr);
}

GArrowBufferInputStream *
garrow_buffer_input_stream_new_raw_buffer(std::shared_ptr<arrow::io::BufferReader> *arrow_buffer_reader,
                                          GArrowBuffer *buffer)
{
  auto buffer_input_stream =
    GARROW_BUFFER_INPUT_STREAM(g_object_new(GARROW_TYPE_BUFFER_INPUT_STREAM,
                                            "input-stream", arrow_buffer_reader,
                                            "buffer", buffer,
                                            NULL));
  return buffer_input_stream;
}

std::shared_ptr<arrow::io::BufferReader>
garrow_buffer_input_stream_get_raw(GArrowBufferInputStream *buffer_input_stream)
{
  auto arrow_input_stream =
    garrow_input_stream_get_raw(GARROW_INPUT_STREAM(buffer_input_stream));
  auto arrow_buffer_reader =
    std::static_pointer_cast<arrow::io::BufferReader>(arrow_input_stream);
  return arrow_buffer_reader;
}

GArrowMemoryMappedInputStream *
garrow_memory_mapped_input_stream_new_raw(std::shared_ptr<arrow::io::MemoryMappedFile> *arrow_memory_mapped_file)
{
  auto object = g_object_new(GARROW_TYPE_MEMORY_MAPPED_INPUT_STREAM,
                             "input-stream", arrow_memory_mapped_file,
                             NULL);
  auto memory_mapped_input_stream = GARROW_MEMORY_MAPPED_INPUT_STREAM(object);
  return memory_mapped_input_stream;
}

GArrowCompressedInputStream *
garrow_compressed_input_stream_new_raw(std::shared_ptr<arrow::io::CompressedInputStream> *arrow_raw,
                                       GArrowCodec *codec,
                                       GArrowInputStream *raw)
{
  auto compressed_input_stream =
    g_object_new(GARROW_TYPE_COMPRESSED_INPUT_STREAM,
                 "input-stream", arrow_raw,
                 "codec", codec,
                 "raw", raw,
                 NULL);
  return GARROW_COMPRESSED_INPUT_STREAM(compressed_input_stream);
}

std::shared_ptr<arrow::io::InputStream>
garrow_compressed_input_stream_get_raw(GArrowCompressedInputStream *compressed_input_stream)
{
  auto input_stream = GARROW_INPUT_STREAM(compressed_input_stream);
  auto arrow_input_stream = garrow_input_stream_get_raw(input_stream);
  auto arrow_compressed_input_stream =
    std::static_pointer_cast<arrow::io::CompressedInputStream>(arrow_input_stream);
  return arrow_compressed_input_stream->raw();
}
