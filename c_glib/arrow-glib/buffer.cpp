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

#include <arrow-glib/buffer.hpp>
#include <arrow-glib/error.hpp>

G_BEGIN_DECLS

/**
 * SECTION: buffer
 * @section_id: buffer-classes
 * @title: Buffer classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowBuffer is a class for keeping data. Other classes such as
 * #GArrowArray and #GArrowTensor can use data in buffer.
 *
 * #GArrowBuffer is immutable.
 *
 * #GArrowMutableBuffer is mutable.
 *
 * #GArrowResizableBuffer is mutable and resizable.
 */

typedef struct GArrowBufferPrivate_ {
  std::shared_ptr<arrow::Buffer> buffer;
  GBytes *data;
} GArrowBufferPrivate;

enum {
  PROP_0,
  PROP_BUFFER,
  PROP_DATA
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowBuffer, garrow_buffer, G_TYPE_OBJECT)

#define GARROW_BUFFER_GET_PRIVATE(obj) \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj), GARROW_TYPE_BUFFER, GArrowBufferPrivate))

static void
garrow_buffer_dispose(GObject *object)
{
  auto priv = GARROW_BUFFER_GET_PRIVATE(object);

  if (priv->data) {
    g_bytes_unref(priv->data);
    priv->data = nullptr;
  }

  G_OBJECT_CLASS(garrow_buffer_parent_class)->dispose(object);
}

static void
garrow_buffer_finalize(GObject *object)
{
  auto priv = GARROW_BUFFER_GET_PRIVATE(object);

  priv->buffer = nullptr;

  G_OBJECT_CLASS(garrow_buffer_parent_class)->finalize(object);
}

static void
garrow_buffer_set_property(GObject *object,
                           guint prop_id,
                           const GValue *value,
                           GParamSpec *pspec)
{
  auto priv = GARROW_BUFFER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_BUFFER:
    priv->buffer =
      *static_cast<std::shared_ptr<arrow::Buffer> *>(g_value_get_pointer(value));
    break;
  case PROP_DATA:
    priv->data = static_cast<GBytes *>(g_value_dup_boxed(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_buffer_get_property(GObject *object,
                           guint prop_id,
                           GValue *value,
                           GParamSpec *pspec)
{
  auto priv = GARROW_BUFFER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_DATA:
    g_value_set_boxed(value, priv->data);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_buffer_init(GArrowBuffer *object)
{
}

static void
garrow_buffer_class_init(GArrowBufferClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_buffer_dispose;
  gobject_class->finalize     = garrow_buffer_finalize;
  gobject_class->set_property = garrow_buffer_set_property;
  gobject_class->get_property = garrow_buffer_get_property;

  spec = g_param_spec_pointer("buffer",
                              "Buffer",
                              "The raw std::shared<arrow::Buffer> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_BUFFER, spec);

  spec = g_param_spec_boxed("data",
                            "Data",
                            "The raw data passed as GBytes *",
                            G_TYPE_BYTES,
                            static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                     G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_DATA, spec);
}

/**
 * garrow_buffer_new:
 * @data: (array length=size): Data for the buffer.
 *   They aren't owned by the new buffer.
 *   You must not free the data while the new buffer is alive.
 * @size: The number of bytes of the data.
 *
 * Returns: A newly created #GArrowBuffer.
 *
 * Since: 0.3.0
 */
GArrowBuffer *
garrow_buffer_new(const guint8 *data, gint64 size)
{
  auto arrow_buffer = std::make_shared<arrow::Buffer>(data, size);
  return garrow_buffer_new_raw(&arrow_buffer);
}

/**
 * garrow_buffer_new_bytes:
 * @data: Data for the buffer.
 *
 * Returns: A newly created #GArrowBuffer.
 *
 * Since: 0.9.0
 */
GArrowBuffer *
garrow_buffer_new_bytes(GBytes *data)
{
  size_t data_size;
  auto raw_data = g_bytes_get_data(data, &data_size);
  auto arrow_buffer =
    std::make_shared<arrow::Buffer>(static_cast<const uint8_t *>(raw_data),
                                    data_size);
  return garrow_buffer_new_raw_bytes(&arrow_buffer, data);
}

/**
 * garrow_buffer_equal:
 * @buffer: A #GArrowBuffer.
 * @other_buffer: A #GArrowBuffer to be compared.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 *
 * Since: 0.4.0
 */
gboolean
garrow_buffer_equal(GArrowBuffer *buffer, GArrowBuffer *other_buffer)
{
  const auto arrow_buffer = garrow_buffer_get_raw(buffer);
  const auto arrow_other_buffer = garrow_buffer_get_raw(other_buffer);
  return arrow_buffer->Equals(*arrow_other_buffer);
}

/**
 * garrow_buffer_equal_n_bytes:
 * @buffer: A #GArrowBuffer.
 * @other_buffer: A #GArrowBuffer to be compared.
 * @n_bytes: The number of first bytes to be compared.
 *
 * Returns: %TRUE if both of them have the same data in the first
 *   `n_bytes`, %FALSE otherwise.
 *
 * Since: 0.4.0
 */
gboolean
garrow_buffer_equal_n_bytes(GArrowBuffer *buffer,
                            GArrowBuffer *other_buffer,
                            gint64 n_bytes)
{
  const auto arrow_buffer = garrow_buffer_get_raw(buffer);
  const auto arrow_other_buffer = garrow_buffer_get_raw(other_buffer);
  return arrow_buffer->Equals(*arrow_other_buffer, n_bytes);
}

/**
 * garrow_buffer_is_mutable:
 * @buffer: A #GArrowBuffer.
 *
 * Returns: %TRUE if the buffer is mutable, %FALSE otherwise.
 *
 * Since: 0.3.0
 */
gboolean
garrow_buffer_is_mutable(GArrowBuffer *buffer)
{
  auto arrow_buffer = garrow_buffer_get_raw(buffer);
  return arrow_buffer->is_mutable();
}

/**
 * garrow_buffer_get_capacity:
 * @buffer: A #GArrowBuffer.
 *
 * Returns: The number of bytes that where allocated for the buffer in
 *   total.
 *
 * Since: 0.3.0
 */
gint64
garrow_buffer_get_capacity(GArrowBuffer *buffer)
{
  auto arrow_buffer = garrow_buffer_get_raw(buffer);
  return arrow_buffer->capacity();
}

/**
 * garrow_buffer_get_data:
 * @buffer: A #GArrowBuffer.
 *
 * Returns: (transfer full): The data of the buffer. The data is owned by
 *   the buffer. You should not free or modify the data.
 *
 * Since: 0.3.0
 */
GBytes *
garrow_buffer_get_data(GArrowBuffer *buffer)
{
  auto priv = GARROW_BUFFER_GET_PRIVATE(buffer);
  if (priv->data) {
    g_bytes_ref(priv->data);
    return priv->data;
  }

  auto arrow_buffer = garrow_buffer_get_raw(buffer);
  auto data = g_bytes_new_static(arrow_buffer->data(),
                                 arrow_buffer->size());
  return data;
}

/**
 * garrow_buffer_get_mutable_data:
 * @buffer: A #GArrowBuffer.
 *
 * Returns: (transfer full) (nullable): The data of the buffer. If the
 *   buffer is imutable, it returns %NULL. The data is owned by the
 *   buffer. You should not free the data.
 *
 * Since: 0.3.0
 */
GBytes *
garrow_buffer_get_mutable_data(GArrowBuffer *buffer)
{
  auto arrow_buffer = garrow_buffer_get_raw(buffer);
  if (!arrow_buffer->is_mutable()) {
    return NULL;
  }

  auto priv = GARROW_BUFFER_GET_PRIVATE(buffer);
  if (priv->data) {
    g_bytes_ref(priv->data);
    return priv->data;
  }

  return g_bytes_new_static(arrow_buffer->mutable_data(),
                            arrow_buffer->size());
}

/**
 * garrow_buffer_get_size:
 * @buffer: A #GArrowBuffer.
 *
 * Returns: The number of bytes that might have valid data.
 *
 * Since: 0.3.0
 */
gint64
garrow_buffer_get_size(GArrowBuffer *buffer)
{
  auto arrow_buffer = garrow_buffer_get_raw(buffer);
  return arrow_buffer->size();
}

/**
 * garrow_buffer_get_parent:
 * @buffer: A #GArrowBuffer.
 *
 * Returns: (nullable) (transfer full):
 *   The parent #GArrowBuffer or %NULL.
 *
 * Since: 0.3.0
 */
GArrowBuffer *
garrow_buffer_get_parent(GArrowBuffer *buffer)
{
  auto arrow_buffer = garrow_buffer_get_raw(buffer);
  auto arrow_parent_buffer = arrow_buffer->parent();

  if (arrow_parent_buffer) {
    auto priv = GARROW_BUFFER_GET_PRIVATE(buffer);
    return garrow_buffer_new_raw_bytes(&arrow_parent_buffer, priv->data);
  } else {
    return NULL;
  }
}

/**
 * garrow_buffer_copy:
 * @buffer: A #GArrowBuffer.
 * @start: An offset of data to be copied in byte.
 * @size: The number of bytes to be copied from the start.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   A newly copied #GArrowBuffer on success, %NULL on error.
 *
 * Since: 0.3.0
 */
GArrowBuffer *
garrow_buffer_copy(GArrowBuffer *buffer,
                   gint64 start,
                   gint64 size,
                   GError **error)
{
  auto arrow_buffer = garrow_buffer_get_raw(buffer);
  std::shared_ptr<arrow::Buffer> arrow_copied_buffer;
  auto status = arrow_buffer->Copy(start, size, &arrow_copied_buffer);
  if (garrow_error_check(error, status, "[buffer][copy]")) {
    return garrow_buffer_new_raw(&arrow_copied_buffer);
  } else {
    return NULL;
  }
}

/**
 * garrow_buffer_slice:
 * @buffer: A #GArrowBuffer.
 * @offset: An offset in the buffer data in byte.
 * @size: The number of bytes of the sliced data.
 *
 * Returns: (transfer full): A newly created #GArrowBuffer that shares
 *   data of the base #GArrowBuffer. The created #GArrowBuffer has data
 *   start with offset from the base buffer data and are the specified
 *   bytes size.
 *
 * Since: 0.3.0
 */
GArrowBuffer *
garrow_buffer_slice(GArrowBuffer *buffer, gint64 offset, gint64 size)
{
  auto arrow_parent_buffer = garrow_buffer_get_raw(buffer);
  auto arrow_buffer = std::make_shared<arrow::Buffer>(arrow_parent_buffer,
                                                      offset,
                                                      size);
  auto priv = GARROW_BUFFER_GET_PRIVATE(buffer);
  return garrow_buffer_new_raw_bytes(&arrow_buffer, priv->data);
}


G_DEFINE_TYPE(GArrowMutableBuffer,              \
              garrow_mutable_buffer,            \
              GARROW_TYPE_BUFFER)

static void
garrow_mutable_buffer_init(GArrowMutableBuffer *object)
{
}

static void
garrow_mutable_buffer_class_init(GArrowMutableBufferClass *klass)
{
}

/**
 * garrow_mutable_buffer_new:
 * @data: (array length=size): Data for the buffer.
 *   They aren't owned by the new buffer.
 *   You must not free the data while the new buffer is alive.
 * @size: The number of bytes of the data.
 *
 * Returns: A newly created #GArrowMutableBuffer.
 *
 * Since: 0.3.0
 */
GArrowMutableBuffer *
garrow_mutable_buffer_new(guint8 *data, gint64 size)
{
  auto arrow_buffer = std::make_shared<arrow::MutableBuffer>(data, size);
  return garrow_mutable_buffer_new_raw(&arrow_buffer);
}

/**
 * garrow_mutable_buffer_new_bytes:
 * @data: Data for the buffer.
 *
 * Returns: A newly created #GArrowMutableBuffer.
 *
 * Since: 0.9.0
 */
GArrowMutableBuffer *
garrow_mutable_buffer_new_bytes(GBytes *data)
{
  size_t data_size;
  auto raw_data = g_bytes_get_data(data, &data_size);
  auto mutable_raw_data = const_cast<gpointer>(raw_data);
  auto arrow_buffer =
    std::make_shared<arrow::MutableBuffer>(static_cast<uint8_t *>(mutable_raw_data),
                                           data_size);
  return garrow_mutable_buffer_new_raw_bytes(&arrow_buffer, data);
}

/**
 * garrow_mutable_buffer_slice:
 * @buffer: A #GArrowMutableBuffer.
 * @offset: An offset in the buffer data in byte.
 * @size: The number of bytes of the sliced data.
 *
 * Returns: (transfer full): A newly created #GArrowMutableBuffer that
 *   shares data of the base #GArrowMutableBuffer. The created
 *   #GArrowMutableBuffer has data start with offset from the base
 *   buffer data and are the specified bytes size.
 *
 * Since: 0.3.0
 */
GArrowMutableBuffer *
garrow_mutable_buffer_slice(GArrowMutableBuffer *buffer,
                            gint64 offset,
                            gint64 size)
{
  auto arrow_parent_buffer = garrow_buffer_get_raw(GARROW_BUFFER(buffer));
  auto arrow_buffer =
    std::make_shared<arrow::MutableBuffer>(arrow_parent_buffer,
                                           offset,
                                           size);
  auto priv = GARROW_BUFFER_GET_PRIVATE(buffer);
  return garrow_mutable_buffer_new_raw_bytes(&arrow_buffer, priv->data);
}


G_DEFINE_TYPE(GArrowResizableBuffer,              \
              garrow_resizable_buffer,            \
              GARROW_TYPE_MUTABLE_BUFFER)

static void
garrow_resizable_buffer_init(GArrowResizableBuffer *object)
{
}

static void
garrow_resizable_buffer_class_init(GArrowResizableBufferClass *klass)
{
}

/**
 * garrow_resizable_buffer_new:
 * @initial_size: The initial buffer size in bytes.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowResizableBuffer.
 *
 * Since: 0.10.0
 */
GArrowResizableBuffer *
garrow_resizable_buffer_new(gint64 initial_size,
                            GError **error)
{
  std::shared_ptr<arrow::ResizableBuffer> arrow_buffer;
  auto status = arrow::AllocateResizableBuffer(initial_size, &arrow_buffer);
  if (garrow_error_check(error, status, "[resizable-buffer][new]")) {
    return garrow_resizable_buffer_new_raw(&arrow_buffer);
  } else {
    return NULL;
  }
}


/**
 * garrow_resizable_buffer_resize:
 * @buffer: A #GArrowResizableBuffer.
 * @new_size: The new buffer size in bytes.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Since: 0.3.0
 */
gboolean
garrow_resizable_buffer_resize(GArrowResizableBuffer *buffer,
                               gint64 new_size,
                               GError **error)
{
  auto arrow_buffer = garrow_buffer_get_raw(GARROW_BUFFER(buffer));
  auto arrow_resizable_buffer =
    std::static_pointer_cast<arrow::ResizableBuffer>(arrow_buffer);
  auto status = arrow_resizable_buffer->Resize(new_size);
  return garrow_error_check(error, status, "[resizable-buffer][resize]");
}

/**
 * garrow_resizable_buffer_reserve:
 * @buffer: A #GArrowResizableBuffer.
 * @new_capacity: The new buffer capacity in bytes.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Since: 0.3.0
 */
gboolean
garrow_resizable_buffer_reserve(GArrowResizableBuffer *buffer,
                                gint64 new_capacity,
                                GError **error)
{
  auto arrow_buffer = garrow_buffer_get_raw(GARROW_BUFFER(buffer));
  auto arrow_resizable_buffer =
    std::static_pointer_cast<arrow::ResizableBuffer>(arrow_buffer);
  auto status = arrow_resizable_buffer->Reserve(new_capacity);
  return garrow_error_check(error, status, "[resizable-buffer][capacity]");
}


G_END_DECLS

GArrowBuffer *
garrow_buffer_new_raw(std::shared_ptr<arrow::Buffer> *arrow_buffer)
{
  return garrow_buffer_new_raw_bytes(arrow_buffer, nullptr);
}

GArrowBuffer *
garrow_buffer_new_raw_bytes(std::shared_ptr<arrow::Buffer> *arrow_buffer,
                            GBytes *data)
{
  auto buffer = GARROW_BUFFER(g_object_new(GARROW_TYPE_BUFFER,
                                           "buffer", arrow_buffer,
                                           "data", data,
                                           NULL));
  return buffer;
}

std::shared_ptr<arrow::Buffer>
garrow_buffer_get_raw(GArrowBuffer *buffer)
{
  if (!buffer)
    return nullptr;

  auto priv = GARROW_BUFFER_GET_PRIVATE(buffer);
  return priv->buffer;
}

GArrowMutableBuffer *
garrow_mutable_buffer_new_raw(std::shared_ptr<arrow::MutableBuffer> *arrow_buffer)
{
  return garrow_mutable_buffer_new_raw_bytes(arrow_buffer, nullptr);
}

GArrowMutableBuffer *
garrow_mutable_buffer_new_raw_bytes(std::shared_ptr<arrow::MutableBuffer> *arrow_buffer,
                                    GBytes *data)
{
  auto buffer = GARROW_MUTABLE_BUFFER(g_object_new(GARROW_TYPE_MUTABLE_BUFFER,
                                                   "buffer", arrow_buffer,
                                                   "data", data,
                                                   NULL));
  return buffer;
}

GArrowResizableBuffer *
garrow_resizable_buffer_new_raw(std::shared_ptr<arrow::ResizableBuffer> *arrow_buffer)
{
  auto buffer =
    GARROW_RESIZABLE_BUFFER(g_object_new(GARROW_TYPE_RESIZABLE_BUFFER,
                                         "buffer", arrow_buffer,
                                         NULL));
  return buffer;
}
