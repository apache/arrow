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
 */

typedef struct GArrowBufferPrivate_ {
  std::shared_ptr<arrow::Buffer> buffer;
} GArrowBufferPrivate;

enum {
  PROP_0,
  PROP_BUFFER
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowBuffer, garrow_buffer, G_TYPE_OBJECT)

#define GARROW_BUFFER_GET_PRIVATE(obj) \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj), GARROW_TYPE_BUFFER, GArrowBufferPrivate))

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
  switch (prop_id) {
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

  gobject_class->finalize     = garrow_buffer_finalize;
  gobject_class->set_property = garrow_buffer_set_property;
  gobject_class->get_property = garrow_buffer_get_property;

  spec = g_param_spec_pointer("buffer",
                              "Buffer",
                              "The raw std::shared<arrow::Buffer> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_BUFFER, spec);
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
    return garrow_buffer_new_raw(&arrow_parent_buffer);
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
  if (status.ok()) {
    return garrow_buffer_new_raw(&arrow_copied_buffer);
  } else {
    garrow_error_set(error, status, "[buffer][copy]");
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
  return garrow_buffer_new_raw(&arrow_buffer);
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
  return garrow_mutable_buffer_new_raw(&arrow_buffer);
}

G_END_DECLS

GArrowBuffer *
garrow_buffer_new_raw(std::shared_ptr<arrow::Buffer> *arrow_buffer)
{
  auto buffer = GARROW_BUFFER(g_object_new(GARROW_TYPE_BUFFER,
                                           "buffer", arrow_buffer,
                                           NULL));
  return buffer;
}

std::shared_ptr<arrow::Buffer>
garrow_buffer_get_raw(GArrowBuffer *buffer)
{
  auto priv = GARROW_BUFFER_GET_PRIVATE(buffer);
  return priv->buffer;
}

GArrowMutableBuffer *
garrow_mutable_buffer_new_raw(std::shared_ptr<arrow::MutableBuffer> *arrow_buffer)
{
  auto buffer = GARROW_MUTABLE_BUFFER(g_object_new(GARROW_TYPE_MUTABLE_BUFFER,
                                                   "buffer", arrow_buffer,
                                                   NULL));
  return buffer;
}
