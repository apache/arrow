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

#include <arrow-glib/array.hpp>
#include <arrow-glib/chunked-array.hpp>
#include <arrow-glib/data-type.hpp>
#include <arrow-glib/type.hpp>
#include <arrow-glib/error.hpp>

#include <sstream>

G_BEGIN_DECLS

/**
 * SECTION: chunked-array
 * @short_description: Chunked array class
 *
 * #GArrowChunkedArray is a class for chunked array. Chunked array
 * makes a list of #GArrowArrays one logical large array.
 */

struct GArrowChunkedArrayPrivate {
  std::shared_ptr<arrow::ChunkedArray> chunked_array;
  GArrowDataType *data_type;
};

enum {
  PROP_CHUNKED_ARRAY = 1,
  PROP_DATA_TYPE,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowChunkedArray,
                           garrow_chunked_array,
                           G_TYPE_OBJECT)

#define GARROW_CHUNKED_ARRAY_GET_PRIVATE(obj)         \
  static_cast<GArrowChunkedArrayPrivate *>(           \
     garrow_chunked_array_get_instance_private(       \
       GARROW_CHUNKED_ARRAY(obj)))

static void
garrow_chunked_array_dispose(GObject *object)
{
  auto priv = GARROW_CHUNKED_ARRAY_GET_PRIVATE(object);

  if (priv->data_type) {
    g_object_unref(priv->data_type);
    priv->data_type = nullptr;
  }

  G_OBJECT_CLASS(garrow_chunked_array_parent_class)->dispose(object);
}

static void
garrow_chunked_array_finalize(GObject *object)
{
  auto priv = GARROW_CHUNKED_ARRAY_GET_PRIVATE(object);

  priv->chunked_array.~shared_ptr();

  G_OBJECT_CLASS(garrow_chunked_array_parent_class)->finalize(object);
}

static void
garrow_chunked_array_set_property(GObject *object,
                                  guint prop_id,
                                  const GValue *value,
                                  GParamSpec *pspec)
{
  auto priv = GARROW_CHUNKED_ARRAY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_CHUNKED_ARRAY:
    priv->chunked_array =
      *static_cast<std::shared_ptr<arrow::ChunkedArray> *>(g_value_get_pointer(value));
    break;
  case PROP_DATA_TYPE:
    priv->data_type = GARROW_DATA_TYPE(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_chunked_array_get_property(GObject *object,
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
garrow_chunked_array_init(GArrowChunkedArray *object)
{
  auto priv = GARROW_CHUNKED_ARRAY_GET_PRIVATE(object);
  new(&priv->chunked_array) std::shared_ptr<arrow::ChunkedArray>;
}

static void
garrow_chunked_array_class_init(GArrowChunkedArrayClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_chunked_array_dispose;
  gobject_class->finalize     = garrow_chunked_array_finalize;
  gobject_class->set_property = garrow_chunked_array_set_property;
  gobject_class->get_property = garrow_chunked_array_get_property;

  spec = g_param_spec_pointer("chunked-array",
                              "Chunked array",
                              "The raw std::shared<arrow::ChunkedArray> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_CHUNKED_ARRAY, spec);

  spec = g_param_spec_object("data-type",
                             "Data type",
                             "The data type of this chunked array",
                             GARROW_TYPE_DATA_TYPE,
                             static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_DATA_TYPE, spec);
}

/**
 * garrow_chunked_array_new:
 * @chunks: (element-type GArrowArray): The array chunks.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable):
 *   A newly created #GArrowChunkedArray or %NULL on error.
 */
GArrowChunkedArray *
garrow_chunked_array_new(GList *chunks, GError **error)
{
  std::vector<std::shared_ptr<arrow::Array>> arrow_chunks;
  for (GList *node = chunks; node; node = node->next) {
    GArrowArray *chunk = GARROW_ARRAY(node->data);
    arrow_chunks.push_back(garrow_array_get_raw(chunk));
  }

  auto arrow_chunked_array_result = arrow::ChunkedArray::Make(arrow_chunks);
  if (garrow::check(error, arrow_chunked_array_result, "[chunked-array][new]")) {
    auto arrow_chunked_array = *arrow_chunked_array_result;
    return garrow_chunked_array_new_raw(&arrow_chunked_array);
  } else {
    return nullptr;
  }
}

/**
 * garrow_chunked_array_new_empty:
 * @data_type: The #GArrowDataType of this chunked array.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable):
 *   A newly created empty #GArrowChunkedArray or %NULL on error.
 *
 * Since: 11.0.0
 */
GArrowChunkedArray *
garrow_chunked_array_new_empty(GArrowDataType *data_type, GError **error)
{
  auto arrow_data_type = garrow_data_type_get_raw(data_type);
  auto arrow_chunked_array_result =
    arrow::ChunkedArray::MakeEmpty(arrow_data_type);
  if (garrow::check(error, arrow_chunked_array_result, "[chunked-array][new]")) {
    auto arrow_chunked_array = *arrow_chunked_array_result;
    return garrow_chunked_array_new_raw(&arrow_chunked_array);
  } else {
    return nullptr;
  }
}

/**
 * garrow_chunked_array_equal:
 * @chunked_array: A #GArrowChunkedArray.
 * @other_chunked_array: A #GArrowChunkedArray to be compared.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 *
 * Since: 0.4.0
 */
gboolean
garrow_chunked_array_equal(GArrowChunkedArray *chunked_array,
                           GArrowChunkedArray *other_chunked_array)
{
  const auto arrow_chunked_array = garrow_chunked_array_get_raw(chunked_array);
  const auto arrow_other_chunked_array =
    garrow_chunked_array_get_raw(other_chunked_array);
  return arrow_chunked_array->Equals(arrow_other_chunked_array);
}

/**
 * garrow_chunked_array_get_value_data_type:
 * @chunked_array: A #GArrowChunkedArray.
 *
 * Returns: (transfer full): The #GArrowDataType of the value of
 *   the chunked array.
 *
 * Since: 0.9.0
 */
GArrowDataType *
garrow_chunked_array_get_value_data_type(GArrowChunkedArray *chunked_array)
{
  auto priv = GARROW_CHUNKED_ARRAY_GET_PRIVATE(chunked_array);
  if (!priv->data_type) {
    auto arrow_chunked_array = garrow_chunked_array_get_raw(chunked_array);
    auto arrow_type = arrow_chunked_array->type();
    priv->data_type = garrow_data_type_new_raw(&arrow_type);
  }
  g_object_ref(priv->data_type);
  return priv->data_type;
}

/**
 * garrow_chunked_array_get_value_type:
 * @chunked_array: A #GArrowChunkedArray.
 *
 * Returns: The #GArrowType of the value of the chunked array.
 *
 * Since: 0.9.0
 */
GArrowType
garrow_chunked_array_get_value_type(GArrowChunkedArray *chunked_array)
{
  auto arrow_chunked_array = garrow_chunked_array_get_raw(chunked_array);
  auto arrow_type = arrow_chunked_array->type();
  return garrow_type_from_raw(arrow_type->id());
}

/**
 * garrow_chunked_array_get_length:
 * @chunked_array: A #GArrowChunkedArray.
 *
 * Returns: The total number of rows in the chunked array.
 *
 * Deprecated: 0.15.0: Use garrow_chunked_array_get_n_rows() instead.
 */
guint64
garrow_chunked_array_get_length(GArrowChunkedArray *chunked_array)
{
  return garrow_chunked_array_get_n_rows(chunked_array);
}

/**
 * garrow_chunked_array_get_n_rows:
 * @chunked_array: A #GArrowChunkedArray.
 *
 * Returns: The total number of rows in the chunked array.
 *
 * Since: 0.15.0
 */
guint64
garrow_chunked_array_get_n_rows(GArrowChunkedArray *chunked_array)
{
  const auto arrow_chunked_array = garrow_chunked_array_get_raw(chunked_array);
  return arrow_chunked_array->length();
}

/**
 * garrow_chunked_array_get_n_nulls:
 * @chunked_array: A #GArrowChunkedArray.
 *
 * Returns: The total number of NULL in the chunked array.
 */
guint64
garrow_chunked_array_get_n_nulls(GArrowChunkedArray *chunked_array)
{
  const auto arrow_chunked_array = garrow_chunked_array_get_raw(chunked_array);
  return arrow_chunked_array->null_count();
}

/**
 * garrow_chunked_array_get_n_chunks:
 * @chunked_array: A #GArrowChunkedArray.
 *
 * Returns: The total number of chunks in the chunked array.
 */
guint
garrow_chunked_array_get_n_chunks(GArrowChunkedArray *chunked_array)
{
  const auto arrow_chunked_array = garrow_chunked_array_get_raw(chunked_array);
  return arrow_chunked_array->num_chunks();
}

/**
 * garrow_chunked_array_get_chunk:
 * @chunked_array: A #GArrowChunkedArray.
 * @i: The index of the target chunk.
 *
 * Returns: (transfer full): The i-th chunk of the chunked array.
 */
GArrowArray *
garrow_chunked_array_get_chunk(GArrowChunkedArray *chunked_array,
                               guint i)
{
  const auto arrow_chunked_array = garrow_chunked_array_get_raw(chunked_array);
  auto arrow_chunk = arrow_chunked_array->chunk(i);
  return garrow_array_new_raw(&arrow_chunk);
}

/**
 * garrow_chunked_array_get_chunks:
 * @chunked_array: A #GArrowChunkedArray.
 *
 * Returns: (element-type GArrowArray) (transfer full):
 *   The chunks in the chunked array.
 */
GList *
garrow_chunked_array_get_chunks(GArrowChunkedArray *chunked_array)
{
  const auto arrow_chunked_array = garrow_chunked_array_get_raw(chunked_array);

  GList *chunks = NULL;
  for (auto arrow_chunk : arrow_chunked_array->chunks()) {
    GArrowArray *chunk = garrow_array_new_raw(&arrow_chunk);
    chunks = g_list_prepend(chunks, chunk);
  }

  return g_list_reverse(chunks);
}

/**
 * garrow_chunked_array_slice:
 * @chunked_array: A #GArrowChunkedArray.
 * @offset: The offset of sub #GArrowChunkedArray.
 * @length: The length of sub #GArrowChunkedArray.
 *
 * Returns: (transfer full): The sub #GArrowChunkedArray. It covers only from
 *   `offset` to `offset + length` range. The sub #GArrowChunkedArray shares
 *   values with the base #GArrowChunkedArray.
 */
GArrowChunkedArray  *
garrow_chunked_array_slice(GArrowChunkedArray *chunked_array,
                           guint64 offset,
                           guint64 length)
{
  const auto arrow_chunked_array = garrow_chunked_array_get_raw(chunked_array);
  auto arrow_sub_chunked_array = arrow_chunked_array->Slice(offset, length);
  return garrow_chunked_array_new_raw(&arrow_sub_chunked_array);
}

/**
 * garrow_chunked_array_to_string:
 * @chunked_array: A #GArrowChunkedArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable):
 *   The formatted chunked array content or %NULL on error.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 0.11.0
 */
gchar *
garrow_chunked_array_to_string(GArrowChunkedArray *chunked_array, GError **error)
{
  const auto arrow_chunked_array = garrow_chunked_array_get_raw(chunked_array);
  const auto string = arrow_chunked_array->ToString();
  return g_strdup(string.c_str());
}

/**
 * garrow_chunked_array_combine:
 * @chunked_array: A #GArrowChunkedArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The combined array that has
 *   all data in all chunks.
 *
 * Since: 4.0.0
 */
GArrowArray *
garrow_chunked_array_combine(GArrowChunkedArray *chunked_array, GError **error)
{
  const auto arrow_chunked_array = garrow_chunked_array_get_raw(chunked_array);
  auto arrow_combined_array = arrow::Concatenate(arrow_chunked_array->chunks());
  if (garrow::check(error,
                    arrow_combined_array,
                    "[chunked-array][combine]")) {
    return garrow_array_new_raw(&(*arrow_combined_array));
  } else {
    return NULL;
  }
}

G_END_DECLS

GArrowChunkedArray *
garrow_chunked_array_new_raw(
  std::shared_ptr<arrow::ChunkedArray> *arrow_chunked_array)
{
  return garrow_chunked_array_new_raw(arrow_chunked_array, nullptr);
}

GArrowChunkedArray *
garrow_chunked_array_new_raw(
  std::shared_ptr<arrow::ChunkedArray> *arrow_chunked_array,
  GArrowDataType *data_type)
{
  auto chunked_array =
    GARROW_CHUNKED_ARRAY(g_object_new(GARROW_TYPE_CHUNKED_ARRAY,
                                      "chunked-array", arrow_chunked_array,
                                      "data-type", data_type,
                                      NULL));
  return chunked_array;
}

std::shared_ptr<arrow::ChunkedArray>
garrow_chunked_array_get_raw(GArrowChunkedArray *chunked_array)
{
  auto priv = GARROW_CHUNKED_ARRAY_GET_PRIVATE(chunked_array);
  return priv->chunked_array;
}
