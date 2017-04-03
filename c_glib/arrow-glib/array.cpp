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

#include <arrow-glib/array.hpp>
#include <arrow-glib/binary-array.h>
#include <arrow-glib/boolean-array.h>
#include <arrow-glib/data-type.hpp>
#include <arrow-glib/double-array.h>
#include <arrow-glib/float-array.h>
#include <arrow-glib/int8-array.h>
#include <arrow-glib/int16-array.h>
#include <arrow-glib/int32-array.h>
#include <arrow-glib/int64-array.h>
#include <arrow-glib/list-array.h>
#include <arrow-glib/null-array.h>
#include <arrow-glib/string-array.h>
#include <arrow-glib/struct-array.h>
#include <arrow-glib/type.hpp>
#include <arrow-glib/uint8-array.h>
#include <arrow-glib/uint16-array.h>
#include <arrow-glib/uint32-array.h>
#include <arrow-glib/uint64-array.h>

#include <iostream>

G_BEGIN_DECLS

/**
 * SECTION: array
 * @short_description: Base class for all array classes
 *
 * #GArrowArray is a base class for all array classes such as
 * #GArrowBooleanArray.
 *
 * Array is immutable. You need to use array builder class such as
 * #GArrowBooleanArrayBuilder to create a new array.
 */

typedef struct GArrowArrayPrivate_ {
  std::shared_ptr<arrow::Array> array;
} GArrowArrayPrivate;

enum {
  PROP_0,
  PROP_ARRAY
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowArray, garrow_array, G_TYPE_OBJECT)

#define GARROW_ARRAY_GET_PRIVATE(obj)                                   \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj), GARROW_TYPE_ARRAY, GArrowArrayPrivate))

static void
garrow_array_finalize(GObject *object)
{
  auto priv = GARROW_ARRAY_GET_PRIVATE(object);

  priv->array = nullptr;

  G_OBJECT_CLASS(garrow_array_parent_class)->finalize(object);
}

static void
garrow_array_set_property(GObject *object,
                          guint prop_id,
                          const GValue *value,
                          GParamSpec *pspec)
{
  auto priv = GARROW_ARRAY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_ARRAY:
    priv->array =
      *static_cast<std::shared_ptr<arrow::Array> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_array_get_property(GObject *object,
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
garrow_array_init(GArrowArray *object)
{
}

static void
garrow_array_class_init(GArrowArrayClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_array_finalize;
  gobject_class->set_property = garrow_array_set_property;
  gobject_class->get_property = garrow_array_get_property;

  spec = g_param_spec_pointer("array",
                              "Array",
                              "The raw std::shared<arrow::Array> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_ARRAY, spec);
}

/**
 * garrow_array_is_null:
 * @array: A #GArrowArray.
 * @i: The index of the target value.
 *
 * Returns: Whether the i-th value is null or not.
 */
gboolean
garrow_array_is_null(GArrowArray *array, gint64 i)
{
  auto arrow_array = garrow_array_get_raw(array);
  return arrow_array->IsNull(i);
}

/**
 * garrow_array_get_length:
 * @array: A #GArrowArray.
 *
 * Returns: The number of rows in the array.
 */
gint64
garrow_array_get_length(GArrowArray *array)
{
  auto arrow_array = garrow_array_get_raw(array);
  return arrow_array->length();
}

/**
 * garrow_array_get_offset:
 * @array: A #GArrowArray.
 *
 * Returns: The number of values in the array.
 */
gint64
garrow_array_get_offset(GArrowArray *array)
{
  auto arrow_array = garrow_array_get_raw(array);
  return arrow_array->offset();
}

/**
 * garrow_array_get_n_nulls:
 * @array: A #GArrowArray.
 *
 * Returns: The number of NULLs in the array.
 */
gint64
garrow_array_get_n_nulls(GArrowArray *array)
{
  auto arrow_array = garrow_array_get_raw(array);
  return arrow_array->null_count();
}

/**
 * garrow_array_get_value_data_type:
 * @array: A #GArrowArray.
 *
 * Since: 0.3.0
 * Returns: (transfer full): The #GArrowDataType for each value of the
 *   array.
 */
GArrowDataType *
garrow_array_get_value_data_type(GArrowArray *array)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_data_type = arrow_array->type();
  return garrow_data_type_new_raw(&arrow_data_type);
}

/**
 * garrow_array_get_value_type:
 * @array: A #GArrowArray.
 *
 * Since: 0.3.0
 * Returns: The #GArrowType for each value of the array.
 */
GArrowType
garrow_array_get_value_type(GArrowArray *array)
{
  auto arrow_array = garrow_array_get_raw(array);
  return garrow_type_from_raw(arrow_array->type_enum());
}

/**
 * garrow_array_slice:
 * @array: A #GArrowArray.
 * @offset: The offset of sub #GArrowArray.
 * @length: The length of sub #GArrowArray.
 *
 * Returns: (transfer full): The sub #GArrowArray. It covers only from
 *   `offset` to `offset + length` range. The sub #GArrowArray shares
 *   values with the base #GArrowArray.
 */
GArrowArray *
garrow_array_slice(GArrowArray *array,
                   gint64 offset,
                   gint64 length)
{
  const auto arrow_array = garrow_array_get_raw(array);
  auto arrow_sub_array = arrow_array->Slice(offset, length);
  return garrow_array_new_raw(&arrow_sub_array);
}

G_END_DECLS

GArrowArray *
garrow_array_new_raw(std::shared_ptr<arrow::Array> *arrow_array)
{
  GType type;
  GArrowArray *array;

  switch ((*arrow_array)->type_enum()) {
  case arrow::Type::type::NA:
    type = GARROW_TYPE_NULL_ARRAY;
    break;
  case arrow::Type::type::BOOL:
    type = GARROW_TYPE_BOOLEAN_ARRAY;
    break;
  case arrow::Type::type::UINT8:
    type = GARROW_TYPE_UINT8_ARRAY;
    break;
  case arrow::Type::type::INT8:
    type = GARROW_TYPE_INT8_ARRAY;
    break;
  case arrow::Type::type::UINT16:
    type = GARROW_TYPE_UINT16_ARRAY;
    break;
  case arrow::Type::type::INT16:
    type = GARROW_TYPE_INT16_ARRAY;
    break;
  case arrow::Type::type::UINT32:
    type = GARROW_TYPE_UINT32_ARRAY;
    break;
  case arrow::Type::type::INT32:
    type = GARROW_TYPE_INT32_ARRAY;
    break;
  case arrow::Type::type::UINT64:
    type = GARROW_TYPE_UINT64_ARRAY;
    break;
  case arrow::Type::type::INT64:
    type = GARROW_TYPE_INT64_ARRAY;
    break;
  case arrow::Type::type::FLOAT:
    type = GARROW_TYPE_FLOAT_ARRAY;
    break;
  case arrow::Type::type::DOUBLE:
    type = GARROW_TYPE_DOUBLE_ARRAY;
    break;
  case arrow::Type::type::BINARY:
    type = GARROW_TYPE_BINARY_ARRAY;
    break;
  case arrow::Type::type::STRING:
    type = GARROW_TYPE_STRING_ARRAY;
    break;
  case arrow::Type::type::LIST:
    type = GARROW_TYPE_LIST_ARRAY;
    break;
  case arrow::Type::type::STRUCT:
    type = GARROW_TYPE_STRUCT_ARRAY;
    break;
  default:
    type = GARROW_TYPE_ARRAY;
    break;
  }
  array = GARROW_ARRAY(g_object_new(type,
                                    "array", arrow_array,
                                    NULL));
  return array;
}

std::shared_ptr<arrow::Array>
garrow_array_get_raw(GArrowArray *array)
{
  auto priv = GARROW_ARRAY_GET_PRIVATE(array);
  return priv->array;
}
