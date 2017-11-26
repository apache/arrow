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
#include <arrow-glib/buffer.hpp>
#include <arrow-glib/compute.hpp>
#include <arrow-glib/data-type.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/type.hpp>

G_BEGIN_DECLS

/**
 * SECTION: composite-array
 * @section_id: composite-array-classes
 * @title: Composite array classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowListArray is a class for list array. It can store zero or
 * more list data. If you don't have Arrow format data, you need to
 * use #GArrowListArrayBuilder to create a new array.
 *
 * #GArrowStructArray is a class for struct array. It can store zero
 * or more structs. One struct has zero or more fields. If you don't
 * have Arrow format data, you need to use #GArrowStructArrayBuilder
 * to create a new array.
 */

G_DEFINE_TYPE(GArrowListArray,               \
              garrow_list_array,             \
              GARROW_TYPE_ARRAY)

static void
garrow_list_array_init(GArrowListArray *object)
{
}

static void
garrow_list_array_class_init(GArrowListArrayClass *klass)
{
}

/**
 * garrow_list_array_new:
 * @length: The number of elements.
 * @value_offsets: The offsets of @values in Arrow format.
 * @values: The values as #GArrowArray.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowListArray.
 *
 * Since: 0.4.0
 */
GArrowListArray *
garrow_list_array_new(gint64 length,
                      GArrowBuffer *value_offsets,
                      GArrowArray *values,
                      GArrowBuffer *null_bitmap,
                      gint64 n_nulls)
{
  const auto arrow_value_offsets = garrow_buffer_get_raw(value_offsets);
  const auto arrow_values = garrow_array_get_raw(values);
  const auto arrow_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_data_type = arrow::list(arrow_values->type());
  auto arrow_list_array =
    std::make_shared<arrow::ListArray>(arrow_data_type,
                                       length,
                                       arrow_value_offsets,
                                       arrow_values,
                                       arrow_bitmap,
                                       n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_list_array);
  return GARROW_LIST_ARRAY(garrow_array_new_raw(&arrow_array));
}

/**
 * garrow_list_array_get_value_type:
 * @array: A #GArrowListArray.
 *
 * Returns: (transfer full): The data type of value in each list.
 */
GArrowDataType *
garrow_list_array_get_value_type(GArrowListArray *array)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_list_array =
    static_cast<arrow::ListArray *>(arrow_array.get());
  auto arrow_value_type = arrow_list_array->value_type();
  return garrow_data_type_new_raw(&arrow_value_type);
}

/**
 * garrow_list_array_get_value:
 * @array: A #GArrowListArray.
 * @i: The index of the target value.
 *
 * Returns: (transfer full): The i-th list.
 */
GArrowArray *
garrow_list_array_get_value(GArrowListArray *array,
                            gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_list_array =
    static_cast<arrow::ListArray *>(arrow_array.get());
  auto arrow_list =
    arrow_list_array->values()->Slice(arrow_list_array->value_offset(i),
                                      arrow_list_array->value_length(i));
  return garrow_array_new_raw(&arrow_list);
}


G_DEFINE_TYPE(GArrowStructArray,               \
              garrow_struct_array,             \
              GARROW_TYPE_ARRAY)

static void
garrow_struct_array_init(GArrowStructArray *object)
{
}

static void
garrow_struct_array_class_init(GArrowStructArrayClass *klass)
{
}

/**
 * garrow_struct_array_new:
 * @data_type: The data type of the struct.
 * @length: The number of elements.
 * @children: (element-type GArrowArray): The arrays for each field
 *   as #GList of #GArrowArray.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowStructArray.
 *
 * Since: 0.4.0
 */
GArrowStructArray *
garrow_struct_array_new(GArrowDataType *data_type,
                        gint64 length,
                        GList *children,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls)
{
  const auto arrow_data_type = garrow_data_type_get_raw(data_type);
  std::vector<std::shared_ptr<arrow::Array>> arrow_children;
  for (GList *node = children; node; node = node->next) {
    GArrowArray *child = GARROW_ARRAY(node->data);
    arrow_children.push_back(garrow_array_get_raw(child));
  }
  const auto arrow_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_struct_array =
    std::make_shared<arrow::StructArray>(arrow_data_type,
                                         length,
                                         arrow_children,
                                         arrow_bitmap,
                                         n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_struct_array);
  return GARROW_STRUCT_ARRAY(garrow_array_new_raw(&arrow_array));
}

/**
 * garrow_struct_array_get_field
 * @array: A #GArrowStructArray.
 * @i: The index of the field in the struct.
 *
 * Returns: (transfer full): The i-th field.
 */
GArrowArray *
garrow_struct_array_get_field(GArrowStructArray *array,
                              gint i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_struct_array =
    static_cast<arrow::StructArray *>(arrow_array.get());
  auto arrow_field = arrow_struct_array->field(i);
  return garrow_array_new_raw(&arrow_field);
}

/**
 * garrow_struct_array_get_fields
 * @array: A #GArrowStructArray.
 *
 * Returns: (element-type GArrowArray) (transfer full):
 *   The fields in the struct.
 */
GList *
garrow_struct_array_get_fields(GArrowStructArray *array)
{
  const auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  const auto arrow_struct_array =
    static_cast<const arrow::StructArray *>(arrow_array.get());

  GList *fields = NULL;
  for (int i = 0; i < arrow_struct_array->num_fields(); ++i) {
    auto arrow_field = arrow_struct_array->field(i);
    GArrowArray *field = garrow_array_new_raw(&arrow_field);
    fields = g_list_prepend(fields, field);
  }

  return g_list_reverse(fields);
}

G_END_DECLS
