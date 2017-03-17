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
#include <arrow-glib/data-type.hpp>
#include <arrow-glib/list-array.h>

G_BEGIN_DECLS

/**
 * SECTION: list-array
 * @short_description: List array class
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowListArray is a class for list array. It can store zero
 * or more list data.
 *
 * #GArrowListArray is immutable. You need to use
 * #GArrowListArrayBuilder to create a new array.
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

G_END_DECLS
