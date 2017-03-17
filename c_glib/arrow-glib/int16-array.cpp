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
#include <arrow-glib/int16-array.h>

G_BEGIN_DECLS

/**
 * SECTION: int16-array
 * @short_description: 16-bit integer array class
 *
 * #GArrowInt16Array is a class for 16-bit integer array. It can store
 * zero or more 16-bit integer data.
 *
 * #GArrowInt16Array is immutable. You need to use
 * #GArrowInt16ArrayBuilder to create a new array.
 */

G_DEFINE_TYPE(GArrowInt16Array,               \
              garrow_int16_array,             \
              GARROW_TYPE_ARRAY)

static void
garrow_int16_array_init(GArrowInt16Array *object)
{
}

static void
garrow_int16_array_class_init(GArrowInt16ArrayClass *klass)
{
}

/**
 * garrow_int16_array_get_value:
 * @array: A #GArrowInt16Array.
 * @i: The index of the target value.
 *
 * Returns: The i-th value.
 */
gint16
garrow_int16_array_get_value(GArrowInt16Array *array,
                             gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return static_cast<arrow::Int16Array *>(arrow_array.get())->Value(i);
}

G_END_DECLS
