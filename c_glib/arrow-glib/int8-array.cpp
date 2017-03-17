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
#include <arrow-glib/int8-array.h>

G_BEGIN_DECLS

/**
 * SECTION: int8-array
 * @short_description: 8-bit integer array class
 *
 * #GArrowInt8Array is a class for 8-bit integer array. It can store
 * zero or more 8-bit integer data.
 *
 * #GArrowInt8Array is immutable. You need to use
 * #GArrowInt8ArrayBuilder to create a new array.
 */

G_DEFINE_TYPE(GArrowInt8Array,               \
              garrow_int8_array,             \
              GARROW_TYPE_ARRAY)

static void
garrow_int8_array_init(GArrowInt8Array *object)
{
}

static void
garrow_int8_array_class_init(GArrowInt8ArrayClass *klass)
{
}

/**
 * garrow_int8_array_get_value:
 * @array: A #GArrowInt8Array.
 * @i: The index of the target value.
 *
 * Returns: The i-th value.
 */
gint8
garrow_int8_array_get_value(GArrowInt8Array *array,
                            gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return static_cast<arrow::Int8Array *>(arrow_array.get())->Value(i);
}

G_END_DECLS
