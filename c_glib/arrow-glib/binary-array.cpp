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

G_BEGIN_DECLS

/**
 * SECTION: binary-array
 * @short_description: Binary array class
 *
 * #GArrowBinaryArray is a class for binary array. It can store zero
 * or more binary data.
 *
 * #GArrowBinaryArray is immutable. You need to use
 * #GArrowBinaryArrayBuilder to create a new array.
 */

G_DEFINE_TYPE(GArrowBinaryArray,               \
              garrow_binary_array,             \
              GARROW_TYPE_ARRAY)

static void
garrow_binary_array_init(GArrowBinaryArray *object)
{
}

static void
garrow_binary_array_class_init(GArrowBinaryArrayClass *klass)
{
}

/**
 * garrow_binary_array_get_value:
 * @array: A #GArrowBinaryArray.
 * @i: The index of the target value.
 * @length: (out): The length of the value.
 *
 * Returns: (array length=length): The i-th value.
 */
const guint8 *
garrow_binary_array_get_value(GArrowBinaryArray *array,
                              gint64 i,
                              gint32 *length)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_binary_array =
    static_cast<arrow::BinaryArray *>(arrow_array.get());
  return arrow_binary_array->GetValue(i, length);
}

G_END_DECLS
