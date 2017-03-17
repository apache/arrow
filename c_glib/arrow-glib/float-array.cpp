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
#include <arrow-glib/float-array.h>

G_BEGIN_DECLS

/**
 * SECTION: float-array
 * @short_description: 32-bit floating point array class
 *
 * #GArrowFloatArray is a class for 32-bit floating point array. It
 * can store zero or more 32-bit floating data.
 *
 * #GArrowFloatArray is immutable. You need to use
 * #GArrowFloatArrayBuilder to create a new array.
 */

G_DEFINE_TYPE(GArrowFloatArray,               \
              garrow_float_array,             \
              GARROW_TYPE_ARRAY)

static void
garrow_float_array_init(GArrowFloatArray *object)
{
}

static void
garrow_float_array_class_init(GArrowFloatArrayClass *klass)
{
}

/**
 * garrow_float_array_get_value:
 * @array: A #GArrowFloatArray.
 * @i: The index of the target value.
 *
 * Returns: The i-th value.
 */
gfloat
garrow_float_array_get_value(GArrowFloatArray *array,
                             gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return static_cast<arrow::FloatArray *>(arrow_array.get())->Value(i);
}

G_END_DECLS
