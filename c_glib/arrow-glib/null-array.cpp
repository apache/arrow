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
#include <arrow-glib/null-array.h>

G_BEGIN_DECLS

/**
 * SECTION: null-array
 * @short_description: Null array class
 *
 * #GArrowNullArray is a class for null array. It can store zero
 * or more null values.
 *
 * #GArrowNullArray is immutable. You need to specify an array length
 * to create a new array.
 */

G_DEFINE_TYPE(GArrowNullArray,               \
              garrow_null_array,             \
              GARROW_TYPE_ARRAY)

static void
garrow_null_array_init(GArrowNullArray *object)
{
}

static void
garrow_null_array_class_init(GArrowNullArrayClass *klass)
{
}

/**
 * garrow_null_array_new:
 * @length: An array length.
 *
 * Returns: A newly created #GArrowNullArray.
 */
GArrowNullArray *
garrow_null_array_new(gint64 length)
{
  auto arrow_null_array = std::make_shared<arrow::NullArray>(length);
  std::shared_ptr<arrow::Array> arrow_array = arrow_null_array;
  auto array = garrow_array_new_raw(&arrow_array);
  return GARROW_NULL_ARRAY(array);
}

G_END_DECLS
