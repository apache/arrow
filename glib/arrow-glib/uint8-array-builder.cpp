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

#include <arrow-glib/array-builder.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/uint8-array-builder.h>

G_BEGIN_DECLS

/**
 * SECTION: uint8-array-builder
 * @short_description: 8-bit unsigned integer array builder class
 *
 * #GArrowUInt8ArrayBuilder is the class to create a new
 * #GArrowUInt8Array.
 */

G_DEFINE_TYPE(GArrowUInt8ArrayBuilder,
              garrow_uint8_array_builder,
              GARROW_TYPE_ARRAY_BUILDER)

static void
garrow_uint8_array_builder_init(GArrowUInt8ArrayBuilder *builder)
{
}

static void
garrow_uint8_array_builder_class_init(GArrowUInt8ArrayBuilderClass *klass)
{
}

/**
 * garrow_uint8_array_builder_new:
 *
 * Returns: A newly created #GArrowUInt8ArrayBuilder.
 */
GArrowUInt8ArrayBuilder *
garrow_uint8_array_builder_new(void)
{
  auto memory_pool = arrow::default_memory_pool();
  auto arrow_builder =
    std::make_shared<arrow::UInt8Builder>(memory_pool, arrow::uint8());
  auto builder =
    GARROW_UINT8_ARRAY_BUILDER(g_object_new(GARROW_TYPE_UINT8_ARRAY_BUILDER,
                                            "array-builder", &arrow_builder,
                                            NULL));
  return builder;
}

/**
 * garrow_uint8_array_builder_append:
 * @builder: A #GArrowUInt8ArrayBuilder.
 * @value: An uint8 value.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 */
gboolean
garrow_uint8_array_builder_append(GArrowUInt8ArrayBuilder *builder,
                                  guint8 value,
                                  GError **error)
{
  auto arrow_builder =
    static_cast<arrow::UInt8Builder *>(
      garrow_array_builder_get_raw(GARROW_ARRAY_BUILDER(builder)).get());

  auto status = arrow_builder->Append(value);
  if (status.ok()) {
    return TRUE;
  } else {
    garrow_error_set(error, status, "[uint8-array-builder][append]");
    return FALSE;
  }
}

/**
 * garrow_uint8_array_builder_append_null:
 * @builder: A #GArrowUInt8ArrayBuilder.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 */
gboolean
garrow_uint8_array_builder_append_null(GArrowUInt8ArrayBuilder *builder,
                                       GError **error)
{
  auto arrow_builder =
    static_cast<arrow::UInt8Builder *>(
      garrow_array_builder_get_raw(GARROW_ARRAY_BUILDER(builder)).get());

  auto status = arrow_builder->AppendNull();
  if (status.ok()) {
    return TRUE;
  } else {
    garrow_error_set(error, status, "[uint8-array-builder][append-null]");
    return FALSE;
  }
}

G_END_DECLS
