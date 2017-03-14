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
#include <arrow-glib/binary-array-builder.h>
#include <arrow-glib/error.hpp>

G_BEGIN_DECLS

/**
 * SECTION: binary-array-builder
 * @short_description: Binary array builder class
 *
 * #GArrowBinaryArrayBuilder is the class to create a new
 * #GArrowBinaryArray.
 */

G_DEFINE_TYPE(GArrowBinaryArrayBuilder,
              garrow_binary_array_builder,
              GARROW_TYPE_ARRAY_BUILDER)

static void
garrow_binary_array_builder_init(GArrowBinaryArrayBuilder *builder)
{
}

static void
garrow_binary_array_builder_class_init(GArrowBinaryArrayBuilderClass *klass)
{
}

/**
 * garrow_binary_array_builder_new:
 *
 * Returns: A newly created #GArrowBinaryArrayBuilder.
 */
GArrowBinaryArrayBuilder *
garrow_binary_array_builder_new(void)
{
  auto memory_pool = arrow::default_memory_pool();
  auto arrow_builder =
    std::make_shared<arrow::BinaryBuilder>(memory_pool, arrow::binary());
  auto builder =
    GARROW_BINARY_ARRAY_BUILDER(g_object_new(GARROW_TYPE_BINARY_ARRAY_BUILDER,
                                             "array-builder", &arrow_builder,
                                             NULL));
  return builder;
}

/**
 * garrow_binary_array_builder_append:
 * @builder: A #GArrowBinaryArrayBuilder.
 * @value: (array length=length): A binary value.
 * @length: A value length.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 */
gboolean
garrow_binary_array_builder_append(GArrowBinaryArrayBuilder *builder,
                                   const guint8 *value,
                                   gint32 length,
                                   GError **error)
{
  auto arrow_builder =
    static_cast<arrow::BinaryBuilder *>(
      garrow_array_builder_get_raw(GARROW_ARRAY_BUILDER(builder)).get());

  auto status = arrow_builder->Append(value, length);
  if (status.ok()) {
    return TRUE;
  } else {
    garrow_error_set(error, status, "[binary-array-builder][append]");
    return FALSE;
  }
}

/**
 * garrow_binary_array_builder_append_null:
 * @builder: A #GArrowBinaryArrayBuilder.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 */
gboolean
garrow_binary_array_builder_append_null(GArrowBinaryArrayBuilder *builder,
                                        GError **error)
{
  auto arrow_builder =
    static_cast<arrow::BinaryBuilder *>(
      garrow_array_builder_get_raw(GARROW_ARRAY_BUILDER(builder)).get());

  auto status = arrow_builder->AppendNull();
  if (status.ok()) {
    return TRUE;
  } else {
    garrow_error_set(error, status, "[binary-array-builder][append-null]");
    return FALSE;
  }
}

G_END_DECLS
