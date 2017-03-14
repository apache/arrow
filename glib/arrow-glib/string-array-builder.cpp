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
#include <arrow-glib/string-array-builder.h>
#include <arrow-glib/error.hpp>

G_BEGIN_DECLS

/**
 * SECTION: string-array-builder
 * @short_description: UTF-8 encoded string array builder class
 *
 * #GArrowStringArrayBuilder is the class to create a new
 * #GArrowStringArray.
 */

G_DEFINE_TYPE(GArrowStringArrayBuilder,
              garrow_string_array_builder,
              GARROW_TYPE_BINARY_ARRAY_BUILDER)

static void
garrow_string_array_builder_init(GArrowStringArrayBuilder *builder)
{
}

static void
garrow_string_array_builder_class_init(GArrowStringArrayBuilderClass *klass)
{
}

/**
 * garrow_string_array_builder_new:
 *
 * Returns: A newly created #GArrowStringArrayBuilder.
 */
GArrowStringArrayBuilder *
garrow_string_array_builder_new(void)
{
  auto memory_pool = arrow::default_memory_pool();
  auto arrow_builder =
    std::make_shared<arrow::StringBuilder>(memory_pool);
  auto builder =
    GARROW_STRING_ARRAY_BUILDER(g_object_new(GARROW_TYPE_STRING_ARRAY_BUILDER,
                                             "array-builder", &arrow_builder,
                                             NULL));
  return builder;
}

/**
 * garrow_string_array_builder_append:
 * @builder: A #GArrowStringArrayBuilder.
 * @value: A string value.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 */
gboolean
garrow_string_array_builder_append(GArrowStringArrayBuilder *builder,
                                   const gchar *value,
                                   GError **error)
{
  auto arrow_builder =
    static_cast<arrow::StringBuilder *>(
      garrow_array_builder_get_raw(GARROW_ARRAY_BUILDER(builder)).get());

  auto status = arrow_builder->Append(value,
                                      static_cast<gint32>(strlen(value)));
  if (status.ok()) {
    return TRUE;
  } else {
    garrow_error_set(error, status, "[string-array-builder][append]");
    return FALSE;
  }
}

G_END_DECLS
