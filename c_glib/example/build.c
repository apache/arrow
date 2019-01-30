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

#include <stdlib.h>

#include <arrow-glib/arrow-glib.h>

int
main(int argc, char **argv)
{
  GArrowArray *array;

  {
    GArrowInt32ArrayBuilder *builder;
    gboolean success = TRUE;
    GError *error = NULL;

    builder = garrow_int32_array_builder_new();
    if (success) {
      success = garrow_int32_array_builder_append_value(builder, 29, &error);
    }
    if (success) {
      success = garrow_int32_array_builder_append_value(builder, 2929, &error);
    }
    if (success) {
      success = garrow_int32_array_builder_append_value(builder, 292929, &error);
    }
    if (!success) {
      g_print("failed to append: %s\n", error->message);
      g_error_free(error);
      g_object_unref(builder);
      return EXIT_FAILURE;
    }
    array = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder), &error);
    if (!array) {
      g_print("failed to finish: %s\n", error->message);
      g_error_free(error);
      g_object_unref(builder);
      return EXIT_FAILURE;
    }
    g_object_unref(builder);
  }

  {
    gint64 i, n;

    n = garrow_array_get_length(array);
    g_print("length: %" G_GINT64_FORMAT "\n", n);
    for (i = 0; i < n; i++) {
      gint32 value;

      value = garrow_int32_array_get_value(GARROW_INT32_ARRAY(array), i);
      g_print("array[%" G_GINT64_FORMAT "] = %d\n",
              i, value);
    }
  }

  g_object_unref(array);

  return EXIT_SUCCESS;
}
