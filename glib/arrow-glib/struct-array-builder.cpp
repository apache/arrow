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
#include <arrow-glib/data-type.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/field.hpp>
#include <arrow-glib/struct-array-builder.h>

G_BEGIN_DECLS

/**
 * SECTION: struct-array-builder
 * @short_description: Struct array builder class
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowStructArrayBuilder is the class to create a new
 * #GArrowStructArray.
 */

G_DEFINE_TYPE(GArrowStructArrayBuilder,
              garrow_struct_array_builder,
              GARROW_TYPE_ARRAY_BUILDER)

static void
garrow_struct_array_builder_init(GArrowStructArrayBuilder *builder)
{
}

static void
garrow_struct_array_builder_class_init(GArrowStructArrayBuilderClass *klass)
{
}

/**
 * garrow_struct_array_builder_new:
 * @data_type: #GArrowStructDataType for the struct.
 * @field_builders: (element-type GArrowArray): #GArrowArrayBuilders
 *   for fields.
 *
 * Returns: A newly created #GArrowStructArrayBuilder.
 */
GArrowStructArrayBuilder *
garrow_struct_array_builder_new(GArrowStructDataType *data_type,
                                GList *field_builders)
{
  auto memory_pool = arrow::default_memory_pool();
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type));
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> arrow_field_builders;
  for (GList *node = field_builders; node; node = g_list_next(node)) {
    auto field_builder = static_cast<GArrowArrayBuilder *>(node->data);
    auto arrow_field_builder = garrow_array_builder_get_raw(field_builder);
    arrow_field_builders.push_back(arrow_field_builder);
  }

  auto arrow_struct_builder =
    std::make_shared<arrow::StructBuilder>(memory_pool,
                                           arrow_data_type,
                                           arrow_field_builders);
  std::shared_ptr<arrow::ArrayBuilder> arrow_builder = arrow_struct_builder;
  auto builder = garrow_array_builder_new_raw(&arrow_builder);
  return GARROW_STRUCT_ARRAY_BUILDER(builder);
}

/**
 * garrow_struct_array_builder_append:
 * @builder: A #GArrowStructArrayBuilder.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * It appends a new struct element. To append a new struct element,
 * you need to call this function then append struct element field
 * values to all `field_builder`s. `field_value`s are the
 * #GArrowArrayBuilder specified to constructor. You can get
 * `field_builder` by garrow_struct_array_builder_get_field_builder()
 * or garrow_struct_array_builder_get_field_builders().
 *
 * |[<!-- language="C" -->
 * // TODO
 * ]|
 */
gboolean
garrow_struct_array_builder_append(GArrowStructArrayBuilder *builder,
                                   GError **error)
{
  auto arrow_builder =
    static_cast<arrow::StructBuilder *>(
      garrow_array_builder_get_raw(GARROW_ARRAY_BUILDER(builder)).get());

  auto status = arrow_builder->Append();
  if (status.ok()) {
    return TRUE;
  } else {
    garrow_error_set(error, status, "[struct-array-builder][append]");
    return FALSE;
  }
}

/**
 * garrow_struct_array_builder_append_null:
 * @builder: A #GArrowStructArrayBuilder.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * It appends a new NULL element.
 */
gboolean
garrow_struct_array_builder_append_null(GArrowStructArrayBuilder *builder,
                                        GError **error)
{
  auto arrow_builder =
    static_cast<arrow::StructBuilder *>(
      garrow_array_builder_get_raw(GARROW_ARRAY_BUILDER(builder)).get());

  auto status = arrow_builder->AppendNull();
  if (status.ok()) {
    return TRUE;
  } else {
    garrow_error_set(error, status, "[struct-array-builder][append-null]");
    return FALSE;
  }
}

/**
 * garrow_struct_array_builder_get_field_builder:
 * @builder: A #GArrowStructArrayBuilder.
 * @i: The index of the field in the struct.
 *
 * Returns: (transfer full): The #GArrowArrayBuilder for the i-th field.
 */
GArrowArrayBuilder *
garrow_struct_array_builder_get_field_builder(GArrowStructArrayBuilder *builder,
                                              gint i)
{
  auto arrow_builder =
    static_cast<arrow::StructBuilder *>(
      garrow_array_builder_get_raw(GARROW_ARRAY_BUILDER(builder)).get());
  auto arrow_field_builder = arrow_builder->field_builder(i);
  return garrow_array_builder_new_raw(&arrow_field_builder);
}

/**
 * garrow_struct_array_builder_get_field_builders:
 * @builder: A #GArrowStructArrayBuilder.
 *
 * Returns: (element-type GArrowArray) (transfer full):
 *   The #GArrowArrayBuilder for all fields.
 */
GList *
garrow_struct_array_builder_get_field_builders(GArrowStructArrayBuilder *builder)
{
  auto arrow_struct_builder =
    static_cast<arrow::StructBuilder *>(
      garrow_array_builder_get_raw(GARROW_ARRAY_BUILDER(builder)).get());

  GList *field_builders = NULL;
  for (auto arrow_field_builder : arrow_struct_builder->field_builders()) {
    auto field_builder = garrow_array_builder_new_raw(&arrow_field_builder);
    field_builders = g_list_prepend(field_builders, field_builder);
  }

  return g_list_reverse(field_builders);
}

G_END_DECLS
