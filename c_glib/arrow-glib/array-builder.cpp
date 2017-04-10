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
#include <arrow-glib/boolean-array-builder.h>
#include <arrow-glib/double-array-builder.h>
#include <arrow-glib/float-array-builder.h>
#include <arrow-glib/int8-array-builder.h>
#include <arrow-glib/int16-array-builder.h>
#include <arrow-glib/int32-array-builder.h>
#include <arrow-glib/int64-array-builder.h>
#include <arrow-glib/list-array-builder.h>
#include <arrow-glib/string-array-builder.h>
#include <arrow-glib/struct-array-builder.h>
#include <arrow-glib/uint8-array-builder.h>
#include <arrow-glib/uint16-array-builder.h>
#include <arrow-glib/uint32-array-builder.h>
#include <arrow-glib/uint64-array-builder.h>

G_BEGIN_DECLS

/**
 * SECTION: array-builder
 * @short_description: Base class for all array builder classes.
 *
 * #GArrowArrayBuilder is a base class for all array builder classes
 * such as #GArrowBooleanArrayBuilder.
 *
 * You need to use array builder class to create a new array.
 */

typedef struct GArrowArrayBuilderPrivate_ {
  std::shared_ptr<arrow::ArrayBuilder> array_builder;
} GArrowArrayBuilderPrivate;

enum {
  PROP_0,
  PROP_ARRAY_BUILDER
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GArrowArrayBuilder,
                                    garrow_array_builder,
                                    G_TYPE_OBJECT)

#define GARROW_ARRAY_BUILDER_GET_PRIVATE(obj)                           \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                                   \
                               GARROW_TYPE_ARRAY_BUILDER,               \
                               GArrowArrayBuilderPrivate))

static void
garrow_array_builder_finalize(GObject *object)
{
  GArrowArrayBuilderPrivate *priv;

  priv = GARROW_ARRAY_BUILDER_GET_PRIVATE(object);

  priv->array_builder = nullptr;

  G_OBJECT_CLASS(garrow_array_builder_parent_class)->finalize(object);
}

static void
garrow_array_builder_set_property(GObject *object,
                                  guint prop_id,
                                  const GValue *value,
                                  GParamSpec *pspec)
{
  GArrowArrayBuilderPrivate *priv;

  priv = GARROW_ARRAY_BUILDER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_ARRAY_BUILDER:
    priv->array_builder =
      *static_cast<std::shared_ptr<arrow::ArrayBuilder> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_array_builder_get_property(GObject *object,
                                  guint prop_id,
                                  GValue *value,
                                  GParamSpec *pspec)
{
  switch (prop_id) {
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_array_builder_init(GArrowArrayBuilder *builder)
{
}

static void
garrow_array_builder_class_init(GArrowArrayBuilderClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_array_builder_finalize;
  gobject_class->set_property = garrow_array_builder_set_property;
  gobject_class->get_property = garrow_array_builder_get_property;

  spec = g_param_spec_pointer("array-builder",
                              "Array builder",
                              "The raw std::shared<arrow::ArrayBuilder> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_ARRAY_BUILDER, spec);
}

/**
 * garrow_array_builder_finish:
 * @builder: A #GArrowArrayBuilder.
 *
 * Returns: (transfer full): The built #GArrowArray.
 */
GArrowArray *
garrow_array_builder_finish(GArrowArrayBuilder *builder)
{
  auto arrow_builder = garrow_array_builder_get_raw(builder);
  std::shared_ptr<arrow::Array> arrow_array;
  arrow_builder->Finish(&arrow_array);
  return garrow_array_new_raw(&arrow_array);
}

G_END_DECLS

GArrowArrayBuilder *
garrow_array_builder_new_raw(std::shared_ptr<arrow::ArrayBuilder> *arrow_builder)
{
  GType type;

  switch ((*arrow_builder)->type()->id()) {
  case arrow::Type::type::BOOL:
    type = GARROW_TYPE_BOOLEAN_ARRAY_BUILDER;
    break;
  case arrow::Type::type::UINT8:
    type = GARROW_TYPE_UINT8_ARRAY_BUILDER;
    break;
  case arrow::Type::type::INT8:
    type = GARROW_TYPE_INT8_ARRAY_BUILDER;
    break;
  case arrow::Type::type::UINT16:
    type = GARROW_TYPE_UINT16_ARRAY_BUILDER;
    break;
  case arrow::Type::type::INT16:
    type = GARROW_TYPE_INT16_ARRAY_BUILDER;
    break;
  case arrow::Type::type::UINT32:
    type = GARROW_TYPE_UINT32_ARRAY_BUILDER;
    break;
  case arrow::Type::type::INT32:
    type = GARROW_TYPE_INT32_ARRAY_BUILDER;
    break;
  case arrow::Type::type::UINT64:
    type = GARROW_TYPE_UINT64_ARRAY_BUILDER;
    break;
  case arrow::Type::type::INT64:
    type = GARROW_TYPE_INT64_ARRAY_BUILDER;
    break;
  case arrow::Type::type::FLOAT:
    type = GARROW_TYPE_FLOAT_ARRAY_BUILDER;
    break;
  case arrow::Type::type::DOUBLE:
    type = GARROW_TYPE_DOUBLE_ARRAY_BUILDER;
    break;
  case arrow::Type::type::BINARY:
    type = GARROW_TYPE_BINARY_ARRAY_BUILDER;
    break;
  case arrow::Type::type::STRING:
    type = GARROW_TYPE_STRING_ARRAY_BUILDER;
    break;
  case arrow::Type::type::LIST:
    type = GARROW_TYPE_LIST_ARRAY_BUILDER;
    break;
  case arrow::Type::type::STRUCT:
    type = GARROW_TYPE_STRUCT_ARRAY_BUILDER;
    break;
  default:
    type = GARROW_TYPE_ARRAY_BUILDER;
    break;
  }

  auto builder =
    GARROW_ARRAY_BUILDER(g_object_new(type,
                                      "array-builder", arrow_builder,
                                      NULL));
  return builder;
}

std::shared_ptr<arrow::ArrayBuilder>
garrow_array_builder_get_raw(GArrowArrayBuilder *builder)
{
  GArrowArrayBuilderPrivate *priv;

  priv = GARROW_ARRAY_BUILDER_GET_PRIVATE(builder);
  return priv->array_builder;
}
