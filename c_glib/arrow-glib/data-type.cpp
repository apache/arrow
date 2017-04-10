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

#include <arrow-glib/boolean-data-type.h>
#include <arrow-glib/binary-data-type.h>
#include <arrow-glib/boolean-data-type.h>
#include <arrow-glib/data-type.hpp>
#include <arrow-glib/double-data-type.h>
#include <arrow-glib/float-data-type.h>
#include <arrow-glib/int8-data-type.h>
#include <arrow-glib/int16-data-type.h>
#include <arrow-glib/int32-data-type.h>
#include <arrow-glib/int64-data-type.h>
#include <arrow-glib/list-data-type.h>
#include <arrow-glib/null-data-type.h>
#include <arrow-glib/string-data-type.h>
#include <arrow-glib/struct-data-type.h>
#include <arrow-glib/type.hpp>
#include <arrow-glib/uint8-data-type.h>
#include <arrow-glib/uint16-data-type.h>
#include <arrow-glib/uint32-data-type.h>
#include <arrow-glib/uint64-data-type.h>

G_BEGIN_DECLS

/**
 * SECTION: data-type
 * @short_description: Base class for all data type classes
 *
 * #GArrowDataType is a base class for all data type classes such as
 * #GArrowBooleanDataType.
 */

typedef struct GArrowDataTypePrivate_ {
  std::shared_ptr<arrow::DataType> data_type;
} GArrowDataTypePrivate;

enum {
  PROP_0,
  PROP_DATA_TYPE
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GArrowDataType,
                                    garrow_data_type,
                                    G_TYPE_OBJECT)

#define GARROW_DATA_TYPE_GET_PRIVATE(obj)               \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                   \
                               GARROW_TYPE_DATA_TYPE,   \
                               GArrowDataTypePrivate))

static void
garrow_data_type_finalize(GObject *object)
{
  GArrowDataTypePrivate *priv;

  priv = GARROW_DATA_TYPE_GET_PRIVATE(object);

  priv->data_type = nullptr;

  G_OBJECT_CLASS(garrow_data_type_parent_class)->finalize(object);
}

static void
garrow_data_type_set_property(GObject *object,
                              guint prop_id,
                              const GValue *value,
                              GParamSpec *pspec)
{
  GArrowDataTypePrivate *priv;

  priv = GARROW_DATA_TYPE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_DATA_TYPE:
    priv->data_type =
      *static_cast<std::shared_ptr<arrow::DataType> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_data_type_get_property(GObject *object,
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
garrow_data_type_init(GArrowDataType *object)
{
}

static void
garrow_data_type_class_init(GArrowDataTypeClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_data_type_finalize;
  gobject_class->set_property = garrow_data_type_set_property;
  gobject_class->get_property = garrow_data_type_get_property;

  spec = g_param_spec_pointer("data-type",
                              "DataType",
                              "The raw std::shared<arrow::DataType> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_DATA_TYPE, spec);
}

/**
 * garrow_data_type_equal:
 * @data_type: A #GArrowDataType.
 * @other_data_type: A #GArrowDataType.
 *
 * Returns: Whether they are equal or not.
 */
gboolean
garrow_data_type_equal(GArrowDataType *data_type,
                       GArrowDataType *other_data_type)
{
  const auto arrow_data_type = garrow_data_type_get_raw(data_type);
  const auto arrow_other_data_type = garrow_data_type_get_raw(other_data_type);
  return arrow_data_type->Equals(arrow_other_data_type);
}

/**
 * garrow_data_type_to_string:
 * @data_type: A #GArrowDataType.
 *
 * Returns: The string representation of the data type. The caller
 *   must free it by g_free() when the caller doesn't need it anymore.
 */
gchar *
garrow_data_type_to_string(GArrowDataType *data_type)
{
  const auto arrow_data_type = garrow_data_type_get_raw(data_type);
  return g_strdup(arrow_data_type->ToString().c_str());
}

/**
 * garrow_data_type_type:
 * @data_type: A #GArrowDataType.
 *
 * Returns: The type of the data type.
 */
GArrowType
garrow_data_type_type(GArrowDataType *data_type)
{
  const auto arrow_data_type = garrow_data_type_get_raw(data_type);
  return garrow_type_from_raw(arrow_data_type->id());
}

G_END_DECLS

GArrowDataType *
garrow_data_type_new_raw(std::shared_ptr<arrow::DataType> *arrow_data_type)
{
  GType type;
  GArrowDataType *data_type;

  switch ((*arrow_data_type)->id()) {
  case arrow::Type::type::NA:
    type = GARROW_TYPE_NULL_DATA_TYPE;
    break;
  case arrow::Type::type::BOOL:
    type = GARROW_TYPE_BOOLEAN_DATA_TYPE;
    break;
  case arrow::Type::type::UINT8:
    type = GARROW_TYPE_UINT8_DATA_TYPE;
    break;
  case arrow::Type::type::INT8:
    type = GARROW_TYPE_INT8_DATA_TYPE;
    break;
  case arrow::Type::type::UINT16:
    type = GARROW_TYPE_UINT16_DATA_TYPE;
    break;
  case arrow::Type::type::INT16:
    type = GARROW_TYPE_INT16_DATA_TYPE;
    break;
  case arrow::Type::type::UINT32:
    type = GARROW_TYPE_UINT32_DATA_TYPE;
    break;
  case arrow::Type::type::INT32:
    type = GARROW_TYPE_INT32_DATA_TYPE;
    break;
  case arrow::Type::type::UINT64:
    type = GARROW_TYPE_UINT64_DATA_TYPE;
    break;
  case arrow::Type::type::INT64:
    type = GARROW_TYPE_INT64_DATA_TYPE;
    break;
  case arrow::Type::type::FLOAT:
    type = GARROW_TYPE_FLOAT_DATA_TYPE;
    break;
  case arrow::Type::type::DOUBLE:
    type = GARROW_TYPE_DOUBLE_DATA_TYPE;
    break;
  case arrow::Type::type::BINARY:
    type = GARROW_TYPE_BINARY_DATA_TYPE;
    break;
  case arrow::Type::type::STRING:
    type = GARROW_TYPE_STRING_DATA_TYPE;
    break;
  case arrow::Type::type::LIST:
    type = GARROW_TYPE_LIST_DATA_TYPE;
    break;
  case arrow::Type::type::STRUCT:
    type = GARROW_TYPE_STRUCT_DATA_TYPE;
    break;
  default:
    type = GARROW_TYPE_DATA_TYPE;
    break;
  }
  data_type = GARROW_DATA_TYPE(g_object_new(type,
                                            "data_type", arrow_data_type,
                                            NULL));
  return data_type;
}

std::shared_ptr<arrow::DataType>
garrow_data_type_get_raw(GArrowDataType *data_type)
{
  GArrowDataTypePrivate *priv;

  priv = GARROW_DATA_TYPE_GET_PRIVATE(data_type);
  return priv->data_type;
}
