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

#include <arrow-glib/data-type.hpp>
#include <arrow-glib/field.hpp>
#include <arrow-glib/type.hpp>

G_BEGIN_DECLS

/**
 * SECTION: data-type
 * @section_id: data-type-classes
 * @title: Data type classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowDataType is a base class for all data type classes such as
 * #GArrowBooleanDataType.
 *
 * #GArrowNullDataType is a class for null data type.
 *
 * #GArrowBooleanDataType is a class for boolean data type.
 *
 * #GArrowInt8DataType is a class for 8-bit integer data type.
 *
 * #GArrowUInt8DataType is a class for 8-bit unsigned integer data type.
 *
 * #GArrowInt16DataType is a class for 16-bit integer data type.
 *
 * #GArrowUInt16DataType is a class for 16-bit unsigned integer data type.
 *
 * #GArrowInt32DataType is a class for 32-bit integer data type.
 *
 * #GArrowUInt32DataType is a class for 32-bit unsigned integer data type.
 *
 * #GArrowInt64DataType is a class for 64-bit integer data type.
 *
 * #GArrowUInt64DataType is a class for 64-bit unsigned integer data type.
 *
 * #GArrowFloatDataType is a class for 32-bit floating point data
 * type.
 *
 * #GArrowDoubleDataType is a class for 64-bit floating point data
 * type.
 *
 * #GArrowBinaryDataType is a class for binary data type.
 *
 * #GArrowStringDataType is a class for UTF-8 encoded string data
 * type.
 *
 * #GArrowData32DataType is a class for the number of days since UNIX
 * epoch in 32-bit signed integer data type.
 *
 * #GArrowData64DataType is a class for the number of milliseconds
 * since UNIX epoch in 64-bit signed integer data type.
 *
 * #GArrowListDataType is a class for list data type.
 *
 * #GArrowStructDataType is a class for struct data type.
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
 * @other_data_type: A #GArrowDataType to be compared.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
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
 * garrow_data_type_get_id:
 * @data_type: A #GArrowDataType.
 *
 * Returns: The #GArrowType of the data type.
 */
GArrowType
garrow_data_type_get_id(GArrowDataType *data_type)
{
  const auto arrow_data_type = garrow_data_type_get_raw(data_type);
  return garrow_type_from_raw(arrow_data_type->id());
}


G_DEFINE_TYPE(GArrowNullDataType,                \
              garrow_null_data_type,             \
              GARROW_TYPE_DATA_TYPE)

static void
garrow_null_data_type_init(GArrowNullDataType *object)
{
}

static void
garrow_null_data_type_class_init(GArrowNullDataTypeClass *klass)
{
}

/**
 * garrow_null_data_type_new:
 *
 * Returns: The newly created null data type.
 */
GArrowNullDataType *
garrow_null_data_type_new(void)
{
  auto arrow_data_type = arrow::null();

  GArrowNullDataType *data_type =
    GARROW_NULL_DATA_TYPE(g_object_new(GARROW_TYPE_NULL_DATA_TYPE,
                                       "data-type", &arrow_data_type,
                                       NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowBooleanDataType,                \
              garrow_boolean_data_type,             \
              GARROW_TYPE_DATA_TYPE)

static void
garrow_boolean_data_type_init(GArrowBooleanDataType *object)
{
}

static void
garrow_boolean_data_type_class_init(GArrowBooleanDataTypeClass *klass)
{
}

/**
 * garrow_boolean_data_type_new:
 *
 * Returns: The newly created boolean data type.
 */
GArrowBooleanDataType *
garrow_boolean_data_type_new(void)
{
  auto arrow_data_type = arrow::boolean();

  GArrowBooleanDataType *data_type =
    GARROW_BOOLEAN_DATA_TYPE(g_object_new(GARROW_TYPE_BOOLEAN_DATA_TYPE,
                                          "data-type", &arrow_data_type,
                                          NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowInt8DataType,                \
              garrow_int8_data_type,             \
              GARROW_TYPE_DATA_TYPE)

static void
garrow_int8_data_type_init(GArrowInt8DataType *object)
{
}

static void
garrow_int8_data_type_class_init(GArrowInt8DataTypeClass *klass)
{
}

/**
 * garrow_int8_data_type_new:
 *
 * Returns: The newly created 8-bit integer data type.
 */
GArrowInt8DataType *
garrow_int8_data_type_new(void)
{
  auto arrow_data_type = arrow::int8();

  GArrowInt8DataType *data_type =
    GARROW_INT8_DATA_TYPE(g_object_new(GARROW_TYPE_INT8_DATA_TYPE,
                                       "data-type", &arrow_data_type,
                                       NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowUInt8DataType,                \
              garrow_uint8_data_type,             \
              GARROW_TYPE_DATA_TYPE)

static void
garrow_uint8_data_type_init(GArrowUInt8DataType *object)
{
}

static void
garrow_uint8_data_type_class_init(GArrowUInt8DataTypeClass *klass)
{
}

/**
 * garrow_uint8_data_type_new:
 *
 * Returns: The newly created 8-bit unsigned integer data type.
 */
GArrowUInt8DataType *
garrow_uint8_data_type_new(void)
{
  auto arrow_data_type = arrow::uint8();

  GArrowUInt8DataType *data_type =
    GARROW_UINT8_DATA_TYPE(g_object_new(GARROW_TYPE_UINT8_DATA_TYPE,
                                        "data-type", &arrow_data_type,
                                        NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowInt16DataType,                \
              garrow_int16_data_type,             \
              GARROW_TYPE_DATA_TYPE)

static void
garrow_int16_data_type_init(GArrowInt16DataType *object)
{
}

static void
garrow_int16_data_type_class_init(GArrowInt16DataTypeClass *klass)
{
}

/**
 * garrow_int16_data_type_new:
 *
 * Returns: The newly created 16-bit integer data type.
 */
GArrowInt16DataType *
garrow_int16_data_type_new(void)
{
  auto arrow_data_type = arrow::int16();

  GArrowInt16DataType *data_type =
    GARROW_INT16_DATA_TYPE(g_object_new(GARROW_TYPE_INT16_DATA_TYPE,
                                        "data-type", &arrow_data_type,
                                        NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowUInt16DataType,                \
              garrow_uint16_data_type,             \
              GARROW_TYPE_DATA_TYPE)

static void
garrow_uint16_data_type_init(GArrowUInt16DataType *object)
{
}

static void
garrow_uint16_data_type_class_init(GArrowUInt16DataTypeClass *klass)
{
}

/**
 * garrow_uint16_data_type_new:
 *
 * Returns: The newly created 16-bit unsigned integer data type.
 */
GArrowUInt16DataType *
garrow_uint16_data_type_new(void)
{
  auto arrow_data_type = arrow::uint16();

  GArrowUInt16DataType *data_type =
    GARROW_UINT16_DATA_TYPE(g_object_new(GARROW_TYPE_UINT16_DATA_TYPE,
                                         "data-type", &arrow_data_type,
                                         NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowInt32DataType,                \
              garrow_int32_data_type,             \
              GARROW_TYPE_DATA_TYPE)

static void
garrow_int32_data_type_init(GArrowInt32DataType *object)
{
}

static void
garrow_int32_data_type_class_init(GArrowInt32DataTypeClass *klass)
{
}

/**
 * garrow_int32_data_type_new:
 *
 * Returns: The newly created 32-bit integer data type.
 */
GArrowInt32DataType *
garrow_int32_data_type_new(void)
{
  auto arrow_data_type = arrow::int32();

  GArrowInt32DataType *data_type =
    GARROW_INT32_DATA_TYPE(g_object_new(GARROW_TYPE_INT32_DATA_TYPE,
                                        "data-type", &arrow_data_type,
                                        NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowUInt32DataType,                \
              garrow_uint32_data_type,             \
              GARROW_TYPE_DATA_TYPE)

static void
garrow_uint32_data_type_init(GArrowUInt32DataType *object)
{
}

static void
garrow_uint32_data_type_class_init(GArrowUInt32DataTypeClass *klass)
{
}

/**
 * garrow_uint32_data_type_new:
 *
 * Returns: The newly created 32-bit unsigned integer data type.
 */
GArrowUInt32DataType *
garrow_uint32_data_type_new(void)
{
  auto arrow_data_type = arrow::uint32();

  GArrowUInt32DataType *data_type =
    GARROW_UINT32_DATA_TYPE(g_object_new(GARROW_TYPE_UINT32_DATA_TYPE,
                                         "data-type", &arrow_data_type,
                                         NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowInt64DataType,                \
              garrow_int64_data_type,             \
              GARROW_TYPE_DATA_TYPE)

static void
garrow_int64_data_type_init(GArrowInt64DataType *object)
{
}

static void
garrow_int64_data_type_class_init(GArrowInt64DataTypeClass *klass)
{
}

/**
 * garrow_int64_data_type_new:
 *
 * Returns: The newly created 64-bit integer data type.
 */
GArrowInt64DataType *
garrow_int64_data_type_new(void)
{
  auto arrow_data_type = arrow::int64();

  GArrowInt64DataType *data_type =
    GARROW_INT64_DATA_TYPE(g_object_new(GARROW_TYPE_INT64_DATA_TYPE,
                                        "data-type", &arrow_data_type,
                                        NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowUInt64DataType,                \
              garrow_uint64_data_type,             \
              GARROW_TYPE_DATA_TYPE)

static void
garrow_uint64_data_type_init(GArrowUInt64DataType *object)
{
}

static void
garrow_uint64_data_type_class_init(GArrowUInt64DataTypeClass *klass)
{
}

/**
 * garrow_uint64_data_type_new:
 *
 * Returns: The newly created 64-bit unsigned integer data type.
 */
GArrowUInt64DataType *
garrow_uint64_data_type_new(void)
{
  auto arrow_data_type = arrow::uint64();

  GArrowUInt64DataType *data_type =
    GARROW_UINT64_DATA_TYPE(g_object_new(GARROW_TYPE_UINT64_DATA_TYPE,
                                         "data-type", &arrow_data_type,
                                         NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowFloatDataType,                \
              garrow_float_data_type,             \
              GARROW_TYPE_DATA_TYPE)

static void
garrow_float_data_type_init(GArrowFloatDataType *object)
{
}

static void
garrow_float_data_type_class_init(GArrowFloatDataTypeClass *klass)
{
}

/**
 * garrow_float_data_type_new:
 *
 * Returns: The newly created float data type.
 */
GArrowFloatDataType *
garrow_float_data_type_new(void)
{
  auto arrow_data_type = arrow::float32();

  GArrowFloatDataType *data_type =
    GARROW_FLOAT_DATA_TYPE(g_object_new(GARROW_TYPE_FLOAT_DATA_TYPE,
                                        "data-type", &arrow_data_type,
                                        NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowDoubleDataType,                \
              garrow_double_data_type,             \
              GARROW_TYPE_DATA_TYPE)

static void
garrow_double_data_type_init(GArrowDoubleDataType *object)
{
}

static void
garrow_double_data_type_class_init(GArrowDoubleDataTypeClass *klass)
{
}

/**
 * garrow_double_data_type_new:
 *
 * Returns: The newly created 64-bit floating point data type.
 */
GArrowDoubleDataType *
garrow_double_data_type_new(void)
{
  auto arrow_data_type = arrow::float64();

  GArrowDoubleDataType *data_type =
    GARROW_DOUBLE_DATA_TYPE(g_object_new(GARROW_TYPE_DOUBLE_DATA_TYPE,
                                         "data-type", &arrow_data_type,
                                         NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowBinaryDataType,                \
              garrow_binary_data_type,             \
              GARROW_TYPE_DATA_TYPE)

static void
garrow_binary_data_type_init(GArrowBinaryDataType *object)
{
}

static void
garrow_binary_data_type_class_init(GArrowBinaryDataTypeClass *klass)
{
}

/**
 * garrow_binary_data_type_new:
 *
 * Returns: The newly created binary data type.
 */
GArrowBinaryDataType *
garrow_binary_data_type_new(void)
{
  auto arrow_data_type = arrow::binary();

  GArrowBinaryDataType *data_type =
    GARROW_BINARY_DATA_TYPE(g_object_new(GARROW_TYPE_BINARY_DATA_TYPE,
                                         "data-type", &arrow_data_type,
                                         NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowStringDataType,                \
              garrow_string_data_type,             \
              GARROW_TYPE_DATA_TYPE)

static void
garrow_string_data_type_init(GArrowStringDataType *object)
{
}

static void
garrow_string_data_type_class_init(GArrowStringDataTypeClass *klass)
{
}

/**
 * garrow_string_data_type_new:
 *
 * Returns: The newly created UTF-8 encoded string data type.
 */
GArrowStringDataType *
garrow_string_data_type_new(void)
{
  auto arrow_data_type = arrow::utf8();

  GArrowStringDataType *data_type =
    GARROW_STRING_DATA_TYPE(g_object_new(GARROW_TYPE_STRING_DATA_TYPE,
                                         "data-type", &arrow_data_type,
                                         NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowDate32DataType,                \
              garrow_date32_data_type,             \
              GARROW_TYPE_DATA_TYPE)

static void
garrow_date32_data_type_init(GArrowDate32DataType *object)
{
}

static void
garrow_date32_data_type_class_init(GArrowDate32DataTypeClass *klass)
{
}

/**
 * garrow_date32_data_type_new:
 *
 * Returns: The newly created 64-bit floating point data type.
 *
 * Since: 0.7.0
 */
GArrowDate32DataType *
garrow_date32_data_type_new(void)
{
  auto arrow_data_type = arrow::date32();

  GArrowDate32DataType *data_type =
    GARROW_DATE32_DATA_TYPE(g_object_new(GARROW_TYPE_DATE32_DATA_TYPE,
                                         "data-type", &arrow_data_type,
                                         NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowDate64DataType,                \
              garrow_date64_data_type,             \
              GARROW_TYPE_DATA_TYPE)

static void
garrow_date64_data_type_init(GArrowDate64DataType *object)
{
}

static void
garrow_date64_data_type_class_init(GArrowDate64DataTypeClass *klass)
{
}

/**
 * garrow_date64_data_type_new:
 *
 * Returns: The newly created 64-bit floating point data type.
 *
 * Since: 0.7.0
 */
GArrowDate64DataType *
garrow_date64_data_type_new(void)
{
  auto arrow_data_type = arrow::date64();

  GArrowDate64DataType *data_type =
    GARROW_DATE64_DATA_TYPE(g_object_new(GARROW_TYPE_DATE64_DATA_TYPE,
                                         "data-type", &arrow_data_type,
                                         NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowListDataType,                \
              garrow_list_data_type,             \
              GARROW_TYPE_DATA_TYPE)

static void
garrow_list_data_type_init(GArrowListDataType *object)
{
}

static void
garrow_list_data_type_class_init(GArrowListDataTypeClass *klass)
{
}

/**
 * garrow_list_data_type_new:
 * @field: The field of elements
 *
 * Returns: The newly created list data type.
 */
GArrowListDataType *
garrow_list_data_type_new(GArrowField *field)
{
  auto arrow_field = garrow_field_get_raw(field);
  auto arrow_data_type =
    std::make_shared<arrow::ListType>(arrow_field);

  GArrowListDataType *data_type =
    GARROW_LIST_DATA_TYPE(g_object_new(GARROW_TYPE_LIST_DATA_TYPE,
                                       "data-type", &arrow_data_type,
                                       NULL));
  return data_type;
}

/**
 * garrow_list_data_type_get_value_field:
 * @list_data_type: A #GArrowListDataType.
 *
 * Returns: (transfer full): The field of value.
 */
GArrowField *
garrow_list_data_type_get_value_field(GArrowListDataType *list_data_type)
{
  auto arrow_data_type =
    garrow_data_type_get_raw(GARROW_DATA_TYPE(list_data_type));
  auto arrow_list_data_type =
    static_cast<arrow::ListType *>(arrow_data_type.get());

  auto arrow_field = arrow_list_data_type->value_field();
  auto field = garrow_field_new_raw(&arrow_field);

  return field;
}


G_DEFINE_TYPE(GArrowStructDataType,                \
              garrow_struct_data_type,             \
              GARROW_TYPE_DATA_TYPE)

static void
garrow_struct_data_type_init(GArrowStructDataType *object)
{
}

static void
garrow_struct_data_type_class_init(GArrowStructDataTypeClass *klass)
{
}

/**
 * garrow_struct_data_type_new:
 * @fields: (element-type GArrowField): The fields of the struct.
 *
 * Returns: The newly created struct data type.
 */
GArrowStructDataType *
garrow_struct_data_type_new(GList *fields)
{
  std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
  for (GList *node = fields; node; node = g_list_next(node)) {
    auto field = GARROW_FIELD(node->data);
    auto arrow_field = garrow_field_get_raw(field);
    arrow_fields.push_back(arrow_field);
  }

  auto arrow_data_type = std::make_shared<arrow::StructType>(arrow_fields);
  GArrowStructDataType *data_type =
    GARROW_STRUCT_DATA_TYPE(g_object_new(GARROW_TYPE_STRUCT_DATA_TYPE,
                                         "data-type", &arrow_data_type,
                                         NULL));
  return data_type;
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
  case arrow::Type::type::DATE32:
    type = GARROW_TYPE_DATE32_DATA_TYPE;
    break;
  case arrow::Type::type::DATE64:
    type = GARROW_TYPE_DATE64_DATA_TYPE;
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
