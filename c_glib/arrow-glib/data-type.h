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

#pragma once

#include <glib-object.h>

#include <arrow-glib/type.h>

G_BEGIN_DECLS

#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowField         GArrowField;
#endif

#define GARROW_TYPE_DATA_TYPE                   \
  (garrow_data_type_get_type())
#define GARROW_DATA_TYPE(obj)                           \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_DATA_TYPE,    \
                              GArrowDataType))
#define GARROW_DATA_TYPE_CLASS(klass)                   \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_DATA_TYPE,       \
                           GArrowDataTypeClass))
#define GARROW_IS_DATA_TYPE(obj)                        \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                    \
                              GARROW_TYPE_DATA_TYPE))
#define GARROW_IS_DATA_TYPE_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_DATA_TYPE))
#define GARROW_DATA_TYPE_GET_CLASS(obj)                 \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_DATA_TYPE,     \
                             GArrowDataTypeClass))

typedef struct _GArrowDataType         GArrowDataType;
typedef struct _GArrowDataTypeClass    GArrowDataTypeClass;

/**
 * GArrowDataType:
 *
 * It wraps `arrow::DataType`.
 */
struct _GArrowDataType
{
  /*< private >*/
  GObject parent_instance;
};

struct _GArrowDataTypeClass
{
  GObjectClass parent_class;
};

GType      garrow_data_type_get_type  (void) G_GNUC_CONST;
gboolean   garrow_data_type_equal     (GArrowDataType *data_type,
                                       GArrowDataType *other_data_type);
gchar     *garrow_data_type_to_string (GArrowDataType *data_type);
GArrowType garrow_data_type_get_id    (GArrowDataType *data_type);


#define GARROW_TYPE_NULL_DATA_TYPE              \
  (garrow_null_data_type_get_type())
#define GARROW_NULL_DATA_TYPE(obj)                              \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_NULL_DATA_TYPE,       \
                              GArrowNullDataType))
#define GARROW_NULL_DATA_TYPE_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_NULL_DATA_TYPE,  \
                           GArrowNullDataTypeClass))
#define GARROW_IS_NULL_DATA_TYPE(obj)                           \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_NULL_DATA_TYPE))
#define GARROW_IS_NULL_DATA_TYPE_CLASS(klass)           \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_NULL_DATA_TYPE))
#define GARROW_NULL_DATA_TYPE_GET_CLASS(obj)                    \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_NULL_DATA_TYPE,        \
                             GArrowNullDataTypeClass))

typedef struct _GArrowNullDataType         GArrowNullDataType;
typedef struct _GArrowNullDataTypeClass    GArrowNullDataTypeClass;

/**
 * GArrowNullDataType:
 *
 * It wraps `arrow::NullType`.
 */
struct _GArrowNullDataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowNullDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType               garrow_null_data_type_get_type (void) G_GNUC_CONST;
GArrowNullDataType *garrow_null_data_type_new      (void);


#define GARROW_TYPE_BOOLEAN_DATA_TYPE           \
  (garrow_boolean_data_type_get_type())
#define GARROW_BOOLEAN_DATA_TYPE(obj)                           \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_BOOLEAN_DATA_TYPE,    \
                              GArrowBooleanDataType))
#define GARROW_BOOLEAN_DATA_TYPE_CLASS(klass)                   \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_BOOLEAN_DATA_TYPE,       \
                           GArrowBooleanDataTypeClass))
#define GARROW_IS_BOOLEAN_DATA_TYPE(obj)                        \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_BOOLEAN_DATA_TYPE))
#define GARROW_IS_BOOLEAN_DATA_TYPE_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_BOOLEAN_DATA_TYPE))
#define GARROW_BOOLEAN_DATA_TYPE_GET_CLASS(obj)                 \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_BOOLEAN_DATA_TYPE,     \
                             GArrowBooleanDataTypeClass))

typedef struct _GArrowBooleanDataType         GArrowBooleanDataType;
typedef struct _GArrowBooleanDataTypeClass    GArrowBooleanDataTypeClass;

/**
 * GArrowBooleanDataType:
 *
 * It wraps `arrow::BooleanType`.
 */
struct _GArrowBooleanDataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowBooleanDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                  garrow_boolean_data_type_get_type (void) G_GNUC_CONST;
GArrowBooleanDataType *garrow_boolean_data_type_new      (void);


#define GARROW_TYPE_INT8_DATA_TYPE            \
  (garrow_int8_data_type_get_type())
#define GARROW_INT8_DATA_TYPE(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                          \
                              GARROW_TYPE_INT8_DATA_TYPE,     \
                              GArrowInt8DataType))
#define GARROW_INT8_DATA_TYPE_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                           \
                           GARROW_TYPE_INT8_DATA_TYPE,        \
                           GArrowInt8DataTypeClass))
#define GARROW_IS_INT8_DATA_TYPE(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                          \
                              GARROW_TYPE_INT8_DATA_TYPE))
#define GARROW_IS_INT8_DATA_TYPE_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                           \
                           GARROW_TYPE_INT8_DATA_TYPE))
#define GARROW_INT8_DATA_TYPE_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                           \
                             GARROW_TYPE_INT8_DATA_TYPE,      \
                             GArrowInt8DataTypeClass))

typedef struct _GArrowInt8DataType         GArrowInt8DataType;
typedef struct _GArrowInt8DataTypeClass    GArrowInt8DataTypeClass;

/**
 * GArrowInt8DataType:
 *
 * It wraps `arrow::Int8Type`.
 */
struct _GArrowInt8DataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowInt8DataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                 garrow_int8_data_type_get_type (void) G_GNUC_CONST;
GArrowInt8DataType   *garrow_int8_data_type_new      (void);


#define GARROW_TYPE_UINT8_DATA_TYPE            \
  (garrow_uint8_data_type_get_type())
#define GARROW_UINT8_DATA_TYPE(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                           \
                              GARROW_TYPE_UINT8_DATA_TYPE,     \
                              GArrowUInt8DataType))
#define GARROW_UINT8_DATA_TYPE_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                            \
                           GARROW_TYPE_UINT8_DATA_TYPE,        \
                           GArrowUInt8DataTypeClass))
#define GARROW_IS_UINT8_DATA_TYPE(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                           \
                              GARROW_TYPE_UINT8_DATA_TYPE))
#define GARROW_IS_UINT8_DATA_TYPE_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                            \
                           GARROW_TYPE_UINT8_DATA_TYPE))
#define GARROW_UINT8_DATA_TYPE_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                            \
                             GARROW_TYPE_UINT8_DATA_TYPE,      \
                             GArrowUInt8DataTypeClass))

typedef struct _GArrowUInt8DataType         GArrowUInt8DataType;
typedef struct _GArrowUInt8DataTypeClass    GArrowUInt8DataTypeClass;

/**
 * GArrowUInt8DataType:
 *
 * It wraps `arrow::UInt8Type`.
 */
struct _GArrowUInt8DataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowUInt8DataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                 garrow_uint8_data_type_get_type (void) G_GNUC_CONST;
GArrowUInt8DataType  *garrow_uint8_data_type_new      (void);


#define GARROW_TYPE_INT16_DATA_TYPE            \
  (garrow_int16_data_type_get_type())
#define GARROW_INT16_DATA_TYPE(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                           \
                              GARROW_TYPE_INT16_DATA_TYPE,     \
                              GArrowInt16DataType))
#define GARROW_INT16_DATA_TYPE_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                            \
                           GARROW_TYPE_INT16_DATA_TYPE,        \
                           GArrowInt16DataTypeClass))
#define GARROW_IS_INT16_DATA_TYPE(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                           \
                              GARROW_TYPE_INT16_DATA_TYPE))
#define GARROW_IS_INT16_DATA_TYPE_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                            \
                           GARROW_TYPE_INT16_DATA_TYPE))
#define GARROW_INT16_DATA_TYPE_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                            \
                             GARROW_TYPE_INT16_DATA_TYPE,      \
                             GArrowInt16DataTypeClass))

typedef struct _GArrowInt16DataType         GArrowInt16DataType;
typedef struct _GArrowInt16DataTypeClass    GArrowInt16DataTypeClass;

/**
 * GArrowInt16DataType:
 *
 * It wraps `arrow::Int16Type`.
 */
struct _GArrowInt16DataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowInt16DataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                 garrow_int16_data_type_get_type (void) G_GNUC_CONST;
GArrowInt16DataType  *garrow_int16_data_type_new      (void);


#define GARROW_TYPE_UINT16_DATA_TYPE            \
  (garrow_uint16_data_type_get_type())
#define GARROW_UINT16_DATA_TYPE(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_UINT16_DATA_TYPE,     \
                              GArrowUInt16DataType))
#define GARROW_UINT16_DATA_TYPE_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_UINT16_DATA_TYPE,        \
                           GArrowUInt16DataTypeClass))
#define GARROW_IS_UINT16_DATA_TYPE(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_UINT16_DATA_TYPE))
#define GARROW_IS_UINT16_DATA_TYPE_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_UINT16_DATA_TYPE))
#define GARROW_UINT16_DATA_TYPE_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_UINT16_DATA_TYPE,      \
                             GArrowUInt16DataTypeClass))

typedef struct _GArrowUInt16DataType         GArrowUInt16DataType;
typedef struct _GArrowUInt16DataTypeClass    GArrowUInt16DataTypeClass;

/**
 * GArrowUInt16DataType:
 *
 * It wraps `arrow::UInt16Type`.
 */
struct _GArrowUInt16DataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowUInt16DataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                 garrow_uint16_data_type_get_type (void) G_GNUC_CONST;
GArrowUInt16DataType *garrow_uint16_data_type_new      (void);


#define GARROW_TYPE_INT32_DATA_TYPE            \
  (garrow_int32_data_type_get_type())
#define GARROW_INT32_DATA_TYPE(obj)                             \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_INT32_DATA_TYPE,      \
                              GArrowInt32DataType))
#define GARROW_INT32_DATA_TYPE_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_INT32_DATA_TYPE, \
                           GArrowInt32DataTypeClass))
#define GARROW_IS_INT32_DATA_TYPE(obj)                          \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_INT32_DATA_TYPE))
#define GARROW_IS_INT32_DATA_TYPE_CLASS(klass)                  \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_INT32_DATA_TYPE))
#define GARROW_INT32_DATA_TYPE_GET_CLASS(obj)                   \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_INT32_DATA_TYPE,       \
                             GArrowInt32DataTypeClass))

typedef struct _GArrowInt32DataType         GArrowInt32DataType;
typedef struct _GArrowInt32DataTypeClass    GArrowInt32DataTypeClass;

/**
 * GArrowInt32DataType:
 *
 * It wraps `arrow::Int32Type`.
 */
struct _GArrowInt32DataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowInt32DataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                 garrow_int32_data_type_get_type (void) G_GNUC_CONST;
GArrowInt32DataType  *garrow_int32_data_type_new      (void);


#define GARROW_TYPE_UINT32_DATA_TYPE            \
  (garrow_uint32_data_type_get_type())
#define GARROW_UINT32_DATA_TYPE(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_UINT32_DATA_TYPE,     \
                              GArrowUInt32DataType))
#define GARROW_UINT32_DATA_TYPE_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_UINT32_DATA_TYPE,        \
                           GArrowUInt32DataTypeClass))
#define GARROW_IS_UINT32_DATA_TYPE(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_UINT32_DATA_TYPE))
#define GARROW_IS_UINT32_DATA_TYPE_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_UINT32_DATA_TYPE))
#define GARROW_UINT32_DATA_TYPE_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_UINT32_DATA_TYPE,      \
                             GArrowUInt32DataTypeClass))

typedef struct _GArrowUInt32DataType         GArrowUInt32DataType;
typedef struct _GArrowUInt32DataTypeClass    GArrowUInt32DataTypeClass;

/**
 * GArrowUInt32DataType:
 *
 * It wraps `arrow::UInt32Type`.
 */
struct _GArrowUInt32DataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowUInt32DataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                 garrow_uint32_data_type_get_type (void) G_GNUC_CONST;
GArrowUInt32DataType *garrow_uint32_data_type_new      (void);


#define GARROW_TYPE_INT64_DATA_TYPE            \
  (garrow_int64_data_type_get_type())
#define GARROW_INT64_DATA_TYPE(obj)                             \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_INT64_DATA_TYPE,      \
                              GArrowInt64DataType))
#define GARROW_INT64_DATA_TYPE_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_INT64_DATA_TYPE, \
                           GArrowInt64DataTypeClass))
#define GARROW_IS_INT64_DATA_TYPE(obj)                          \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_INT64_DATA_TYPE))
#define GARROW_IS_INT64_DATA_TYPE_CLASS(klass)                  \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_INT64_DATA_TYPE))
#define GARROW_INT64_DATA_TYPE_GET_CLASS(obj)                   \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_INT64_DATA_TYPE,       \
                             GArrowInt64DataTypeClass))

typedef struct _GArrowInt64DataType         GArrowInt64DataType;
typedef struct _GArrowInt64DataTypeClass    GArrowInt64DataTypeClass;

/**
 * GArrowInt64DataType:
 *
 * It wraps `arrow::Int64Type`.
 */
struct _GArrowInt64DataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowInt64DataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                 garrow_int64_data_type_get_type (void) G_GNUC_CONST;
GArrowInt64DataType  *garrow_int64_data_type_new      (void);


#define GARROW_TYPE_UINT64_DATA_TYPE            \
  (garrow_uint64_data_type_get_type())
#define GARROW_UINT64_DATA_TYPE(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_UINT64_DATA_TYPE,     \
                              GArrowUInt64DataType))
#define GARROW_UINT64_DATA_TYPE_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_UINT64_DATA_TYPE,        \
                           GArrowUInt64DataTypeClass))
#define GARROW_IS_UINT64_DATA_TYPE(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_UINT64_DATA_TYPE))
#define GARROW_IS_UINT64_DATA_TYPE_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_UINT64_DATA_TYPE))
#define GARROW_UINT64_DATA_TYPE_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_UINT64_DATA_TYPE,      \
                             GArrowUInt64DataTypeClass))

typedef struct _GArrowUInt64DataType         GArrowUInt64DataType;
typedef struct _GArrowUInt64DataTypeClass    GArrowUInt64DataTypeClass;

/**
 * GArrowUInt64DataType:
 *
 * It wraps `arrow::UInt64Type`.
 */
struct _GArrowUInt64DataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowUInt64DataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                 garrow_uint64_data_type_get_type (void) G_GNUC_CONST;
GArrowUInt64DataType *garrow_uint64_data_type_new      (void);


#define GARROW_TYPE_FLOAT_DATA_TYPE           \
  (garrow_float_data_type_get_type())
#define GARROW_FLOAT_DATA_TYPE(obj)                           \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                          \
                              GARROW_TYPE_FLOAT_DATA_TYPE,    \
                              GArrowFloatDataType))
#define GARROW_FLOAT_DATA_TYPE_CLASS(klass)                   \
  (G_TYPE_CHECK_CLASS_CAST((klass),                           \
                           GARROW_TYPE_FLOAT_DATA_TYPE,       \
                           GArrowFloatDataTypeClass))
#define GARROW_IS_FLOAT_DATA_TYPE(obj)                        \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                          \
                              GARROW_TYPE_FLOAT_DATA_TYPE))
#define GARROW_IS_FLOAT_DATA_TYPE_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                           \
                           GARROW_TYPE_FLOAT_DATA_TYPE))
#define GARROW_FLOAT_DATA_TYPE_GET_CLASS(obj)                 \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                           \
                             GARROW_TYPE_FLOAT_DATA_TYPE,     \
                             GArrowFloatDataTypeClass))

typedef struct _GArrowFloatDataType         GArrowFloatDataType;
typedef struct _GArrowFloatDataTypeClass    GArrowFloatDataTypeClass;

/**
 * GArrowFloatDataType:
 *
 * It wraps `arrow::FloatType`.
 */
struct _GArrowFloatDataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowFloatDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                garrow_float_data_type_get_type (void) G_GNUC_CONST;
GArrowFloatDataType *garrow_float_data_type_new      (void);


#define GARROW_TYPE_DOUBLE_DATA_TYPE           \
  (garrow_double_data_type_get_type())
#define GARROW_DOUBLE_DATA_TYPE(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_DOUBLE_DATA_TYPE,     \
                              GArrowDoubleDataType))
#define GARROW_DOUBLE_DATA_TYPE_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_DOUBLE_DATA_TYPE,        \
                           GArrowDoubleDataTypeClass))
#define GARROW_IS_DOUBLE_DATA_TYPE(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_DOUBLE_DATA_TYPE))
#define GARROW_IS_DOUBLE_DATA_TYPE_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_DOUBLE_DATA_TYPE))
#define GARROW_DOUBLE_DATA_TYPE_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_DOUBLE_DATA_TYPE,      \
                             GArrowDoubleDataTypeClass))

typedef struct _GArrowDoubleDataType         GArrowDoubleDataType;
typedef struct _GArrowDoubleDataTypeClass    GArrowDoubleDataTypeClass;

/**
 * GArrowDoubleDataType:
 *
 * It wraps `arrow::DoubleType`.
 */
struct _GArrowDoubleDataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowDoubleDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                 garrow_double_data_type_get_type (void) G_GNUC_CONST;
GArrowDoubleDataType *garrow_double_data_type_new      (void);


#define GARROW_TYPE_BINARY_DATA_TYPE            \
  (garrow_binary_data_type_get_type())
#define GARROW_BINARY_DATA_TYPE(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_BINARY_DATA_TYPE,     \
                              GArrowBinaryDataType))
#define GARROW_BINARY_DATA_TYPE_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_BINARY_DATA_TYPE,        \
                           GArrowBinaryDataTypeClass))
#define GARROW_IS_BINARY_DATA_TYPE(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_BINARY_DATA_TYPE))
#define GARROW_IS_BINARY_DATA_TYPE_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_BINARY_DATA_TYPE))
#define GARROW_BINARY_DATA_TYPE_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_BINARY_DATA_TYPE,      \
                             GArrowBinaryDataTypeClass))

typedef struct _GArrowBinaryDataType         GArrowBinaryDataType;
typedef struct _GArrowBinaryDataTypeClass    GArrowBinaryDataTypeClass;

/**
 * GArrowBinaryDataType:
 *
 * It wraps `arrow::BinaryType`.
 */
struct _GArrowBinaryDataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowBinaryDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                 garrow_binary_data_type_get_type (void) G_GNUC_CONST;
GArrowBinaryDataType *garrow_binary_data_type_new      (void);


#define GARROW_TYPE_STRING_DATA_TYPE            \
  (garrow_string_data_type_get_type())
#define GARROW_STRING_DATA_TYPE(obj)                           \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                           \
                              GARROW_TYPE_STRING_DATA_TYPE,    \
                              GArrowStringDataType))
#define GARROW_STRING_DATA_TYPE_CLASS(klass)                   \
  (G_TYPE_CHECK_CLASS_CAST((klass),                            \
                           GARROW_TYPE_STRING_DATA_TYPE,       \
                           GArrowStringDataTypeClass))
#define GARROW_IS_STRING_DATA_TYPE(obj)                        \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                           \
                              GARROW_TYPE_STRING_DATA_TYPE))
#define GARROW_IS_STRING_DATA_TYPE_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                            \
                           GARROW_TYPE_STRING_DATA_TYPE))
#define GARROW_STRING_DATA_TYPE_GET_CLASS(obj)                 \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                            \
                             GARROW_TYPE_STRING_DATA_TYPE,     \
                             GArrowStringDataTypeClass))

typedef struct _GArrowStringDataType         GArrowStringDataType;
typedef struct _GArrowStringDataTypeClass    GArrowStringDataTypeClass;

/**
 * GArrowStringDataType:
 *
 * It wraps `arrow::StringType`.
 */
struct _GArrowStringDataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowStringDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                 garrow_string_data_type_get_type (void) G_GNUC_CONST;
GArrowStringDataType *garrow_string_data_type_new      (void);


#define GARROW_TYPE_DATE32_DATA_TYPE           \
  (garrow_date32_data_type_get_type())
#define GARROW_DATE32_DATA_TYPE(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_DATE32_DATA_TYPE,     \
                              GArrowDate32DataType))
#define GARROW_DATE32_DATA_TYPE_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_DATE32_DATA_TYPE,        \
                           GArrowDate32DataTypeClass))
#define GARROW_IS_DATE32_DATA_TYPE(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_DATE32_DATA_TYPE))
#define GARROW_IS_DATE32_DATA_TYPE_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_DATE32_DATA_TYPE))
#define GARROW_DATE32_DATA_TYPE_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_DATE32_DATA_TYPE,      \
                             GArrowDate32DataTypeClass))

typedef struct _GArrowDate32DataType         GArrowDate32DataType;
typedef struct _GArrowDate32DataTypeClass    GArrowDate32DataTypeClass;

/**
 * GArrowDate32DataType:
 *
 * It wraps `arrow::Date32Type`.
 */
struct _GArrowDate32DataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowDate32DataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                 garrow_date32_data_type_get_type (void) G_GNUC_CONST;
GArrowDate32DataType *garrow_date32_data_type_new      (void);


#define GARROW_TYPE_DATE64_DATA_TYPE           \
  (garrow_date64_data_type_get_type())
#define GARROW_DATE64_DATA_TYPE(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_DATE64_DATA_TYPE,     \
                              GArrowDate64DataType))
#define GARROW_DATE64_DATA_TYPE_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_DATE64_DATA_TYPE,        \
                           GArrowDate64DataTypeClass))
#define GARROW_IS_DATE64_DATA_TYPE(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_DATE64_DATA_TYPE))
#define GARROW_IS_DATE64_DATA_TYPE_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_DATE64_DATA_TYPE))
#define GARROW_DATE64_DATA_TYPE_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_DATE64_DATA_TYPE,      \
                             GArrowDate64DataTypeClass))

typedef struct _GArrowDate64DataType         GArrowDate64DataType;
typedef struct _GArrowDate64DataTypeClass    GArrowDate64DataTypeClass;

/**
 * GArrowDate64DataType:
 *
 * It wraps `arrow::Date64Type`.
 */
struct _GArrowDate64DataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowDate64DataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                 garrow_date64_data_type_get_type (void) G_GNUC_CONST;
GArrowDate64DataType *garrow_date64_data_type_new      (void);


#define GARROW_TYPE_TIMESTAMP_DATA_TYPE         \
  (garrow_timestamp_data_type_get_type())
#define GARROW_TIMESTAMP_DATA_TYPE(obj)                         \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_TIMESTAMP_DATA_TYPE,  \
                              GArrowTimestampDataType))
#define GARROW_TIMESTAMP_DATA_TYPE_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_TIMESTAMP_DATA_TYPE,     \
                           GArrowTimestampDataTypeClass))
#define GARROW_IS_TIMESTAMP_DATA_TYPE(obj)                      \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_TIMESTAMP_DATA_TYPE))
#define GARROW_IS_TIMESTAMP_DATA_TYPE_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_TIMESTAMP_DATA_TYPE))
#define GARROW_TIMESTAMP_DATA_TYPE_GET_CLASS(obj)               \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_TIMESTAMP_DATA_TYPE,   \
                             GArrowTimestampDataTypeClass))

typedef struct _GArrowTimestampDataType         GArrowTimestampDataType;
typedef struct _GArrowTimestampDataTypeClass    GArrowTimestampDataTypeClass;

/**
 * GArrowTimestampDataType:
 *
 * It wraps `arrow::TimestampType`.
 */
struct _GArrowTimestampDataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowTimestampDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                 garrow_timestamp_data_type_get_type (void) G_GNUC_CONST;
GArrowTimestampDataType *garrow_timestamp_data_type_new   (GArrowTimeUnit unit);


#define GARROW_TYPE_TIME_DATA_TYPE              \
  (garrow_time_data_type_get_type())
#define GARROW_TIME_DATA_TYPE(obj)                              \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_TIME_DATA_TYPE,       \
                              GArrowTimeDataType))
#define GARROW_TIME_DATA_TYPE_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_TIME_DATA_TYPE,  \
                           GArrowTimeDataTypeClass))
#define GARROW_IS_TIME_DATA_TYPE(obj)                           \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_TIME_DATA_TYPE))
#define GARROW_IS_TIME_DATA_TYPE_CLASS(klass)           \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_TIME_DATA_TYPE))
#define GARROW_TIME_DATA_TYPE_GET_CLASS(obj)                    \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_TIME_DATA_TYPE,        \
                             GArrowTimeDataTypeClass))

typedef struct _GArrowTimeDataType         GArrowTimeDataType;
typedef struct _GArrowTimeDataTypeClass    GArrowTimeDataTypeClass;

/**
 * GArrowTimeDataType:
 *
 * It wraps `arrow::TimeType`.
 */
struct _GArrowTimeDataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowTimeDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType          garrow_time_data_type_get_type (void) G_GNUC_CONST;
GArrowTimeUnit garrow_time_data_type_get_unit (GArrowTimeDataType *time_data_type);


#define GARROW_TYPE_TIME32_DATA_TYPE           \
  (garrow_time32_data_type_get_type())
#define GARROW_TIME32_DATA_TYPE(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_TIME32_DATA_TYPE,     \
                              GArrowTime32DataType))
#define GARROW_TIME32_DATA_TYPE_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_TIME32_DATA_TYPE,        \
                           GArrowTime32DataTypeClass))
#define GARROW_IS_TIME32_DATA_TYPE(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_TIME32_DATA_TYPE))
#define GARROW_IS_TIME32_DATA_TYPE_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_TIME32_DATA_TYPE))
#define GARROW_TIME32_DATA_TYPE_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_TIME32_DATA_TYPE,      \
                             GArrowTime32DataTypeClass))

typedef struct _GArrowTime32DataType         GArrowTime32DataType;
typedef struct _GArrowTime32DataTypeClass    GArrowTime32DataTypeClass;

/**
 * GArrowTime32DataType:
 *
 * It wraps `arrow::Time32Type`.
 */
struct _GArrowTime32DataType
{
  /*< private >*/
  GArrowTimeDataType parent_instance;
};

struct _GArrowTime32DataTypeClass
{
  GArrowTimeDataTypeClass parent_class;
};

GType                 garrow_time32_data_type_get_type (void) G_GNUC_CONST;
GArrowTime32DataType *garrow_time32_data_type_new      (GArrowTimeUnit unit,
                                                        GError **error);


#define GARROW_TYPE_TIME64_DATA_TYPE           \
  (garrow_time64_data_type_get_type())
#define GARROW_TIME64_DATA_TYPE(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_TIME64_DATA_TYPE,     \
                              GArrowTime64DataType))
#define GARROW_TIME64_DATA_TYPE_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_TIME64_DATA_TYPE,        \
                           GArrowTime64DataTypeClass))
#define GARROW_IS_TIME64_DATA_TYPE(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_TIME64_DATA_TYPE))
#define GARROW_IS_TIME64_DATA_TYPE_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_TIME64_DATA_TYPE))
#define GARROW_TIME64_DATA_TYPE_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_TIME64_DATA_TYPE,      \
                             GArrowTime64DataTypeClass))

typedef struct _GArrowTime64DataType         GArrowTime64DataType;
typedef struct _GArrowTime64DataTypeClass    GArrowTime64DataTypeClass;

/**
 * GArrowTime64DataType:
 *
 * It wraps `arrow::Time64Type`.
 */
struct _GArrowTime64DataType
{
  /*< private >*/
  GArrowTimeDataType parent_instance;
};

struct _GArrowTime64DataTypeClass
{
  GArrowTimeDataTypeClass parent_class;
};

GType                 garrow_time64_data_type_get_type (void) G_GNUC_CONST;
GArrowTime64DataType *garrow_time64_data_type_new      (GArrowTimeUnit unit,
                                                        GError **error);


#define GARROW_TYPE_LIST_DATA_TYPE              \
  (garrow_list_data_type_get_type())
#define GARROW_LIST_DATA_TYPE(obj)                              \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_LIST_DATA_TYPE,       \
                              GArrowListDataType))
#define GARROW_LIST_DATA_TYPE_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_LIST_DATA_TYPE,  \
                           GArrowListDataTypeClass))
#define GARROW_IS_LIST_DATA_TYPE(obj)                           \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_LIST_DATA_TYPE))
#define GARROW_IS_LIST_DATA_TYPE_CLASS(klass)           \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_LIST_DATA_TYPE))
#define GARROW_LIST_DATA_TYPE_GET_CLASS(obj)                    \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_LIST_DATA_TYPE,        \
                             GArrowListDataTypeClass))

typedef struct _GArrowListDataType         GArrowListDataType;
typedef struct _GArrowListDataTypeClass    GArrowListDataTypeClass;

/**
 * GArrowListDataType:
 *
 * It wraps `arrow::ListType`.
 */
struct _GArrowListDataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowListDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType               garrow_list_data_type_get_type (void) G_GNUC_CONST;
GArrowListDataType *garrow_list_data_type_new      (GArrowField *field);
GArrowField *garrow_list_data_type_get_value_field (GArrowListDataType *list_data_type);


#define GARROW_TYPE_STRUCT_DATA_TYPE            \
  (garrow_struct_data_type_get_type())
#define GARROW_STRUCT_DATA_TYPE(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_STRUCT_DATA_TYPE,     \
                              GArrowStructDataType))
#define GARROW_STRUCT_DATA_TYPE_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_STRUCT_DATA_TYPE,        \
                           GArrowStructDataTypeClass))
#define GARROW_IS_STRUCT_DATA_TYPE(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_STRUCT_DATA_TYPE))
#define GARROW_IS_STRUCT_DATA_TYPE_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_STRUCT_DATA_TYPE))
#define GARROW_STRUCT_DATA_TYPE_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_STRUCT_DATA_TYPE,      \
                             GArrowStructDataTypeClass))

typedef struct _GArrowStructDataType         GArrowStructDataType;
typedef struct _GArrowStructDataTypeClass    GArrowStructDataTypeClass;

/**
 * GArrowStructDataType:
 *
 * It wraps `arrow::StructType`.
 */
struct _GArrowStructDataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowStructDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                 garrow_struct_data_type_get_type (void) G_GNUC_CONST;
GArrowStructDataType *garrow_struct_data_type_new      (GList *fields);

G_END_DECLS
