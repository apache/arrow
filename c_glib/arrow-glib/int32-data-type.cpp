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
#include <arrow-glib/int32-data-type.h>

G_BEGIN_DECLS

/**
 * SECTION: int32-data-type
 * @short_description: 32-bit integer data type
 *
 * #GArrowInt32DataType is a class for 32-bit integer data type.
 */

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

G_END_DECLS
