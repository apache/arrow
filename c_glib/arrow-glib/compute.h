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

#include <arrow-glib/gobject-type.h>
#include <arrow-glib/version.h>

G_BEGIN_DECLS

#define GARROW_TYPE_CAST_OPTIONS (garrow_cast_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowCastOptions,
                         garrow_cast_options,
                         GARROW,
                         CAST_OPTIONS,
                         GObject)
struct _GArrowCastOptionsClass
{
  GObjectClass parent_class;
};

GArrowCastOptions *garrow_cast_options_new(void);


/**
 * GArrowCountMode:
 * @GARROW_COUNT_ALL: Count all non-null values.
 * @GARROW_COUNT_NULL: Count all null values.
 *
 * They are corresponding to `arrow::compute::CountOptions::mode` values.
 */
typedef enum {
  GARROW_COUNT_ALL,
  GARROW_COUNT_NULL,
} GArrowCountMode;

#define GARROW_TYPE_COUNT_OPTIONS (garrow_count_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowCountOptions,
                         garrow_count_options,
                         GARROW,
                         COUNT_OPTIONS,
                         GObject)
struct _GArrowCountOptionsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_0_13
GArrowCountOptions *
garrow_count_options_new(void);

G_END_DECLS
