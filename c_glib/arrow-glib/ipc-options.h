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

#define GARROW_TYPE_READ_OPTIONS (garrow_read_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowReadOptions,
                         garrow_read_options,
                         GARROW,
                         READ_OPTIONS,
                         GObject)
struct _GArrowReadOptionsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_1_0
GArrowReadOptions *
garrow_read_options_new(void);
GARROW_AVAILABLE_IN_1_0
int *
garrow_read_options_get_included_fields(GArrowReadOptions *options,
                                        gsize *n_fields);
GARROW_AVAILABLE_IN_1_0
void
garrow_read_options_set_included_fields(GArrowReadOptions *options,
                                        int *fields,
                                        gsize n_fields);

#define GARROW_TYPE_WRITE_OPTIONS (garrow_write_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowWriteOptions,
                         garrow_write_options,
                         GARROW,
                         WRITE_OPTIONS,
                         GObject)
struct _GArrowWriteOptionsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_1_0
GArrowWriteOptions *
garrow_write_options_new(void);

G_END_DECLS
