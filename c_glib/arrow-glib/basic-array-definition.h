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

G_BEGIN_DECLS

#define GARROW_TYPE_ARRAY (garrow_array_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowArray,
                         garrow_array,
                         GARROW,
                         ARRAY,
                         GObject)
struct _GArrowArrayClass
{
  GObjectClass parent_class;
};

#define GARROW_TYPE_EXTENSION_ARRAY (garrow_extension_array_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowExtensionArray,
                         garrow_extension_array,
                         GARROW,
                         EXTENSION_ARRAY,
                         GArrowArray)
struct _GArrowExtensionArrayClass
{
  GArrowArrayClass parent_class;
};

G_END_DECLS
