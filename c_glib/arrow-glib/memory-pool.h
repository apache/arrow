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

#include <arrow-glib/version.h>

G_BEGIN_DECLS

#define GARROW_TYPE_MEMORY_POOL (garrow_memory_pool_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowMemoryPool, garrow_memory_pool, GARROW, MEMORY_POOL, GObject)
struct _GArrowMemoryPoolClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowMemoryPool *
garrow_memory_pool_default();

GARROW_AVAILABLE_IN_ALL
gint64
garrow_memory_pool_get_bytes_allocated(GArrowMemoryPool *memory_pool);

GARROW_AVAILABLE_IN_ALL
gint64
garrow_memory_pool_get_max_memory(GArrowMemoryPool *memory_pool);

GARROW_AVAILABLE_IN_ALL
gchar *
garrow_memory_pool_get_backend_name(GArrowMemoryPool *memory_pool);

G_END_DECLS
