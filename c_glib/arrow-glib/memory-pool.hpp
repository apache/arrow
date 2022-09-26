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

#include <arrow/api.h>

#include <arrow-glib/memory-pool.h>

GArrowMemoryPool *
garrow_memory_pool_new_raw(arrow::MemoryPool *arrow_memory_pool);
GArrowMemoryPool *
garrow_memory_pool_new_raw_bytes(arrow::MemoryPool *arrow_memory_pool);
arrow::MemoryPool *
garrow_memory_pool_get_raw(GArrowMemoryPool *buffer);
