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

#include <glib.h>

#include <arrow/api.h>

static inline std::shared_ptr<arrow::KeyValueMetadata>
garrow_internal_hash_table_to_metadata(GHashTable *metadata)
{
  auto arrow_metadata = std::make_shared<arrow::KeyValueMetadata>();
  g_hash_table_foreach(metadata,
                       [](gpointer key,
                          gpointer value,
                          gpointer user_data) {
                         auto arrow_metadata =
                           static_cast<std::shared_ptr<arrow::KeyValueMetadata> *>(user_data);
                         (*arrow_metadata)->Append(static_cast<gchar *>(key),
                                                   static_cast<gchar *>(value));
                       },
                       &arrow_metadata);
  return arrow_metadata;
}
