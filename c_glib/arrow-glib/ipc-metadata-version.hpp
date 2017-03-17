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

#include <arrow/ipc/api.h>

#include <arrow-glib/ipc-metadata-version.h>

GArrowIPCMetadataVersion garrow_ipc_metadata_version_from_raw(arrow::ipc::MetadataVersion::type version);
arrow::ipc::MetadataVersion::type garrow_ipc_metadata_version_to_raw(GArrowIPCMetadataVersion version);
