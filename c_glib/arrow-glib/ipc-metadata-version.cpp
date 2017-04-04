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

#include <arrow-glib/ipc-metadata-version.hpp>

/**
 * SECTION: ipc-metadata-version
 * @title: GArrowIPCMetadataVersion
 * @short_description: Metadata version mapgging between Arrow and arrow-glib
 *
 * #GArrowIPCMetadataVersion provides metadata versions corresponding
 * to `arrow::ipc::MetadataVersion` values.
 */

GArrowIPCMetadataVersion
garrow_ipc_metadata_version_from_raw(arrow::ipc::MetadataVersion version)
{
  switch (version) {
  case arrow::ipc::MetadataVersion::V1:
    return GARROW_IPC_METADATA_VERSION_V1;
  case arrow::ipc::MetadataVersion::V2:
    return GARROW_IPC_METADATA_VERSION_V2;
  case arrow::ipc::MetadataVersion::V3:
    return GARROW_IPC_METADATA_VERSION_V3;
  default:
    return GARROW_IPC_METADATA_VERSION_V3;
  }
}

arrow::ipc::MetadataVersion
garrow_ipc_metadata_version_to_raw(GArrowIPCMetadataVersion version)
{
  switch (version) {
  case GARROW_IPC_METADATA_VERSION_V1:
    return arrow::ipc::MetadataVersion::V1;
  case GARROW_IPC_METADATA_VERSION_V2:
    return arrow::ipc::MetadataVersion::V2;
  case GARROW_IPC_METADATA_VERSION_V3:
    return arrow::ipc::MetadataVersion::V3;
  default:
    return arrow::ipc::MetadataVersion::V3;
  }
}
