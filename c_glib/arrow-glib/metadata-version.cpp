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

#include <arrow-glib/metadata-version.hpp>

/**
 * SECTION: metadata-version
 * @title: GArrowMetadataVersion
 * @short_description: Metadata version mapping between Arrow and arrow-glib
 *
 * #GArrowMetadataVersion provides metadata versions corresponding
 * to `arrow::ipc::MetadataVersion` values.
 */

GArrowMetadataVersion
garrow_metadata_version_from_raw(arrow::ipc::MetadataVersion version)
{
  switch (version) {
  case arrow::ipc::MetadataVersion::V1:
    return GARROW_METADATA_VERSION_V1;
  case arrow::ipc::MetadataVersion::V2:
    return GARROW_METADATA_VERSION_V2;
  case arrow::ipc::MetadataVersion::V3:
    return GARROW_METADATA_VERSION_V3;
  default:
    return GARROW_METADATA_VERSION_V3;
  }
}

arrow::ipc::MetadataVersion
garrow_metadata_version_to_raw(GArrowMetadataVersion version)
{
  switch (version) {
  case GARROW_METADATA_VERSION_V1:
    return arrow::ipc::MetadataVersion::V1;
  case GARROW_METADATA_VERSION_V2:
    return arrow::ipc::MetadataVersion::V2;
  case GARROW_METADATA_VERSION_V3:
    return arrow::ipc::MetadataVersion::V3;
  default:
    return arrow::ipc::MetadataVersion::V3;
  }
}
