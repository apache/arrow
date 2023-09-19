// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "arrow/c/abi.h"
#include "arrow/util/visibility.h"

// This file only serves as documentation for the C Data Interface integration
// entrypoints. The actual functions are called by Archery through DLL symbol lookup.

extern "C" {

ARROW_EXPORT
const char* ArrowCpp_CDataIntegration_ExportSchemaFromJson(const char* json_path,
                                                           ArrowSchema* out);

ARROW_EXPORT
const char* ArrowCpp_CDataIntegration_ImportSchemaAndCompareToJson(const char* json_path,
                                                                   ArrowSchema* schema);

ARROW_EXPORT
const char* ArrowCpp_CDataIntegration_ExportBatchFromJson(const char* json_path,
                                                          int num_batch, ArrowArray* out);

ARROW_EXPORT
const char* ArrowCpp_CDataIntegration_ImportBatchAndCompareToJson(const char* json_path,
                                                                  int num_batch,
                                                                  ArrowArray* batch);

ARROW_EXPORT
int64_t ArrowCpp_BytesAllocated();

}  // extern "C"
