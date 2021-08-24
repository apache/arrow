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

#include <stdint.h>
#include "arrow/c/abi.h"

#ifdef __cplusplus
extern "C" {
#endif

#define DS_PARQUET_FORMAT 0
#define DS_CSV_FORMAT     1
#define DS_IPC_FORMAT     2

extern const int kInspectAllFragments;
#define DEFAULT_NUM_FRAGMENTS 1
#define DISABLE_INSPECT_FRAGMENTS 0

typedef uintptr_t Dataset;
typedef uintptr_t DatasetFactory;
typedef uintptr_t Scanner;

DatasetFactory factory_from_path(const char* uri, const int file_format_id);
void release_dataset_factory(DatasetFactory factory);
struct ArrowSchema inspect_schema(DatasetFactory factory, const int num_fragments);

Dataset create_dataset(DatasetFactory factory);
void close_dataset(Dataset dataset_id);
struct ArrowSchema get_dataset_schema(Dataset dataset_id);

const char* dataset_type_name(Dataset dataset_id);
#ifdef __cplusplus
}
#endif
