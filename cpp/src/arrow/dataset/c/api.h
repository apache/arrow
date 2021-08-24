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
#define DS_CSV_FORMAT 1
#define DS_IPC_FORMAT 2

extern const int kInspectAllFragments;
#define DEFAULT_NUM_FRAGMENTS 1
#define DISABLE_INSPECT_FRAGMENTS 0

struct Scanner {
  int (*to_stream)(struct Scanner* scanner, struct ArrowArrayStream* out);
  const char* (*last_error)(struct Scanner*);
  void (*release)(struct Scanner*);
  void* private_data;
};

struct Dataset {
  int (*get_schema)(struct Dataset* dataset, struct ArrowSchema* out);
  int (*new_scan)(struct Dataset* dataset, const char** columns, const int n_cols,
                  uint64_t batch_size, struct Scanner* out);
  const char* (*get_dataset_type_name)(struct Dataset*);
  const char* (*last_error)(struct Dataset*);
  void (*release)(struct Dataset*);
  void* private_data;
};

struct DatasetFactory {
  int (*inspect_schema)(struct DatasetFactory* factory,
                        const int num_fragments_to_inspect, struct ArrowSchema* out);
  int (*create_dataset)(struct DatasetFactory* factory, struct Dataset* out);
  const char* (*last_error)(struct DatasetFactory* factory);
  void (*release)(struct DatasetFactory*);
  void* private_data;
};

int dataset_factory_from_path(const char* uri, const int file_format_id,
                              struct DatasetFactory* out);

#ifdef __cplusplus
}
#endif
