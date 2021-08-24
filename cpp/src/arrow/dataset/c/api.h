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

// For passing in as file format IDs and used to choose the correct file
// format class
#define DS_PARQUET_FORMAT 0
#define DS_CSV_FORMAT 1
#define DS_IPC_FORMAT 2

// propagate the special value for inspecting all fragments instead of the default
// that only inspects 1 fragment to get the schema
extern const int kInspectAllFragments;
#define DEFAULT_NUM_FRAGMENTS 1
#define DISABLE_INSPECT_FRAGMENTS 0

// Analagous to arrow::dataset::Scanner
struct Scanner {
  // Callback to populate an ArrowArrayStream for streaming the record batches
  // out of this scanner using the C-Data interface Stream API.
  //
  // Returns 0 if succesful or an errno compatible value otherwise. If successful,
  // the ArrowArrayStream must be released independently.
  int (*to_stream)(struct Scanner* scanner, struct ArrowArrayStream* out);
  // Retrieve more details about the error from the previous call on this object.
  //
  // The pointer returned only lives as long as the livetime of this object, but does
  // not need to be freed independantly.
  const char* (*last_error)(struct Scanner*);
  // release callback
  void (*release)(struct Scanner*);
  // Opaque producer-specific data.
  void* private_data;
};

// Analagous to arrow::dataset::Dataset
struct Dataset {
  // Callback to get the schema from the Dataset.
  //
  // Returns 0 if success, errno-compatible value otherwise.
  //
  // If successful the ArrowSchema must be released independently via ArrowSchemaRelease.
  int (*get_schema)(struct Dataset* dataset, struct ArrowSchema* out);
  // Callback to begin a new synchronous scan of the dataset.
  //
  // columns should be an array of c-strings of length n_cols which are the names
  // of the columns to be scanned (columns to Project). Order and duplicates will be
  // preserved. Fails if any column name doesn't exist in the dataset's schema.
  //
  // If batch_size <= 0, the default batch size will be used.
  //
  // Returns 0 if successful or an errno compatible value otherwise. If successful,
  // the scanner must be released independently via ArrowScannerRelease.
  int (*new_scan)(struct Dataset* dataset, const char** columns, const int n_cols,
                  int64_t batch_size, struct Scanner* out);
  // Callback to retrieve the type of the dataset: filesystem, in-memory, union.
  // Currently only filesystem datasets are implemented in this C api, so it should
  // only return the value "filesystem".
  //
  // Returned pointer is valid for the lifetime of this object until ArrowDatasetRelease
  // is called on it.
  const char* (*get_dataset_type_name)(struct Dataset*);
  // Retrieve more details about the error from the previous call on this object.
  //
  // The pointer returned only lives as long as the livetime of this object, but does
  // not need to be freed independantly.
  const char* (*last_error)(struct Dataset*);
  // Release callback
  void (*release)(struct Dataset*);
  // Opaque producer-specific data.
  void* private_data;
};

// Analagous to arrow::dataset:DatasetFactory
struct DatasetFactory {
  // Get the schema for this dataset factory by potentially inspecting fragments.
  // Pass kInspectAllFragments to force it to inspect all of the fragments before
  // returning to ensure that they all have the same schema. Passing
  // DISABLE_INSPECT_FRAGMENTS will disable fragment inspection to derive the schema.
  //
  // The schema must be Released independently using ArrowSchemaRelease from
  // arrow/c/helpers.h Returns 0 on success or an errno compatible value on failure.
  int (*inspect_schema)(struct DatasetFactory* factory,
                        const int num_fragments_to_inspect, struct ArrowSchema* out);
  // Create a dataset from this factory. The Dataset needs to be released separately via
  // ArrowDatasetRelease.
  //
  // Returns 0 on success or an errno compatible value on failure.
  int (*create_dataset)(struct DatasetFactory* factory, struct Dataset* out);
  // Retrieve more details about the error from the previous call on this object.
  //
  // The pointer returned only lives as long as the livetime of this object, but does
  // not need to be freed independantly.
  const char* (*last_error)(struct DatasetFactory* factory);
  // Release callback
  void (*release)(struct DatasetFactory*);
  // Opaque producer-specific data.
  void* private_data;
};

/// Create a FileSystem dataset Factory using a uri
///
/// For the file_format_id consumers should pass in one of the defined macros
/// that are DS_#####_FORMAT.
///
/// Returns 0 if successful or an errno-compatible value on failure.
///
/// To clean up the corresponding memory and objects, call ArrowDatasetFactoryRelease on
/// the populated DatasetFactory object.
int dataset_factory_from_path(const char* uri, const int file_format_id,
                              struct DatasetFactory* out);

#ifdef __cplusplus
}
#endif
