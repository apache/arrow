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

#ifdef __cplusplus
extern "C" {
#endif

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
  // Array type description
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;

  // Release callback
  void (*release)(struct ArrowSchema*);
  // Opaque producer-specific data
  void* private_data;
};

struct ArrowArray {
  // Array data description
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;

  // Release callback
  void (*release)(struct ArrowArray*);
  // Opaque producer-specific data
  void* private_data;
};

#endif  // ARROW_C_DATA_INTERFACE

#ifndef ARROW_C_DEVICE_DATA_INTERFACE
#define ARROW_C_DEVICE_DATA_INTERFACE

// ArrowDeviceType is compatible with dlpack DLDeviceType for portability
// it uses the same values for each enum as the equivalent kDL<type> from dlpack.h
#ifdef __cplusplus
typedef enum : int32_t {
#else
typedef enum {
#endif
  // CPU device, same as using ArrowArray directly
  kArrowCPU = 1,
  // CUDA GPU Device
  kArrowCUDA = 2,
  // Pinned CUDA CPU memory by cudaMallocHost
  kArrowCUDAHost = 3,
  // OpenCL Device
  kArrowOpenCL = 4,
  // Vulkan buffer for next-gen graphics
  kArrowVulkan = 7,
  // Metal for Apple GPU
  kArrowMetal = 8,
  // Verilog simulator buffer
  kArrowVPI = 9,
  // ROCm GPUs for AMD GPUs
  kArrowROCM = 10,
  // Pinned ROCm CPU memory allocated by hipMallocHost
  kArrowROCMHost = 11,
  // Reserved for extension
  // used to quickly test extension devices,
  // semantics can differ based on the implementation
  kArrowExtDev = 12,
  // CUDA managed/unified memory allocated by cudaMallocManaged
  kArrowCUDAManaged = 13,
  // unified shared memory allocated on a oneAPI non-partitioned
  // device. call to oneAPI runtime is required to determine the
  // device type, the USM allocation type and the sycl context it
  // is bound to
  kArrowOneAPI = 14,
  // GPU support for next-gen WebGPU standard
  kArrowWebGPU = 15,
  // Qualcomm Hexagon DSP
  kArrowHexagon = 16,
} ArrowDeviceType;

struct ArrowDeviceArray {
  // the private_date and release callback of the arrow array
  // should contain any necessary information and structures
  // related to freeing the array according to the device it
  // is allocated on, rather than having a separate release
  // callback embedded here.
  struct ArrowArray* array;
  int device_id;
  ArrowDeviceType device_type;
  // reserve 128 bytes for future expansion
  // of this struct as non-CPU development expands
  // so that we can update without ABI breaking
  // changes.
  int64_t reserved[2];
};

#endif  // ARROW_C_DEVICE_DATA_INTERFACE

#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE

struct ArrowArrayStream {
  // Callback to get the stream type
  // (will be the same for all arrays in the stream).
  //
  // Return value: 0 if successful, an `errno`-compatible error code otherwise.
  //
  // If successful, the ArrowSchema must be released independently from the stream.
  int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);

  // Callback to get the next array
  // (if no error and the array is released, the stream has ended)
  //
  // Return value: 0 if successful, an `errno`-compatible error code otherwise.
  //
  // If successful, the ArrowArray must be released independently from the stream.
  int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);

  // Callback to get optional detailed error information.
  // This must only be called if the last stream operation failed
  // with a non-0 return code.
  //
  // Return value: pointer to a null-terminated character array describing
  // the last error, or NULL if no description is available.
  //
  // The returned pointer is only valid until the next operation on this stream
  // (including release).
  const char* (*get_last_error)(struct ArrowArrayStream*);

  // Release callback: release the stream's own resources.
  // Note that arrays returned by `get_next` must be individually released.
  void (*release)(struct ArrowArrayStream*);

  // Opaque producer-specific data
  void* private_data;
};

#endif  // ARROW_C_STREAM_INTERFACE

#ifndef ARROW_C_DEVICE_STREAM_INTERFACE
#define ARROW_C_DEVICE_STREAM_INTERFACE

struct ArrowDeviceArrayStream {
  // The device that this stream produces data on.
  // All ArrowDeviceArrays that are produced by this
  // stream should have the same device_type as set
  // here. The device_type needs to be provided here
  // so that consumers can provide the correct type
  // of stream_ptr when calling get_next.
  ArrowDeviceType device_type;

  // Callback to get the stream schema
  // (will be the same for all arrays in the stream).
  //
  // Return value: 0 if successful, an `errno`-compatible error code otherwise.
  //
  // If successful, the ArrowSchema must be released independently from the stream.
  int (*get_schema)(struct ArrowDeviceArrayStream*, struct ArrowSchema* out);

  // Callback to get the device id for the next array.
  // This is necessary so that the proper/correct stream pointer can be provided
  // to get_next. The parameter provided must not be null.
  //
  // Return value: 0 if successful, an `errno`-compatible error code otherwise.
  //
  // The next call to `get_next` should provide an ArrowDeviceArray whose
  // device_id matches what is provided here, and whose device_type is the
  // same as the device_type member of this stream.
  int (*get_next_device_id)(struct ArrowDeviceArrayStream*, int* out_device_id);

  // Callback to get the next array
  // (if no error and the array is released, the stream has ended)
  //
  // the provided stream_ptr should be the appropriate stream, or
  // equivalent object, for the device that the data is allocated on
  // to indicate where the consumer wants the data to be accessible.
  // if stream_ptr is NULL then the default stream (e.g. CUDA stream 0)
  // should be used to ensure that the memory is accessible from any stream.
  //
  // because different frameworks use different types to represent this, we
  // accept a void* which should then be reinterpreted into whatever the
  // appropriate type is (e.g. cudaStream_t) for use by the producer.
  //
  // Return value: 0 if successful, an `errno`-compatible error code otherwise.
  //
  // If successful, the ArrowArray must be released independently from the stream.
  int (*get_next)(struct ArrowDeviceArrayStream*, const void* stream_ptr,
                  struct ArrowDeviceArray* out);

  // Callback to get optional detailed error information.
  // This must only be called if the last stream operation failed
  // with a non-0 return code.
  //
  // Return value: pointer to a null-terminated character array describing
  // the last error, or NULL if no description is available.
  //
  // The returned pointer is only valid until the next operation on this stream
  // (including release).
  const char* (*get_last_error)(struct ArrowDeviceArrayStream*);

  // Release callback: release the stream's own resources.
  // Note that arrays returned by `get_next` must be individually released.
  void (*release)(struct ArrowDeviceArrayStream*);

  // Opaque producer-specific data
  void* private_data;
};

#endif  // ARROW_C_DEVICE_STREAM_INTERFACE

#ifdef __cplusplus
}
#endif
