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

/// \file abi.h Arrow C Data Interface
///
/// The Arrow C Data interface defines a very small, stable set
/// of C definitions which can be easily copied into any project's
/// source code and vendored to be used for columnar data interchange
/// in the Arrow format. For non-C/C++ languages and runtimes,
/// it should be almost as easy to translate the C definitions into
/// the corresponding C FFI declarations.
///
/// Applications and libraries can therefore work with Arrow memory
/// without necessarily using the Arrow libraries or reinventing
/// the wheel. Developers can choose between tight integration
/// with the Arrow software project or minimal integration with
/// the Arrow format only.

#pragma once

#include <stdint.h>

// Spec and documentation: https://arrow.apache.org/docs/format/CDataInterface.html

#ifdef __cplusplus
extern "C" {
#endif

#ifndef ARROW_C_DATA_INTERFACE
#  define ARROW_C_DATA_INTERFACE

#  define ARROW_FLAG_DICTIONARY_ORDERED 1
#  define ARROW_FLAG_NULLABLE 2
#  define ARROW_FLAG_MAP_KEYS_SORTED 4

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
#  define ARROW_C_DEVICE_DATA_INTERFACE

// Spec and Documentation: https://arrow.apache.org/docs/format/CDeviceDataInterface.html

// DeviceType for the allocated memory
typedef int32_t ArrowDeviceType;

// CPU device, same as using ArrowArray directly
#  define ARROW_DEVICE_CPU 1
// CUDA GPU Device
#  define ARROW_DEVICE_CUDA 2
// Pinned CUDA CPU memory by cudaMallocHost
#  define ARROW_DEVICE_CUDA_HOST 3
// OpenCL Device
#  define ARROW_DEVICE_OPENCL 4
// Vulkan buffer for next-gen graphics
#  define ARROW_DEVICE_VULKAN 7
// Metal for Apple GPU
#  define ARROW_DEVICE_METAL 8
// Verilog simulator buffer
#  define ARROW_DEVICE_VPI 9
// ROCm GPUs for AMD GPUs
#  define ARROW_DEVICE_ROCM 10
// Pinned ROCm CPU memory allocated by hipMallocHost
#  define ARROW_DEVICE_ROCM_HOST 11
// Reserved for extension
#  define ARROW_DEVICE_EXT_DEV 12
// CUDA managed/unified memory allocated by cudaMallocManaged
#  define ARROW_DEVICE_CUDA_MANAGED 13
// unified shared memory allocated on a oneAPI non-partitioned device.
#  define ARROW_DEVICE_ONEAPI 14
// GPU support for next-gen WebGPU standard
#  define ARROW_DEVICE_WEBGPU 15
// Qualcomm Hexagon DSP
#  define ARROW_DEVICE_HEXAGON 16

struct ArrowDeviceArray {
  // the Allocated Array
  //
  // the buffers in the array (along with the buffers of any
  // children) are what is allocated on the device.
  struct ArrowArray array;
  // The device id to identify a specific device
  int64_t device_id;
  // The type of device which can access this memory.
  ArrowDeviceType device_type;
  // An event-like object to synchronize on if needed.
  void* sync_event;
  // Reserved bytes for future expansion.
  int64_t reserved[3];
};

#endif  // ARROW_C_DEVICE_DATA_INTERFACE

#ifndef ARROW_C_STREAM_INTERFACE
#  define ARROW_C_STREAM_INTERFACE

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
#  define ARROW_C_DEVICE_STREAM_INTERFACE

// Equivalent to ArrowArrayStream, but for ArrowDeviceArrays.
//
// This stream is intended to provide a stream of data on a single
// device, if a producer wants data to be produced on multiple devices
// then multiple streams should be provided. One per device.
struct ArrowDeviceArrayStream {
  // The device that this stream produces data on.
  ArrowDeviceType device_type;

  // Callback to get the stream schema
  // (will be the same for all arrays in the stream).
  //
  // Return value 0 if successful, an `errno`-compatible error code otherwise.
  //
  // If successful, the ArrowSchema must be released independently from the stream.
  // The schema should be accessible via CPU memory.
  int (*get_schema)(struct ArrowDeviceArrayStream* self, struct ArrowSchema* out);

  // Callback to get the next array
  // (if no error and the array is released, the stream has ended)
  //
  // Return value: 0 if successful, an `errno`-compatible error code otherwise.
  //
  // If successful, the ArrowDeviceArray must be released independently from the stream.
  int (*get_next)(struct ArrowDeviceArrayStream* self, struct ArrowDeviceArray* out);

  // Callback to get optional detailed error information.
  // This must only be called if the last stream operation failed
  // with a non-0 return code.
  //
  // Return value: pointer to a null-terminated character array describing
  // the last error, or NULL if no description is available.
  //
  // The returned pointer is only valid until the next operation on this stream
  // (including release).
  const char* (*get_last_error)(struct ArrowDeviceArrayStream* self);

  // Release callback: release the stream's own resources.
  // Note that arrays returned by `get_next` must be individually released.
  void (*release)(struct ArrowDeviceArrayStream* self);

  // Opaque producer-specific data
  void* private_data;
};

#endif  // ARROW_C_DEVICE_STREAM_INTERFACE

#ifndef ARROW_C_ASYNC_STREAM_INTERFACE
#define ARROW_C_ASYNC_STREAM_INTERFACE

// Similar to ArrowDeviceArrayStream, except designed for an asynchronous
// style of interaction. While ArrowDeviceArrayStream provides producer
// defined callbacks, this is intended to be created by the consumer instead.
// The consumer passes this handler to the producer, which in turn uses the
// callbacks to inform the consumer of events in the stream.
struct ArrowAsyncDeviceStreamHandler {
  // Handler for receiving a schema. The passed in stream_schema should be
  // released or moved by the handler (producer is giving ownership of it to
  // the handler).
  //
  // The `extension_param` argument can be null or can be used by a producer
  // to pass arbitrary extra information to the consumer (such as total number
  // of rows, context info, or otherwise).
  //
  // Return value: 0 if successful, `errno`-compatible error otherwise
  int (*on_schema)(struct ArrowAsyncDeviceStreamHandler* self,
                   struct ArrowSchema* stream_schema, void* extension_param);

  // Handler for receiving an array/record batch. Always called at least once
  // unless an error is encountered (which would result in calling on_error).
  // An empty/released array is passed to indicate the end of the stream if no
  // errors have been encountered.
  //
  // The `extension_param` argument can be null or can be used by a producer
  // to pass arbitrary extra information to the consumer.
  //
  // Return value: 0 if successful, `errno`-compatible error otherwise.
  int (*on_next)(struct ArrowAsyncDeviceStreamHandler* self,
                 struct ArrowDeviceArray* next, void* extension_param);

  // Handler for encountering an error. The producer should call release after
  // this returns to clean up any resources.
  //
  // If the message or metadata are non-null, they will only last as long as this
  // function call. The consumer would need to perform a copy of the data if it is
  // it is necessary for them live past the lifetime of this call.
  //
  // Error metadata should be encoded as with metadata in ArrowSchema, defined in
  // the spec at
  // https://arrow.apache.org/docs/format/CDataInterface.html#c.ArrowSchema.metadata
  //
  // After this call, producers should follow-up by calling the release callback.
  void (*on_error)(struct ArrowAsyncDeviceStreamHandler* self, int code,
                   const char* message, const char* metadata);

  // Release callback to release any resources for the handler. Should always be
  // called by a producer when it is done utilizing a handler. No callbacks should
  // be called after this is called.
  void (*release)(struct ArrowAsyncDeviceStreamHandler* self);

  // Opaque handler-specific data
  void* private_data;
};

#endif  // ARROW_C_ASYNC_STREAM_INTERFACE

#ifdef __cplusplus
}
#endif
