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

/// \file abi.h Arrow C-Data Interface
///
/// The Arrow C-Data interface defines a very small, stable set
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

/// \defgroup Arrow C-Data Interface
/// Definitions for the C-Data Interface/C-Stream Interface.
///
/// @{

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

/// \defgroup arrow-device-types Device Types
/// These macros are compatible with the dlpack DLDeviceType values,
/// using the same value for each enum as the equivalent kDL<type>
/// from dlpack.h. This list should continue to be kept in sync with
/// the equivalent dlpack.h enum values over time to ensure
/// compatibility, rather than potentially diverging.
///
/// To ensure predictability with the ABI we use macros instead of
/// an enum so the storage type is not compiler dependent.
///
/// @{

/// \brief DeviceType for the allocated memory
typedef int32_t ArrowDeviceType;

/// \brief CPU device, same as using ArrowArray directly
#define ARROW_DEVICE_CPU = 1
/// \brief CUDA GPU Device
#define ARROW_DEVICE_CUDA = 2
/// \brief Pinned CUDA CPU memory by cudaMallocHost
#define ARROW_DEVICE_CUDA_HOST = 3
/// \brief OpenCL Device
#define ARROW_DEVICE_OPENCL = 4
/// \brief Vulkan buffer for next-gen graphics
#define ARROW_DEVICE_VULKAN = 7
/// \brief Metal for Apple GPU
#define ARROW_DEVICE_METAL = 8
/// \brief Verilog simulator buffer
#define ARROW_DEVICE_VPI = 9
/// \brief ROCm GPUs for AMD GPUs
#define ARROW_DEVICE_ROCM = 10
/// \brief Pinned ROCm CPU memory allocated by hipMallocHost
#define ARROW_DEVICE_ROCM_HOST = 11
/// \brief Reserved for extension
///
/// used to quickly test extension devices, semantics
/// can differ based on the implementation
#define ARROW_DEVICE_EXT_DEV = 12
/// \brief CUDA managed/unified memory allocated by cudaMallocManaged
#define ARROW_DEVICE_CUDA_MANAGED = 13
/// \brief unified shared memory allocated on a oneAPI
/// non-partitioned device.
///
/// A call to the oneAPI runtime is required to determine the device
/// type, the USM allocation type, and the sycl context it is bound to.
#define ARROW_DEVICE_ONEAPI = 14
/// \brief GPU support for next-gen WebGPU standard
#define ARROW_DEVICE_WEBGPU = 15
/// \brief Qualcomm Hexagon DSP
#define ARROW_DEVICE_HEXAGON = 16

/// @}

/// \brief Struct for passing an Arrow Array alongside
/// device memory information.
struct ArrowDeviceArray {
  /// \brief the Allocated Array
  ///
  /// the buffers in the array (along with the buffers of any
  /// children) are what is allocated on the device.
  ///
  /// the private_data and release callback of the arrow array
  /// should contain any necessary information and structures
  /// related to freeing the array according to the device it
  /// is allocated on, rather than having a separate release
  /// callback embedded here.
  struct ArrowArray array;
  /// \brief The device id to identify a specific device
  /// if multiple of this type are on the system.
  ///
  /// the semantics of the id will be hardware dependant.
  int64_t device_id;
  /// \brief The type of device which can access this memory.
  ArrowDeviceType device_type;
  /// \brief An event-like object to synchronize on if needed.
  ///
  /// Many devices, like GPUs, are primarily asynchronous with
  /// respect to CPU processing. As such in order to safely access
  /// memory, it is often necessary to have an object to synchronize
  /// processing on. Since different devices will use different types
  /// to specify this we use a void* that can be coerced into
  /// whatever the device appropriate type is (e.g. cudaEvent_t for
  /// CUDA and hipEvent_t for HIP).
  ///
  /// If synchronization is not needed this can be null. If this is
  /// non-null, then it should be used to call the appropriate sync
  /// method for the device (e.g. cudaStreamWaitEvent / hipStreamWaitEvent).
  ///
  /// Expected type to coerce this void* to depending on device type:
  ///   cuda: cudaEvent_t*
  ///   ROCm: hipEvent_t*
  ///   OpenCL: cl_event*
  ///   Vulkan: VkEvent*
  ///   Metal: MTLEvent*
  ///   OneAPI: syscl::event*
  ///
  void* sync_event;
  /// \brief Reserved bytes for future expansion.
  ///
  /// As non-CPU development expands we can update this struct
  /// without ABI breaking changes. This also rounds out the
  /// total size of this struct to be 128 bytes (power of 2)
  /// on 64-bit systems. These bytes should be zero'd out after
  /// allocation in order to ensure safe evolution of the ABI in
  /// the future.
  int64_t reserved[3];
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

/// \brief Equivalent to ArrowArrayStream, but for ArrowDeviceArrays.
///
/// This stream is intended to provide a stream of data on a single
/// device, if a producer wants data to be produced on multiple devices
/// then multiple streams should be provided. One per device.
struct ArrowDeviceArrayStream {
  /// \brief The device that this stream produces data on.
  ///
  /// All ArrowDeviceArrays that are produced by this
  /// stream should have the same device_type as set
  /// here. The device_type needs to be provided here
  /// so that consumers can provide the correct type
  /// of queue_ptr when calling get_next.
  ArrowDeviceType device_type;

  /// \brief Callback to get the stream schema
  /// (will be the same for all arrays in the stream).
  ///
  /// If successful, the ArrowSchema must be released independantly from the stream.
  /// The schema should be accessible via CPU memory.
  ///
  /// \param[in] self The ArrowDeviceArrayStream object itself
  /// \param[out] out C struct to export the schema to
  /// \return 0 if successful, an `errno`-compatible error code otherwise.
  int (*get_schema)(struct ArrowDeviceArrayStream* self, struct ArrowSchema* out);

  /// \brief Callback to get the next array
  ///
  /// If there is no error and the returned array has been released, the stream
  /// has ended. If successful, the ArrowArray must be released independently
  /// from the stream.
  ///
  /// \param[in] self The ArrowDeviceArrayStream object itself
  /// \param[out] out C struct where to export the Array and device info
  /// \return 0 if successful, an `errno`-compatible error code otherwise.
  int (*get_next)(struct ArrowDeviceArrayStream* self, struct ArrowDeviceArray* out);

  /// \brief Callback to get optional detailed error information.
  ///
  /// This must only be called if the last stream operation failed
  /// with a non-0 return code.
  ///
  /// The returned pointer is only valid until the next operation on this stream
  /// (including release).
  ///
  /// \param[in] self The ArrowDeviceArrayStream object itself
  /// \return pointer to a null-terminated character array describing
  /// the last error, or NULL if no description is available.
  const char* (*get_last_error)(struct ArrowDeviceArrayStream* self);

  /// \brief Release callback: release the stream's own resources.
  ///
  /// Note that arrays returned by `get_next` must be individually released.
  ///
  /// \param[in] self The ArrowDeviceArrayStream object itself
  void (*release)(struct ArrowDeviceArrayStream* self);

  /// \brief Opaque producer-specific data
  void* private_data;
};

#endif  // ARROW_C_DEVICE_STREAM_INTERFACE

#ifdef __cplusplus
}
#endif

/// @}
