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

#include <cstdint>
#include <memory>
#include <string>

#include <cuda.h>

#include "arrow/device.h"
#include "arrow/result.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace cuda {

// Forward declaration
class CudaContext;
class CudaDevice;
class CudaDeviceManager;
class CudaBuffer;
class CudaHostBuffer;
class CudaIpcMemHandle;
class CudaMemoryManager;

// XXX Should CudaContext be merged into CudaMemoryManager?

class ARROW_EXPORT CudaDeviceManager {
 public:
  static Result<CudaDeviceManager*> Instance();

  /// \brief Get a CudaDevice instance for a particular device
  /// \param[in] device_number the CUDA device number
  Result<std::shared_ptr<CudaDevice>> GetDevice(int device_number);

  /// \brief Get the CUDA driver context for a particular device
  /// \param[in] device_number the CUDA device number
  /// \return cached context
  Result<std::shared_ptr<CudaContext>> GetContext(int device_number);

  /// \brief Get the shared CUDA driver context for a particular device
  /// \param[in] device_number the CUDA device number
  /// \param[in] handle CUDA context handle created by another library
  /// \return shared context
  Result<std::shared_ptr<CudaContext>> GetSharedContext(int device_number, void* handle);

  /// \brief Allocate host memory with fast access to given GPU device
  /// \param[in] device_number the CUDA device number
  /// \param[in] nbytes number of bytes
  /// \return Host buffer or Status
  Result<std::shared_ptr<CudaHostBuffer>> AllocateHost(int device_number, int64_t nbytes);

  /// \brief Free host memory
  ///
  /// The given memory pointer must have been allocated with AllocateHost.
  Status FreeHost(void* data, int64_t nbytes);

  int num_devices() const;

 private:
  CudaDeviceManager();
  static std::unique_ptr<CudaDeviceManager> instance_;

  class Impl;
  std::shared_ptr<Impl> impl_;

  friend class CudaContext;
  friend class CudaDevice;
};

/// \brief Device implementation for CUDA
///
/// Each CudaDevice instance is tied to a particular CUDA device
/// (identified by its logical device number).
class ARROW_EXPORT CudaDevice : public Device {
 public:
  const char* type_name() const override;
  std::string ToString() const override;
  bool Equals(const Device&) const override;
  std::shared_ptr<MemoryManager> default_memory_manager() override;
  DeviceAllocationType device_type() const override {
    return DeviceAllocationType::kCUDA;
  }
  int64_t device_id() const override { return device_number(); }

  /// \brief Return a CudaDevice instance for a particular device
  /// \param[in] device_number the CUDA device number
  static Result<std::shared_ptr<CudaDevice>> Make(int device_number);

  /// \brief Return the device logical number
  int device_number() const;

  /// \brief Return the GPU model name
  std::string device_name() const;

  /// \brief Return total memory on this device
  int64_t total_memory() const;

  /// \brief Return a raw CUDA device handle
  ///
  /// The returned value can be used to expose this device to other libraries.
  /// It should be interpreted as `CUdevice`.
  int handle() const;

  /// \brief Get a CUDA driver context for this device
  ///
  /// The returned context is associated with the primary CUDA context for the
  /// device.  This is the recommended way of getting a context for a device,
  /// as it allows interoperating transparently with any library using the
  /// primary CUDA context API.
  Result<std::shared_ptr<CudaContext>> GetContext();

  /// \brief Get a CUDA driver context for this device, using an existing handle
  ///
  /// The handle is not owned: it will not be released when the CudaContext
  /// is destroyed.  This function should only be used if you need interoperation
  /// with a library that uses a non-primary context.
  ///
  /// \param[in] handle CUDA context handle created by another library
  Result<std::shared_ptr<CudaContext>> GetSharedContext(void* handle);

  /// \brief Allocate a host-residing, GPU-accessible buffer
  ///
  /// The buffer is allocated using this device's primary context.
  ///
  /// \param[in] size The buffer size in bytes
  Result<std::shared_ptr<CudaHostBuffer>> AllocateHostBuffer(int64_t size);

  /// \brief EXPERIMENTAL: Wrapper for CUstreams
  ///
  /// Does not *own* the CUstream object which must be separately constructed
  /// and freed using cuStreamCreate and cuStreamDestroy (or equivalent).
  /// Default construction will use the cuda default stream, and does not allow
  /// construction from literal 0 or nullptr.
  class ARROW_EXPORT Stream : public Device::Stream {
   public:
    ~Stream() = default;

    [[nodiscard]] inline CUstream value() const noexcept {
      if (!stream_) {
        return CUstream{};
      }
      return *reinterpret_cast<CUstream*>(stream_.get());
    }
    operator CUstream() const noexcept { return value(); }

    const void* get_raw() const noexcept override { return stream_.get(); }
    Status WaitEvent(const Device::SyncEvent&) override;
    Status Synchronize() const override;

   protected:
    friend class CudaDevice;

    explicit Stream(std::shared_ptr<CudaContext> ctx, CUstream* st,
                    Device::Stream::release_fn_t release_fn)
        : Device::Stream(reinterpret_cast<void*>(st), release_fn),
          context_{std::move(ctx)} {}

    // disable construction from literal 0
    explicit Stream(std::shared_ptr<CudaContext>, int,
                    Device::Stream::release_fn_t) = delete;  // Prevent cast from 0
    explicit Stream(std::shared_ptr<CudaContext>, std::nullptr_t,
                    Device::Stream::release_fn_t) = delete;  // Prevent cast from nullptr

   private:
    std::shared_ptr<CudaContext> context_;
  };

  Result<std::shared_ptr<Device::Stream>> MakeStream() override { return MakeStream(0); }

  /// \brief Create a CUstream wrapper in the current context
  Result<std::shared_ptr<Device::Stream>> MakeStream(unsigned int flags) override;

  /// @brief Wrap a pointer to an existing stream
  ///
  /// @param device_stream passed in stream (should be a CUstream*)
  /// @param release_fn destructor to free the stream. `nullptr` may be passed
  ///        to indicate there is no destruction/freeing necessary.
  Result<std::shared_ptr<Device::Stream>> WrapStream(
      void* device_stream, Stream::release_fn_t release_fn) override;

  class ARROW_EXPORT SyncEvent : public Device::SyncEvent {
   public:
    [[nodiscard]] CUevent value() const {
      if (sync_event_) {
        return *static_cast<CUevent*>(sync_event_.get());
      }
      return CUevent{};
    }
    operator CUevent() const noexcept { return value(); }

    /// @brief Block until the sync event is marked completed
    Status Wait() override;

    /// @brief Record the wrapped event on the stream
    ///
    /// Once the stream completes the tasks previously added to it,
    /// it will trigger the event.
    Status Record(const Device::Stream&) override;

   protected:
    friend class CudaMemoryManager;

    explicit SyncEvent(std::shared_ptr<CudaContext> ctx, CUevent* ev,
                       Device::SyncEvent::release_fn_t release_ev)
        : Device::SyncEvent(reinterpret_cast<void*>(ev), release_ev),
          context_{std::move(ctx)} {}

   private:
    std::shared_ptr<CudaContext> context_;
  };

 protected:
  struct Impl;

  friend class CudaContext;
  /// \cond FALSE
  // (note: emits warning on Doxygen < 1.8.15)
  friend class CudaDeviceManager::Impl;
  /// \endcond

  explicit CudaDevice(Impl);
  std::shared_ptr<Impl> impl_;
};

/// \brief Return whether a device instance is a CudaDevice
ARROW_EXPORT
bool IsCudaDevice(const Device& device);

/// \brief Cast a device instance to a CudaDevice
///
/// An error is returned if the device is not a CudaDevice.
ARROW_EXPORT
Result<std::shared_ptr<CudaDevice>> AsCudaDevice(const std::shared_ptr<Device>& device);

/// \brief MemoryManager implementation for CUDA
class ARROW_EXPORT CudaMemoryManager : public MemoryManager {
 public:
  Result<std::shared_ptr<io::RandomAccessFile>> GetBufferReader(
      std::shared_ptr<Buffer> buf) override;
  Result<std::shared_ptr<io::OutputStream>> GetBufferWriter(
      std::shared_ptr<Buffer> buf) override;

  Result<std::unique_ptr<Buffer>> AllocateBuffer(int64_t size) override;

  /// \brief The CudaDevice instance tied to this MemoryManager
  ///
  /// This is a useful shorthand returning a concrete-typed pointer, avoiding
  /// having to cast the `device()` result.
  std::shared_ptr<CudaDevice> cuda_device() const;

  /// \brief Creates a wrapped CUevent.
  ///
  /// Will call cuEventCreate and it will call cuEventDestroy internally
  /// when the event is destructed.
  Result<std::shared_ptr<Device::SyncEvent>> MakeDeviceSyncEvent() override;

  /// \brief Wraps an existing event into a sync event.
  ///
  /// @param sync_event the event to wrap, must be a CUevent*
  /// @param release_sync_event a function to call during destruction, `nullptr` or
  ///        a no-op function can be passed to indicate ownership is maintained externally
  Result<std::shared_ptr<Device::SyncEvent>> WrapDeviceSyncEvent(
      void* sync_event, Device::SyncEvent::release_fn_t release_sync_event) override;

 protected:
  using MemoryManager::MemoryManager;
  static std::shared_ptr<CudaMemoryManager> Make(const std::shared_ptr<Device>& device);

  Result<std::shared_ptr<Buffer>> CopyBufferFrom(
      const std::shared_ptr<Buffer>& buf,
      const std::shared_ptr<MemoryManager>& from) override;
  Result<std::shared_ptr<Buffer>> CopyBufferTo(
      const std::shared_ptr<Buffer>& buf,
      const std::shared_ptr<MemoryManager>& to) override;
  Result<std::unique_ptr<Buffer>> CopyNonOwnedFrom(
      const Buffer& buf, const std::shared_ptr<MemoryManager>& from) override;
  Result<std::unique_ptr<Buffer>> CopyNonOwnedTo(
      const Buffer& buf, const std::shared_ptr<MemoryManager>& to) override;
  Result<std::shared_ptr<Buffer>> ViewBufferFrom(
      const std::shared_ptr<Buffer>& buf,
      const std::shared_ptr<MemoryManager>& from) override;
  Result<std::shared_ptr<Buffer>> ViewBufferTo(
      const std::shared_ptr<Buffer>& buf,
      const std::shared_ptr<MemoryManager>& to) override;

  friend class CudaDevice;
};

/// \brief Return whether a MemoryManager instance is a CudaMemoryManager
ARROW_EXPORT
bool IsCudaMemoryManager(const MemoryManager& mm);

/// \brief Cast a MemoryManager instance to a CudaMemoryManager
///
/// An error is returned if the MemoryManager is not a CudaMemoryManager.
ARROW_EXPORT
Result<std::shared_ptr<CudaMemoryManager>> AsCudaMemoryManager(
    const std::shared_ptr<MemoryManager>& mm);

/// \class CudaContext
/// \brief Object-oriented interface to the low-level CUDA driver API
class ARROW_EXPORT CudaContext : public std::enable_shared_from_this<CudaContext> {
 public:
  ~CudaContext();

  Status Close();

  /// \brief Allocate CUDA memory on GPU device for this context
  /// \param[in] nbytes number of bytes
  /// \return the allocated buffer
  Result<std::unique_ptr<CudaBuffer>> Allocate(int64_t nbytes);

  /// \brief Release CUDA memory on GPU device for this context
  /// \param[in] device_ptr the buffer address
  /// \param[in] nbytes number of bytes
  /// \return Status
  Status Free(void* device_ptr, int64_t nbytes);

  /// \brief Create a view of CUDA memory on GPU device of this context
  /// \param[in] data the starting device address
  /// \param[in] nbytes number of bytes
  /// \return the view buffer
  ///
  /// \note The caller is responsible for allocating and freeing the
  /// memory as well as ensuring that the memory belongs to the CUDA
  /// context that this CudaContext instance holds.
  Result<std::shared_ptr<CudaBuffer>> View(uint8_t* data, int64_t nbytes);

  /// \brief Open existing CUDA IPC memory handle
  /// \param[in] ipc_handle opaque pointer to CUipcMemHandle (driver API)
  /// \return a CudaBuffer referencing the IPC segment
  Result<std::shared_ptr<CudaBuffer>> OpenIpcBuffer(const CudaIpcMemHandle& ipc_handle);

  /// \brief Close memory mapped with IPC buffer
  /// \param[in] buffer a CudaBuffer referencing
  /// \return Status
  Status CloseIpcBuffer(CudaBuffer* buffer);

  /// \brief Block until the all device tasks are completed.
  Status Synchronize(void);

  int64_t bytes_allocated() const;

  /// \brief Expose CUDA context handle to other libraries
  void* handle() const;

  /// \brief Return the default memory manager tied to this context's device
  std::shared_ptr<CudaMemoryManager> memory_manager() const;

  /// \brief Return the device instance associated with this context
  std::shared_ptr<CudaDevice> device() const;

  /// \brief Return the logical device number
  int device_number() const;

  /// \brief Return the device address that is reachable from kernels
  /// running in the context
  /// \param[in] addr device or host memory address
  /// \return the device address
  ///
  /// The device address is defined as a memory address accessible by
  /// device. While it is often a device memory address, it can be
  /// also a host memory address, for instance, when the memory is
  /// allocated as host memory (using cudaMallocHost or cudaHostAlloc)
  /// or as managed memory (using cudaMallocManaged) or the host
  /// memory is page-locked (using cudaHostRegister).
  Result<uintptr_t> GetDeviceAddress(uint8_t* addr);
  Result<uintptr_t> GetDeviceAddress(uintptr_t addr);

 private:
  CudaContext();

  Result<std::shared_ptr<CudaIpcMemHandle>> ExportIpcBuffer(const void* data,
                                                            int64_t size);
  Status CopyHostToDevice(void* dst, const void* src, int64_t nbytes);
  Status CopyHostToDevice(uintptr_t dst, const void* src, int64_t nbytes);
  Status CopyDeviceToHost(void* dst, const void* src, int64_t nbytes);
  Status CopyDeviceToHost(void* dst, uintptr_t src, int64_t nbytes);
  Status CopyDeviceToDevice(void* dst, const void* src, int64_t nbytes);
  Status CopyDeviceToDevice(uintptr_t dst, uintptr_t src, int64_t nbytes);
  Status CopyDeviceToAnotherDevice(const std::shared_ptr<CudaContext>& dst_ctx, void* dst,
                                   const void* src, int64_t nbytes);
  Status CopyDeviceToAnotherDevice(const std::shared_ptr<CudaContext>& dst_ctx,
                                   uintptr_t dst, uintptr_t src, int64_t nbytes);

  class Impl;
  std::shared_ptr<Impl> impl_;

  friend class CudaBuffer;
  friend class CudaBufferReader;
  friend class CudaBufferWriter;
  friend class CudaDevice;
  friend class CudaMemoryManager;
  /// \cond FALSE
  // (note: emits warning on Doxygen < 1.8.15)
  friend class CudaDeviceManager::Impl;
  /// \endcond
};

}  // namespace cuda
}  // namespace arrow
