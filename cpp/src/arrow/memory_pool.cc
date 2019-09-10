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

#include "arrow/memory_pool.h"

#include <algorithm>  // IWYU pragma: keep
#include <cstdlib>    // IWYU pragma: keep
#include <cstring>    // IWYU pragma: keep
#include <iostream>   // IWYU pragma: keep
#include <limits>
#include <memory>

#include "arrow/status.h"
#include "arrow/util/logging.h"  // IWYU pragma: keep

#ifdef ARROW_JEMALLOC
// Needed to support jemalloc 3 and 4
#define JEMALLOC_MANGLE
// Explicitly link to our version of jemalloc
#include "jemalloc_ep/dist/include/jemalloc/jemalloc.h"
#endif

#ifdef ARROW_MIMALLOC
#include <mimalloc.h>
#endif

namespace arrow {

constexpr size_t kAlignment = 64;

namespace {

// A static piece of memory for 0-size allocations, so as to return
// an aligned non-null pointer.
alignas(kAlignment) static uint8_t zero_size_area[1];

// Helper class directing allocations to the standard system allocator.
class SystemAllocator {
 public:
  // Allocate memory according to the alignment requirements for Arrow
  // (as of May 2016 64 bytes)
  static Status AllocateAligned(int64_t size, uint8_t** out) {
    if (size == 0) {
      *out = zero_size_area;
      return Status::OK();
    }
#ifdef _WIN32
    // Special code path for Windows
    *out = reinterpret_cast<uint8_t*>(
        _aligned_malloc(static_cast<size_t>(size), kAlignment));
    if (!*out) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }
#else
    const int result = posix_memalign(reinterpret_cast<void**>(out), kAlignment,
                                      static_cast<size_t>(size));
    if (result == ENOMEM) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }

    if (result == EINVAL) {
      return Status::Invalid("invalid alignment parameter: ", kAlignment);
    }
#endif
    return Status::OK();
  }

  static Status ReallocateAligned(int64_t old_size, int64_t new_size, uint8_t** ptr) {
    uint8_t* previous_ptr = *ptr;
    if (previous_ptr == zero_size_area) {
      DCHECK_EQ(old_size, 0);
      return AllocateAligned(new_size, ptr);
    }
    if (new_size == 0) {
      DeallocateAligned(previous_ptr, old_size);
      *ptr = zero_size_area;
      return Status::OK();
    }
    // Note: We cannot use realloc() here as it doesn't guarantee alignment.

    // Allocate new chunk
    uint8_t* out = nullptr;
    RETURN_NOT_OK(AllocateAligned(new_size, &out));
    DCHECK(out);
    // Copy contents and release old memory chunk
    memcpy(out, *ptr, static_cast<size_t>(std::min(new_size, old_size)));
#ifdef _WIN32
    _aligned_free(*ptr);
#else
    free(*ptr);
#endif  // defined(_WIN32)
    *ptr = out;
    return Status::OK();
  }

  static void DeallocateAligned(uint8_t* ptr, int64_t size) {
    if (ptr == zero_size_area) {
      DCHECK_EQ(size, 0);
    } else {
#ifdef _WIN32
      _aligned_free(ptr);
#else
      free(ptr);
#endif
    }
  }
};

#ifdef ARROW_JEMALLOC

// Helper class directing allocations to the jemalloc allocator.
class JemallocAllocator {
 public:
  static Status AllocateAligned(int64_t size, uint8_t** out) {
    if (size == 0) {
      *out = zero_size_area;
      return Status::OK();
    }
    *out = reinterpret_cast<uint8_t*>(
        mallocx(static_cast<size_t>(size), MALLOCX_ALIGN(kAlignment)));
    if (*out == NULL) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }
    return Status::OK();
  }

  static Status ReallocateAligned(int64_t old_size, int64_t new_size, uint8_t** ptr) {
    uint8_t* previous_ptr = *ptr;
    if (previous_ptr == zero_size_area) {
      DCHECK_EQ(old_size, 0);
      return AllocateAligned(new_size, ptr);
    }
    if (new_size == 0) {
      DeallocateAligned(previous_ptr, old_size);
      *ptr = zero_size_area;
      return Status::OK();
    }
    *ptr = reinterpret_cast<uint8_t*>(
        rallocx(*ptr, static_cast<size_t>(new_size), MALLOCX_ALIGN(kAlignment)));
    if (*ptr == NULL) {
      *ptr = previous_ptr;
      return Status::OutOfMemory("realloc of size ", new_size, " failed");
    }
    return Status::OK();
  }

  static void DeallocateAligned(uint8_t* ptr, int64_t size) {
    if (ptr == zero_size_area) {
      DCHECK_EQ(size, 0);
    } else {
      dallocx(ptr, MALLOCX_ALIGN(kAlignment));
    }
  }
};

#endif  // defined(ARROW_JEMALLOC)

#ifdef ARROW_MIMALLOC

// Helper class directing allocations to the mimalloc allocator.
class MimallocAllocator {
 public:
  static Status AllocateAligned(int64_t size, uint8_t** out) {
    if (size == 0) {
      *out = zero_size_area;
      return Status::OK();
    }
    *out = reinterpret_cast<uint8_t*>(
        mi_malloc_aligned(static_cast<size_t>(size), kAlignment));
    if (*out == NULL) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }
    return Status::OK();
  }

  static Status ReallocateAligned(int64_t old_size, int64_t new_size, uint8_t** ptr) {
    uint8_t* previous_ptr = *ptr;
    if (previous_ptr == zero_size_area) {
      DCHECK_EQ(old_size, 0);
      return AllocateAligned(new_size, ptr);
    }
    if (new_size == 0) {
      DeallocateAligned(previous_ptr, old_size);
      *ptr = zero_size_area;
      return Status::OK();
    }
    *ptr = reinterpret_cast<uint8_t*>(
        mi_realloc_aligned(previous_ptr, static_cast<size_t>(new_size), kAlignment));
    if (*ptr == NULL) {
      *ptr = previous_ptr;
      return Status::OutOfMemory("realloc of size ", new_size, " failed");
    }
    return Status::OK();
  }

  static void DeallocateAligned(uint8_t* ptr, int64_t size) {
    if (ptr == zero_size_area) {
      DCHECK_EQ(size, 0);
    } else {
      mi_free(ptr);
    }
  }
};

#endif  // defined(ARROW_MIMALLOC)

}  // namespace

MemoryPool::MemoryPool() {}

MemoryPool::~MemoryPool() {}

int64_t MemoryPool::max_memory() const { return -1; }

///////////////////////////////////////////////////////////////////////
// MemoryPool implementation that delegates its core duty
// to an Allocator class.

template <typename Allocator>
class BaseMemoryPoolImpl : public MemoryPool {
 public:
  ~BaseMemoryPoolImpl() override {}

  Status Allocate(int64_t size, uint8_t** out) override {
    if (size < 0) {
      return Status::Invalid("negative malloc size");
    }
    if (static_cast<uint64_t>(size) >= std::numeric_limits<size_t>::max()) {
      return Status::CapacityError("malloc size overflows size_t");
    }
    RETURN_NOT_OK(Allocator::AllocateAligned(size, out));

    stats_.UpdateAllocatedBytes(size);
    return Status::OK();
  }

  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override {
    if (new_size < 0) {
      return Status::Invalid("negative realloc size");
    }
    if (static_cast<uint64_t>(new_size) >= std::numeric_limits<size_t>::max()) {
      return Status::CapacityError("realloc overflows size_t");
    }
    RETURN_NOT_OK(Allocator::ReallocateAligned(old_size, new_size, ptr));

    stats_.UpdateAllocatedBytes(new_size - old_size);
    return Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size) override {
    Allocator::DeallocateAligned(buffer, size);

    stats_.UpdateAllocatedBytes(-size);
  }

  int64_t bytes_allocated() const override { return stats_.bytes_allocated(); }

  int64_t max_memory() const override { return stats_.max_memory(); }

 protected:
  internal::MemoryPoolStats stats_;
};

class SystemMemoryPool : public BaseMemoryPoolImpl<SystemAllocator> {
 public:
  std::string backend_name() const override { return "system"; }
};

#ifdef ARROW_JEMALLOC
class JemallocMemoryPool : public BaseMemoryPoolImpl<JemallocAllocator> {
 public:
  std::string backend_name() const override { return "jemalloc"; }
};
#endif

#ifdef ARROW_MIMALLOC
class MimallocMemoryPool : public BaseMemoryPoolImpl<MimallocAllocator> {
 public:
  std::string backend_name() const override { return "mimalloc"; }
};
#endif

#ifdef ARROW_JEMALLOC
using DefaultMemoryPool = JemallocMemoryPool;
#elif defined(ARROW_MIMALLOC)
using DefaultMemoryPool = MimallocMemoryPool;
#else
using DefaultMemoryPool = SystemMemoryPool;
#endif

std::unique_ptr<MemoryPool> MemoryPool::CreateDefault() {
  return std::unique_ptr<MemoryPool>(new DefaultMemoryPool);
}

static SystemMemoryPool system_pool;
#ifdef ARROW_JEMALLOC
static JemallocMemoryPool jemalloc_pool;
#endif
#ifdef ARROW_MIMALLOC
static MimallocMemoryPool mimalloc_pool;
#endif

MemoryPool* system_memory_pool() { return &system_pool; }

Status jemalloc_memory_pool(MemoryPool** out) {
#ifdef ARROW_JEMALLOC
  *out = &jemalloc_pool;
  return Status::OK();
#else
  return Status::NotImplemented("This Arrow build does not enable jemalloc");
#endif
}

Status mimalloc_memory_pool(MemoryPool** out) {
#ifdef ARROW_MIMALLOC
  *out = &mimalloc_pool;
  return Status::OK();
#else
  return Status::NotImplemented("This Arrow build does not enable mimalloc");
#endif
}

MemoryPool* default_memory_pool() {
#ifdef ARROW_JEMALLOC
  return &jemalloc_pool;
#elif defined(ARROW_MIMALLOC)
  return &mimalloc_pool;
#else
  return &system_pool;
#endif
}

///////////////////////////////////////////////////////////////////////
// LoggingMemoryPool implementation

LoggingMemoryPool::LoggingMemoryPool(MemoryPool* pool) : pool_(pool) {}

Status LoggingMemoryPool::Allocate(int64_t size, uint8_t** out) {
  Status s = pool_->Allocate(size, out);
  std::cout << "Allocate: size = " << size << std::endl;
  return s;
}

Status LoggingMemoryPool::Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
  Status s = pool_->Reallocate(old_size, new_size, ptr);
  std::cout << "Reallocate: old_size = " << old_size << " - new_size = " << new_size
            << std::endl;
  return s;
}

void LoggingMemoryPool::Free(uint8_t* buffer, int64_t size) {
  pool_->Free(buffer, size);
  std::cout << "Free: size = " << size << std::endl;
}

int64_t LoggingMemoryPool::bytes_allocated() const {
  int64_t nb_bytes = pool_->bytes_allocated();
  std::cout << "bytes_allocated: " << nb_bytes << std::endl;
  return nb_bytes;
}

int64_t LoggingMemoryPool::max_memory() const {
  int64_t mem = pool_->max_memory();
  std::cout << "max_memory: " << mem << std::endl;
  return mem;
}

std::string LoggingMemoryPool::backend_name() const { return pool_->backend_name(); }

///////////////////////////////////////////////////////////////////////
// ProxyMemoryPool implementation

class ProxyMemoryPool::ProxyMemoryPoolImpl {
 public:
  explicit ProxyMemoryPoolImpl(MemoryPool* pool) : pool_(pool) {}

  Status Allocate(int64_t size, uint8_t** out) {
    RETURN_NOT_OK(pool_->Allocate(size, out));
    stats_.UpdateAllocatedBytes(size);
    return Status::OK();
  }

  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
    RETURN_NOT_OK(pool_->Reallocate(old_size, new_size, ptr));
    stats_.UpdateAllocatedBytes(new_size - old_size);
    return Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size) {
    pool_->Free(buffer, size);
    stats_.UpdateAllocatedBytes(-size);
  }

  int64_t bytes_allocated() const { return stats_.bytes_allocated(); }

  int64_t max_memory() const { return stats_.max_memory(); }

  std::string backend_name() const { return pool_->backend_name(); }

 private:
  MemoryPool* pool_;
  internal::MemoryPoolStats stats_;
};

ProxyMemoryPool::ProxyMemoryPool(MemoryPool* pool) {
  impl_.reset(new ProxyMemoryPoolImpl(pool));
}

ProxyMemoryPool::~ProxyMemoryPool() {}

Status ProxyMemoryPool::Allocate(int64_t size, uint8_t** out) {
  return impl_->Allocate(size, out);
}

Status ProxyMemoryPool::Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
  return impl_->Reallocate(old_size, new_size, ptr);
}

void ProxyMemoryPool::Free(uint8_t* buffer, int64_t size) {
  return impl_->Free(buffer, size);
}

int64_t ProxyMemoryPool::bytes_allocated() const { return impl_->bytes_allocated(); }

int64_t ProxyMemoryPool::max_memory() const { return impl_->max_memory(); }

std::string ProxyMemoryPool::backend_name() const { return impl_->backend_name(); }

}  // namespace arrow
