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

#ifndef PLASMA_ALLOCATOR_H
#define PLASMA_ALLOCATOR_H

#include <cstddef>
#include <cstdint>

namespace plasma {

class PlasmaAllocator {
 public:
  /// Allocates size bytes and returns a pointer to the allocated memory. The
  /// memory address will be a multiple of alignment, which must be a power of two.
  ///
  /// \param alignment Memory alignment.
  /// \param bytes Number of bytes.
  /// \return Pointer to allocated memory.
  static void* Memalign(size_t alignment, size_t bytes);

  /// Frees the memory space pointed to by mem, which must have been returned by
  /// a previous call to Memalign()
  ///
  /// \param mem Pointer to memory to free.
  /// \param bytes Number of bytes to be freed.
  static void Free(void* mem, size_t bytes);

  /// Sets the memory footprint limit for Plasma.
  ///
  /// \param bytes Plasma memory footprint limit in bytes.
  static void SetFootprintLimit(size_t bytes);

  /// Get the memory footprint limit for Plasma.
  ///
  /// \return Plasma memory footprint limit in bytes.
  static int64_t GetFootprintLimit();

  /// Get the number of bytes allocated by Plasma so far.
  /// \return Number of bytes allocated by Plasma so far.
  static int64_t Allocated();

  /// Sets the eviction buffer limit for Plasma.
  ///
  /// \param bytes Plasma eviction buffer limit in bytes.
  static void SetEvictionBufferLimit(size_t bytes);

  /// Get the eviction buffer limit for Plasma.
  ///
  /// \return Plasma eviction buffer limit in bytes.
  static int64_t GetEvictionBufferLimit();

  /// Marks the bytes for eviction.
  ///
  /// \param bytes Number of bytes to mark for eviction.
  /// \return True if eviction can be buffered, false otherwise.
  static bool MarkForEviction(size_t bytes);

  /// Complete the eviction, and free memory space pointed to by mem.
  ///
  /// \param mem Pointer to memory to free.
  /// \param bytes Number of bytes evicted.
  static void CompleteEviction(void* mem, size_t bytes);

  /// Try to abort evicting given number of bytes, moving them from
  /// eviction buffer to allocated.
  ///
  /// @param bytes Number of bytes to abort evicting.
  /// @return True if abortion is possible, false otherwise.
  static bool AbortEviction(size_t bytes);

 private:
  static int64_t allocated_;
  static int64_t eviction_buffered_;
  static int64_t footprint_limit_;
  static int64_t eviction_buffer_limit_;
};

}  // namespace plasma

#endif  // ARROW_PLASMA_ALLOCATOR_H
