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
#ifdef _WIN32
#include "arrow/io/mman.h"
#undef Realloc
#undef Free
#else
#include <sys/mman.h>
#include <unistd.h>
#endif

namespace arrow {
namespace internal {
// define a wrapper around mremap which the same arguments on linux and on windows
// should only be called with writable files
static void* MemoryMapRemap(void* addr, size_t old_size, size_t new_size, int fildes) {
#ifdef _WIN32
  // flags are ignored on windows
  void* new_addr = MAP_FAILED;
  HANDLE fm, h;

  if (!UnmapViewOfFile(addr)) {
    errno = __map_mman_error(GetLastError(), EPERM);
    return MAP_FAILED;
  }

  h = (HANDLE)_get_osfhandle(fildes);
  if (h == INVALID_HANDLE_VALUE) {
    errno = __map_mman_error(GetLastError(), EPERM);
    return MAP_FAILED;
  }

  LONG new_size_low = static_cast<LONG>(new_size & 0xFFFFFFFFL);
  LONG new_size_high = static_cast<LONG>((new_size >> 32) & 0xFFFFFFFFL);

  SetFilePointer(h, new_size_low, &new_size_high, FILE_BEGIN);
  SetEndOfFile(h);
  fm = CreateFileMapping(h, NULL, PAGE_READWRITE, 0, 0, "");
  if (fm == NULL) {
    errno = __map_mman_error(GetLastError(), EPERM);
    return MAP_FAILED;
  }
  new_addr = MapViewOfFile(fm, FILE_MAP_WRITE, 0, 0, new_size);
  CloseHandle(fm);
  if (new_addr == NULL) {
    errno = __map_mman_error(GetLastError(), EPERM);
    return MAP_FAILED;
  }
  return new_addr;
#else
#ifdef __APPLE__
  // we have to close the mmap first, truncate the file to the new size
  // and recreate the mmap
  if (munmap(addr, old_size) == -1) {
    return MAP_FAILED;
  }
  if (ftruncate(fildes, new_size) == -1) {
    return MAP_FAILED;
  }
  // we set READ / WRITE flags on the new map, since we could only have
  // unlarged a RW map in the first place
  return mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, fildes, 0);
#else
  if (ftruncate(fildes, new_size) == -1) {
    return MAP_FAILED;
  }
  return mremap(addr, old_size, new_size, MREMAP_MAYMOVE);
#endif
#endif
}
}  // namespace internal
}  // namespace arrow
