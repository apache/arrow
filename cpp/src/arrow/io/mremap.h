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

// define a wrapper around mremap which the same arguments on linux and on windows
static void* arrow_mremap(void* addr, size_t old_size, size_t new_size, int fildes) {
#ifdef _WIN32
  // flags are ignored on windows
  void* new_addr = MAP_FAILED;
  HANDLE fm, h;
  DWORD dwErrCode = 0;

  /* First, unmap the file view */
  if (!UnmapViewOfFile(addr)) {
    errno = __map_mman_error(GetLastError(), EPERM);
    return MAP_FAILED;
  }

  h = (HANDLE)_get_osfhandle(fildes);

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
  // we don't need the file descriptor on linux
  if (ftruncate(fildes, new_size) == -1) {
    return MAP_FAILED;
  }
  return mremap(addr, old_size, new_size, MREMAP_MAYMOVE);
#endif
}
