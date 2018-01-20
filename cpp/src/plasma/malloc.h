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

#ifndef PLASMA_MALLOC_H
#define PLASMA_MALLOC_H

#include <inttypes.h>
#include <stddef.h>

void get_malloc_mapinfo(void* addr, int* fd, int64_t* map_length, ptrdiff_t* offset);

/// Get the mmap size corresponding to a specific file descriptor.
///
/// @param fd The file descriptor to look up.
/// @return The size of the corresponding memory-mapped file.
int64_t get_mmap_size(int fd);

void set_malloc_granularity(int value);

#endif  // MALLOC_H
