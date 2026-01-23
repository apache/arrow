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

// Interposition library for testing arrow_mimalloc_* PLT hooks.
// This library is meant to be used with LD_PRELOAD to intercept
// Arrow's mimalloc allocation functions.

#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>

static atomic_size_t g_alloc_count = 0;
static atomic_size_t g_free_count = 0;
static atomic_size_t g_realloc_count = 0;
static atomic_size_t g_total_allocated = 0;

void* arrow_mimalloc_allocate(size_t size, size_t alignment) {
  static void* (*real_fn)(size_t, size_t) = NULL;
  if (!real_fn) {
    real_fn = dlsym(RTLD_NEXT, "arrow_mimalloc_allocate");
    if (!real_fn) {
      fprintf(stderr, "INTERPOSE ERROR: cannot find arrow_mimalloc_allocate\n");
      abort();
    }
  }
  void* ptr = real_fn(size, alignment);
  if (ptr) {
    atomic_fetch_add(&g_alloc_count, 1);
    atomic_fetch_add(&g_total_allocated, size);
  }
  return ptr;
}

void arrow_mimalloc_free(void* ptr) {
  static void (*real_fn)(void*) = NULL;
  if (!real_fn) {
    real_fn = dlsym(RTLD_NEXT, "arrow_mimalloc_free");
    if (!real_fn) {
      fprintf(stderr, "INTERPOSE ERROR: cannot find arrow_mimalloc_free\n");
      abort();
    }
  }
  atomic_fetch_add(&g_free_count, 1);
  real_fn(ptr);
}

void* arrow_mimalloc_reallocate(void* ptr, size_t new_size, size_t alignment) {
  static void* (*real_fn)(void*, size_t, size_t) = NULL;
  if (!real_fn) {
    real_fn = dlsym(RTLD_NEXT, "arrow_mimalloc_reallocate");
    if (!real_fn) {
      fprintf(stderr, "INTERPOSE ERROR: cannot find arrow_mimalloc_reallocate\n");
      abort();
    }
  }
  void* new_ptr = real_fn(ptr, new_size, alignment);
  atomic_fetch_add(&g_realloc_count, 1);
  return new_ptr;
}

// Called at library unload to print statistics
__attribute__((destructor)) static void print_interpose_stats(void) {
  size_t allocs = atomic_load(&g_alloc_count);
  size_t frees = atomic_load(&g_free_count);
  size_t reallocs = atomic_load(&g_realloc_count);
  size_t total = atomic_load(&g_total_allocated);

  // Print in a parseable format
  fprintf(stderr, "ARROW_INTERPOSE_STATS: allocs=%zu frees=%zu reallocs=%zu "
                  "total_bytes=%zu\n",
          allocs, frees, reallocs, total);
}
