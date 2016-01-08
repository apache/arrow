// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef PARQUET_UTIL_COMPILER_UTIL_H
#define PARQUET_UTIL_COMPILER_UTIL_H

// Compiler hint that this branch is likely or unlikely to
// be taken. Take from the "What all programmers should know
// about memory" paper.
// example: if (LIKELY(size > 0)) { ... }
// example: if (UNLIKELY(!status.ok())) { ... }
#ifdef LIKELY
#undef LIKELY
#endif

#ifdef UNLIKELY
#undef UNLIKELY
#endif

#define LIKELY(expr) __builtin_expect(!!(expr), 1)
#define UNLIKELY(expr) __builtin_expect(!!(expr), 0)

#define PREFETCH(addr) __builtin_prefetch(addr)

#endif // PARQUET_UTIL_COMPILER_UTIL_H
