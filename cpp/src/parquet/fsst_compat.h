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

// Provide minimal compatibility shims so third-party FSST sources
// can be compiled with the compilers Arrow supports.

#if defined(_WIN32) && !defined(_MSC_VER)
#  include <cpuid.h>

// MinGW does not provide __cpuidex, but FSST only needs the CPUID
// leaf/sub-leaf variant that __cpuid_count implements.
static inline void arrow_fsst_cpuidex(int info[4], int function_id, int subfunction_id) {
  __cpuid_count(function_id, subfunction_id, info[0], info[1], info[2], info[3]);
}
#  define __cpuidex(info, function_id, subfunction_id) \
    arrow_fsst_cpuidex(info, function_id, subfunction_id)
#endif
