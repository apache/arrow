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

// Force-included into the flight-grpc-example sources (including the
// protoc-generated ones, which we can't edit). Same workaround as
// arrow/flight/transport/grpc/customize_grpc.h, added in GH-45885:
// Abseil built with NDEBUG (e.g. conda-forge) doesn't export
// Mutex::Dtor(), so debug TUs that inline ~Mutex() (via
// grpc::CompletionQueue) fail to link.
// See https://github.com/abseil/abseil-cpp/issues/1624
// and https://github.com/conda-forge/abseil-cpp-feedstock/issues/104
#if __has_include(<absl/synchronization/mutex.h>)
#  ifndef NDEBUG
#    define ARROW_EXAMPLE_NO_NDEBUG
#    define NDEBUG
#  endif
#  include <absl/synchronization/mutex.h>
#  ifdef ARROW_EXAMPLE_NO_NDEBUG
#    undef NDEBUG
#    undef ARROW_EXAMPLE_NO_NDEBUG
#  endif
#endif
