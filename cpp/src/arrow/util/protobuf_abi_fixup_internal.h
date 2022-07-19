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

#include <google/protobuf/stubs/common.h>

// ARROW-17104: on recent protobuf versions, ABI differs between debug
// and non-debug builds (keyed on NDEBUG).  We want to be able to compile
// Arrow C++ in debug mode using non-debug builds of protobuf as provided
// by package managers such as conda, homebrew, apt, etc.  Therefore we
// need to make sure the offending header file is always included with
// NDEBUG set (simulating a non-debug build).
//
// Note the workaround isn't needed (and may actually fail compiling) on MSVC.
//
// More details in upstream issue:
// https://github.com/protocolbuffers/protobuf/issues/9947

#if GOOGLE_PROTOBUF_VERSION >= 3020000 && !defined(_MSC_VER)

#ifndef NDEBUG

#ifdef GOOGLE_PROTOBUF_METADATA_LITE_H__
#error "Protobuf already included, fixup can't be applied!"
#endif
#define SAVED_NDEBUG 0
#define NDEBUG

#else

#define SAVED_NDEBUG 1

#endif

// The offending header file:
// google::protobuf::internal::InternalMetadata::~InternalMetadata()
// is inlined in the header if NDEBUG is defined.
#include <google/protobuf/metadata_lite.h>

#ifndef GOOGLE_PROTOBUF_METADATA_LITE_H__
#error "Protobuf metadata internal not included!"
#endif

#if !SAVED_NDEBUG
#undef NDEBUG
#endif

// It's not enough to fixup header inclusion, we must also actually embed
// the google::protobuf::internal::InternalMetadata::~InternalMetadata()
// implementation explicitly in all Arrow DLLs that depend on protobuf.
// To that aim, a macro is provided that can be used to define a dummy
// internal (but exported) function to ensure ~InternalMetadata() is
// codegen'ed.

// Disable optimizations on function so that ~InternalMetadata() is not
// inlined (which would defeat the point).
#if defined(__clang__)
#define NO_OPTIMIZE __attribute__((optnone))
#elif defined(__GNUC__)
#define NO_OPTIMIZE __attribute__((optimize(0)))
#else
#define NO_OPTIMIZE
#endif

#define DEFINE_ABI_FIXUP_FOR_PROTOBUF()                                 \
  volatile ::google::protobuf::internal::InternalMetadata* dummy_pb_md; \
                                                                        \
  NO_OPTIMIZE void AbiFixupForProtobuf() {                              \
    using ::google::protobuf::internal::InternalMetadata;               \
    dummy_pb_md->~InternalMetadata();                                   \
  }

#else  // GOOGLE_PROTOBUF_VERSION < 3020000 || defined(_MSC_VER)

// No fixup needed. Also, the offending header file may not even exist
// depending on how old protobuf is.

#define DEFINE_ABI_FIXUP_FOR_PROTOBUF()

#endif
