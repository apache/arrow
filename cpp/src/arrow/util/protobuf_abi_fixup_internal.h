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

// TODO explain this

#ifndef NDEBUG

#ifdef GOOGLE_PROTOBUF_METADATA_LITE_H__
#error "Protobuf already included, fixup can't be applied!"
#endif
#define SAVED_NDEBUG 0
#define NDEBUG

#else

#define SAVED_NDEBUG 1

#endif

#include <google/protobuf/metadata_lite.h>

#ifndef GOOGLE_PROTOBUF_METADATA_LITE_H__
#error "Protobuf metadata internal not included!"
#endif

#if !SAVED_NDEBUG
#undef NDEBUG
#endif

#define DEFINE_ABI_FIXUP_FOR_PROTOBUF()                                 \
  volatile ::google::protobuf::internal::InternalMetadata* dummy_pb_md; \
                                                                        \
  void AbiFixupForProtobuf() {                                          \
    using ::google::protobuf::internal::InternalMetadata;               \
    dummy_pb_md->~InternalMetadata();                                   \
  }
