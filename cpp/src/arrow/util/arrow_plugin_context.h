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

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Context that contains information about the Arrow Plugin .
 **/
typedef struct ArrowPluginContext {
  void* priv;
} ArrowPluginContext;

/**
 * @brief Signature of a callback function that compress block of data.
 **/
typedef int64_t (*ArrowPluginCompressCallback)(int64_t input_len, const uint8_t* input,
                                               int64_t output_buffer_len,
                                               uint8_t* output_buffer);

/**
 * @brief Signature of a callback function that decompress block of data.
 **/
typedef int64_t (*ArrowPluginDecompressCallback)(int64_t input_len, const uint8_t* input,
                                                 int64_t output_buffer_len,
                                                 uint8_t* output_buffer);

typedef int64_t (*ArrowPluginMaxCompressedLenCallback)(int64_t input_len,
                                                       const uint8_t* input);

typedef const char* (*ArrowPluginGetNameCallback)();

/**
 * @brief Context that contains information about the Arrow Plugin .
 **/
typedef struct ArrowPluginCompressionContext {
  ArrowPluginMaxCompressedLenCallback maxCompressedLenCallback;
  ArrowPluginCompressCallback compressCallback;
  ArrowPluginDecompressCallback decompressCallback;
  ArrowPluginGetNameCallback getNameCallback;
} ArrowPluginCompressionContext;

#ifdef __cplusplus
}
#endif
