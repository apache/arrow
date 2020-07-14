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

#include "arrow/tensor/converter.h"

#define DISPATCH(ACTION, index_elsize, value_elsize, ...) \
  switch (index_elsize) {                                 \
    case 1:                                               \
      switch (value_elsize) {                             \
        case 1:                                           \
          ACTION(uint8_t, uint8_t, __VA_ARGS__);          \
          break;                                          \
        case 2:                                           \
          ACTION(uint8_t, uint16_t, __VA_ARGS__);         \
          break;                                          \
        case 4:                                           \
          ACTION(uint8_t, uint32_t, __VA_ARGS__);         \
          break;                                          \
        case 8:                                           \
          ACTION(uint8_t, uint64_t, __VA_ARGS__);         \
          break;                                          \
      }                                                   \
      break;                                              \
    case 2:                                               \
      switch (value_elsize) {                             \
        case 1:                                           \
          ACTION(uint16_t, uint8_t, __VA_ARGS__);         \
          break;                                          \
        case 2:                                           \
          ACTION(uint16_t, uint16_t, __VA_ARGS__);        \
          break;                                          \
        case 4:                                           \
          ACTION(uint16_t, uint32_t, __VA_ARGS__);        \
          break;                                          \
        case 8:                                           \
          ACTION(uint16_t, uint64_t, __VA_ARGS__);        \
          break;                                          \
      }                                                   \
      break;                                              \
    case 4:                                               \
      switch (value_elsize) {                             \
        case 1:                                           \
          ACTION(uint32_t, uint8_t, __VA_ARGS__);         \
          break;                                          \
        case 2:                                           \
          ACTION(uint32_t, uint16_t, __VA_ARGS__);        \
          break;                                          \
        case 4:                                           \
          ACTION(uint32_t, uint32_t, __VA_ARGS__);        \
          break;                                          \
        case 8:                                           \
          ACTION(uint32_t, uint64_t, __VA_ARGS__);        \
          break;                                          \
      }                                                   \
      break;                                              \
    case 8:                                               \
      switch (value_elsize) {                             \
        case 1:                                           \
          ACTION(int64_t, uint8_t, __VA_ARGS__);          \
          break;                                          \
        case 2:                                           \
          ACTION(int64_t, uint16_t, __VA_ARGS__);         \
          break;                                          \
        case 4:                                           \
          ACTION(int64_t, uint32_t, __VA_ARGS__);         \
          break;                                          \
        case 8:                                           \
          ACTION(int64_t, uint64_t, __VA_ARGS__);         \
          break;                                          \
      }                                                   \
      break;                                              \
  }
