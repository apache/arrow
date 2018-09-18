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

// String functions

extern "C" {

#include <string.h>
#include "./types.h"

FORCE_INLINE
int32 octet_length_utf8(const utf8 input, int32 length) { return length; }

FORCE_INLINE
int32 bit_length_utf8(const utf8 input, int32 length) { return length * 8; }

FORCE_INLINE
int32 octet_length_binary(const binary input, int32 length) { return length; }

FORCE_INLINE
int32 bit_length_binary(const binary input, int32 length) { return length * 8; }

FORCE_INLINE
int32 mem_compare(const char* left, int32 left_len, const char* right, int32 right_len) {
  int min = left_len;
  if (right_len < min) {
    min = right_len;
  }

  int cmp_ret = memcmp(left, right, min);
  if (cmp_ret != 0) {
    return cmp_ret;
  } else {
    return left_len - right_len;
  }
}

// Expand inner macro for all varlen types.
#define VAR_LEN_OP_TYPES(INNER, NAME, OP) \
  INNER(NAME, utf8, OP)                   \
  INNER(NAME, binary, OP)

// Relational binary fns : left, right params are same, return is bool.
#define BINARY_RELATIONAL(NAME, TYPE, OP)                                        \
  FORCE_INLINE                                                                   \
  bool NAME##_##TYPE##_##TYPE(const TYPE left, int32 left_len, const TYPE right, \
                              int32 right_len) {                                 \
    return mem_compare(left, left_len, right, right_len) OP 0;                   \
  }

VAR_LEN_OP_TYPES(BINARY_RELATIONAL, equal, ==)
VAR_LEN_OP_TYPES(BINARY_RELATIONAL, not_equal, !=)
VAR_LEN_OP_TYPES(BINARY_RELATIONAL, less_than, <)
VAR_LEN_OP_TYPES(BINARY_RELATIONAL, less_than_or_equal_to, <=)
VAR_LEN_OP_TYPES(BINARY_RELATIONAL, greater_than, >)
VAR_LEN_OP_TYPES(BINARY_RELATIONAL, greater_than_or_equal_to, >=)

// Expand inner macro for all varlen types.
#define VAR_LEN_TYPES(INNER) \
  INNER(utf8)                \
  INNER(binary)

FORCE_INLINE
bool starts_with_utf8_utf8(const char *data, int32 data_len, const char *prefix,
                           int32 prefix_len) {
  return ((data_len >= prefix_len) && (memcmp(data, prefix, prefix_len) == 0));
}

FORCE_INLINE
bool ends_with_utf8_utf8(const char *data, int32 data_len, const char *suffix,
                         int32 suffix_len) {
  return ((data_len >= suffix_len) &&
          (memcmp(data + data_len - suffix_len, suffix, suffix_len) == 0));
}

FORCE_INLINE
bool starts_with_plus_one_utf8_utf8(const char *data, int32 data_len, const char *prefix,
                                    int32 prefix_len) {
  return ((data_len == prefix_len + 1) && (memcmp(data, prefix, prefix_len) == 0));
}

FORCE_INLINE
bool ends_with_plus_one_utf8_utf8(const char *data, int32 data_len, const char *suffix,
                                  int32 suffix_len) {
  return ((data_len == suffix_len + 1) && (memcmp(data + 1, suffix, suffix_len) == 0));
}

}  // extern "C"
