// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <arch.h>
#include <stdint.h>
#include <limits.h>
#include <math.h>
#include <float.h>

void FULL_NAME(int8_max_min)(int8_t values[], int len, int8_t* minout, int8_t* maxout) {
  int8_t max = INT8_MIN;
  int8_t min = INT8_MAX;

  for (int i = 0; i < len; ++i) {
    min = min < values[i] ? min : values[i];
    max = max > values[i] ? max : values[i];
  }

  *maxout = max;
  *minout = min;
}

void FULL_NAME(uint8_max_min)(uint8_t values[], int len, uint8_t* minout, uint8_t* maxout) {
  uint8_t max = 0;
  uint8_t min = UINT8_MAX;

  for (int i = 0; i < len; ++i) {
    min = min < values[i] ? min : values[i];
    max = max > values[i] ? max : values[i];
  }

  *maxout = max;
  *minout = min;
}

void FULL_NAME(int16_max_min)(int16_t values[], int len, int16_t* minout, int16_t* maxout) {
  int16_t max = INT16_MIN;
  int16_t min = INT16_MAX;

  for (int i = 0; i < len; ++i) {
    min = min < values[i] ? min : values[i];
    max = max > values[i] ? max : values[i];
  }

  *maxout = max;
  *minout = min;
}

void FULL_NAME(uint16_max_min)(uint16_t values[], int len, uint16_t* minout, uint16_t* maxout) {
  uint16_t max = 0;
  uint16_t min = UINT16_MAX;

  for (int i = 0; i < len; ++i) {
    min = min < values[i] ? min : values[i];
    max = max > values[i] ? max : values[i];
  }

  *maxout = max;
  *minout = min;
}

void FULL_NAME(int32_max_min)(int32_t values[], int len, int32_t* minout, int32_t* maxout) {
  int32_t max = INT32_MIN;
  int32_t min = INT32_MAX;

  for (int i = 0; i < len; ++i) {
    min = min < values[i] ? min : values[i];
    max = max > values[i] ? max : values[i];
  }

  *maxout = max;
  *minout = min;
}

void FULL_NAME(uint32_max_min)(uint32_t values[], int len, uint32_t* minout, uint32_t* maxout) {
  uint32_t max = 0;
  uint32_t min = UINT32_MAX;

  for (int i = 0; i < len; ++i) {
    min = min < values[i] ? min : values[i];
    max = max > values[i] ? max : values[i];
  }

  *maxout = max;
  *minout = min;
}

void FULL_NAME(int64_max_min)(int64_t values[], int len, int64_t* minout, int64_t* maxout) {
  int64_t max = INT64_MIN;
  int64_t min = INT64_MAX;

  for (int i = 0; i < len; ++i) {
    min = min < values[i] ? min : values[i];
    max = max > values[i] ? max : values[i];
  }

  *maxout = max;
  *minout = min;
}

void FULL_NAME(uint64_max_min)(uint64_t values[], int len, uint64_t* minout, uint64_t* maxout) {
  uint64_t max = 0;
  uint64_t min = UINT64_MAX;

  for (int i = 0; i < len; ++i) {
    min = min < values[i] ? min : values[i];
    max = max > values[i] ? max : values[i];
  }

  *maxout = max;
  *minout = min;
}
