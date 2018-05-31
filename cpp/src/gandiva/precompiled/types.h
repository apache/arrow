/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef PRECOMPILED_TYPES_H
#define PRECOMPILED_TYPES_H

#include <stdint.h>

/*
 * Use the same names as in arrow data types. Makes it easy to write pre-processor macros.
 */
using boolean = bool;
using int32 = int32_t;
using int64 = int64_t;
using float32 = float;
using float64 = double;
using date = int64_t;
using time64 = int64_t;
using timestamp64 = int64_t;

#endif //PRECOMPILED_TYPES_H
