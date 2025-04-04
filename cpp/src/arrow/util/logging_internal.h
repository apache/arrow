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

#include "arrow/util/logging.h"

#pragma once

// These are internal-use macros and should not be used in public headers.
#ifndef DCHECK
#  define DCHECK ARROW_DCHECK
#endif
#ifndef DCHECK_OK
#  define DCHECK_OK ARROW_DCHECK_OK
#endif
#ifndef DCHECK_EQ
#  define DCHECK_EQ ARROW_DCHECK_EQ
#endif
#ifndef DCHECK_NE
#  define DCHECK_NE ARROW_DCHECK_NE
#endif
#ifndef DCHECK_LE
#  define DCHECK_LE ARROW_DCHECK_LE
#endif
#ifndef DCHECK_LT
#  define DCHECK_LT ARROW_DCHECK_LT
#endif
#ifndef DCHECK_GE
#  define DCHECK_GE ARROW_DCHECK_GE
#endif
#ifndef DCHECK_GT
#  define DCHECK_GT ARROW_DCHECK_GT
#endif
