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

#pragma once

// This addresses platform-specific defines, e.g. on Windows
#include "arrow/flight/platform.h"  // IWYU pragma: keep
#include "arrow/util/macros.h"

// This header holds the Flight SQL definitions.

#include "arrow/flight/sql/visibility.h"

ARROW_SUPPRESS_DEPRECATION_WARNING
#include "arrow/flight/sql/FlightSql.pb.h"  // IWYU pragma: export
ARROW_UNSUPPRESS_DEPRECATION_WARNING
