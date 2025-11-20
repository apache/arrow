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

#include "arrow/util/macros.h"

// GH-44954: silence [[deprecated]] declarations in protobuf-generated code
ARROW_SUPPRESS_DEPRECATION_WARNING
#include "arrow/flight/sql/protocol_internal.h"

ARROW_SUPPRESS_MISSING_DECLARATIONS_WARNING

// NOTE(lidavidm): Normally this is forbidden, but on Windows to get
// the dllexport/dllimport macro in the right places, we need to
// ensure our header gets included (and Protobuf will not insert the
// include for you)
#include "arrow/flight/sql/FlightSql.pb.cc"  // NOLINT

ARROW_UNSUPPRESS_MISSING_DECLARATIONS_WARNING
ARROW_UNSUPPRESS_DEPRECATION_WARNING
