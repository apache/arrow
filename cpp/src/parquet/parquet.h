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

#ifndef PARQUET_PARQUET_H
#define PARQUET_PARQUET_H

#include <exception>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "parquet/exception.h"

// Column reader API
#include "parquet/column/reader.h"

// File API
#include "parquet/file/reader.h"

// Schemas
#include "parquet/schema/descriptor.h"
#include "parquet/schema/printer.h"
#include "parquet/schema/types.h"

// IO
#include "parquet/util/input.h"
#include "parquet/util/output.h"

#endif
