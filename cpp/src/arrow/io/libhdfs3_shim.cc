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

// This shim interface to libhdfs (for runtime shared library loading) has been
// adapted from the SFrame project, released under the ASF-compatible 3-clause
// BSD license
//
// Using this required having the $JAVA_HOME and $HADOOP_HOME environment
// variables set, so that libjvm and libhdfs can be located easily

// Copyright (C) 2015 Dato, Inc.
// All rights reserved.
//
// This software may be modified and distributed under the terms
// of the BSD license. See the LICENSE file for details.

#ifdef HAS_HADOOP

#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

#include "arrow/util/status.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace io {

Status ARROW_EXPORT ConnectLibHdfs() {
  return Status::OK();
}

}  // namespace io
}  // namespace arrow

#endif  // HAS_HADOOP
