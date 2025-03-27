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

#ifdef _WIN32
#  include <windows.h>
#endif

#include <sql.h>
#include <sqltypes.h>
#include <sqlucode.h>

//  @file odbc_api.h
//
//  Define internal ODBC API function headers.

namespace arrow
{
  SQLRETURN SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE* result);

  SQLRETURN SQLAllocEnv(SQLHENV* env);
} //arrow