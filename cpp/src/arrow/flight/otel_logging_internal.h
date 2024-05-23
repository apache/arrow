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

#include "arrow/util/config.h"

#include "arrow/util/macros.h"
#ifdef ARROW_WITH_OPENTELEMETRY
#include "arrow/flight/otel_logging.h"
#include "arrow/util/logger.h"

namespace arrow::flight::internal {

ARROW_EXPORT std::shared_ptr<util::Logger> GetOtelGrpcClientLogger();
ARROW_EXPORT std::shared_ptr<util::Logger> GetOtelGrpcServerLogger();
ARROW_EXPORT std::shared_ptr<util::Logger> GetOtelSqlClientLogger();
ARROW_EXPORT std::shared_ptr<util::Logger> GetOtelSqlServerLogger();

}  // namespace arrow::flight::internal

#define ARROW_FLIGHT_OTELLOG_CLIENT(LEVEL, ...)                                  \
  ARROW_LOGGER_CALL(::arrow::flight::internal::GetOtelGrpcClientLogger(), LEVEL, \
                    __VA_ARGS__)
#define ARROW_FLIGHT_OTELLOG_SERVER(LEVEL, ...)                                  \
  ARROW_LOGGER_CALL(::arrow::flight::internal::GetOtelGrpcServerLogger(), LEVEL, \
                    __VA_ARGS__)
#define ARROW_FLIGHT_OTELLOG_SQL_CLIENT(LEVEL, ...)                             \
  ARROW_LOGGER_CALL(::arrow::flight::internal::GetOtelSqlClientLogger(), LEVEL, \
                    __VA_ARGS__)
#define ARROW_FLIGHT_OTELLOG_SQL_SERVER(LEVEL, ...)                             \
  ARROW_LOGGER_CALL(::arrow::flight::internal::GetOtelSqlServerLogger(), LEVEL, \
                    __VA_ARGS__)

#else

#define ARROW_FLIGHT_OTELLOG_CLIENT(LEVEL, ...) ARROW_UNUSED(0)
#define ARROW_FLIGHT_OTELLOG_SERVER(LEVEL, ...) ARROW_UNUSED(0)
#define ARROW_FLIGHT_OTELLOG_SQL_CLIENT(LEVEL, ...) ARROW_UNUSED(0)
#define ARROW_FLIGHT_OTELLOG_SQL_SERVER(LEVEL, ...) ARROW_UNUSED(0)

#endif
