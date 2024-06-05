/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <arrow/api.h>

#include <arrow-glib/type.h>

GArrowType
garrow_type_from_raw(arrow::Type::type type);
GArrowTimeUnit
garrow_time_unit_from_raw(arrow::TimeUnit::type unit);
arrow::TimeUnit::type
garrow_time_unit_to_raw(GArrowTimeUnit unit);
GArrowIntervalType
garrow_interval_type_from_raw(arrow::IntervalType::type type);
