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

#include <arrow/compute/api.h>

#include <arrow-glib/compute.h>

GArrowCastOptions *garrow_cast_options_new_raw(arrow::compute::CastOptions *arrow_cast_options);
arrow::compute::CastOptions *garrow_cast_options_get_raw(GArrowCastOptions *cast_options);

GArrowCountOptions *
garrow_count_options_new_raw(arrow::compute::CountOptions *arrow_count_options);
arrow::compute::CountOptions *
garrow_count_options_get_raw(GArrowCountOptions *count_options);

arrow::compute::TakeOptions *
garrow_take_options_get_raw(GArrowTakeOptions *take_options);

arrow::compute::CompareOptions *
garrow_compare_options_get_raw(GArrowCompareOptions *compare_options);
