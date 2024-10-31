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

#include <memory>

#include <arrow/util/decimal.h>

#include <arrow-glib/decimal.h>

GArrowDecimal32 *
garrow_decimal32_new_raw(std::shared_ptr<arrow::Decimal32> *arrow_decimal32);
std::shared_ptr<arrow::Decimal32>
garrow_decimal32_get_raw(GArrowDecimal32 *decimal);

GArrowDecimal64 *
garrow_decimal64_new_raw(std::shared_ptr<arrow::Decimal64> *arrow_decimal64);
std::shared_ptr<arrow::Decimal64>
garrow_decimal64_get_raw(GArrowDecimal64 *decimal);

GArrowDecimal128 *
garrow_decimal128_new_raw(std::shared_ptr<arrow::Decimal128> *arrow_decimal128);
std::shared_ptr<arrow::Decimal128>
garrow_decimal128_get_raw(GArrowDecimal128 *decimal);

GArrowDecimal256 *
garrow_decimal256_new_raw(std::shared_ptr<arrow::Decimal256> *arrow_decimal256);
std::shared_ptr<arrow::Decimal256>
garrow_decimal256_get_raw(GArrowDecimal256 *decimal);
