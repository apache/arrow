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

#include <Rcpp.h>
#undef Free
#include <arrow/api.h>
#include <arrow/type.h>

using xptr_DataType = Rcpp::XPtr<std::shared_ptr<arrow::DataType>>;
using xptr_FixedWidthType = Rcpp::XPtr<std::shared_ptr<arrow::FixedWidthType>>;
using xptr_DateType = Rcpp::XPtr<std::shared_ptr<arrow::DateType>>;
using xptr_TimeType = Rcpp::XPtr<std::shared_ptr<arrow::TimeType>>;
using xptr_DecimalType = Rcpp::XPtr<std::shared_ptr<arrow::DecimalType>>;
using xptr_Decimal128Type = Rcpp::XPtr<std::shared_ptr<arrow::Decimal128Type>>;
using xptr_TimestampType = Rcpp::XPtr<std::shared_ptr<arrow::TimestampType>>;
using xptr_Field = Rcpp::XPtr<std::shared_ptr<arrow::Field>>;
using xptr_Schema = Rcpp::XPtr<std::shared_ptr<arrow::Schema>>;
using xptr_ListType = Rcpp::XPtr<std::shared_ptr<arrow::ListType>>;
using xptr_ArrayData = Rcpp::XPtr<std::shared_ptr<arrow::ArrayData>>;
using xptr_Array = Rcpp::XPtr<std::shared_ptr<arrow::Array>>;

using xptr_ArrayBuilder = Rcpp::XPtr<std::unique_ptr<arrow::ArrayBuilder>>;

RCPP_EXPOSED_ENUM_NODECL(arrow::Type::type)
RCPP_EXPOSED_ENUM_NODECL(arrow::DateUnit)
RCPP_EXPOSED_ENUM_NODECL(arrow::TimeUnit::type)
