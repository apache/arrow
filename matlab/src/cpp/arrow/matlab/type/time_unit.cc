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

#include "arrow/matlab/type/time_unit.h"
#include "arrow/util/utf8.h"

namespace arrow::matlab::type {

    arrow::Result<arrow::TimeUnit::type> timeUnitFromString(std::u16string_view unit_str) {
        if (unit_str == u"Second") {
            return arrow::TimeUnit::type::SECOND;
        } else if (unit_str == u"Millisecond") {
            return arrow::TimeUnit::type::MILLI;
        } else if (unit_str == u"Microsecond") {
            return arrow::TimeUnit::type::MICRO;
        } else if (unit_str == u"Nanosecond") {
            return arrow::TimeUnit::type::NANO;
        } else {
            auto maybe_utf8 = arrow::util::UTF16StringToUTF8(unit_str);
            auto msg = maybe_utf8.ok() ? "Unknown time unit string: " + *maybe_utf8 : "Unknown time unit string";
            return arrow::Status::Invalid(msg);
        }
    }

    template<>
    arrow::Status validateTimeUnit<arrow::Time32Type>(arrow::TimeUnit::type unit) {
        using arrow::TimeUnit;
        if (unit == TimeUnit::type::SECOND || unit == TimeUnit::type::MILLI) {
            return arrow::Status::OK();
        } else {
            return arrow::Status::Invalid("TimeUnit for Time32 must be Second or Millisecond"); 
        }
    }

    template<>
    arrow::Status validateTimeUnit<arrow::Time64Type>(arrow::TimeUnit::type unit) {
        using arrow::TimeUnit;
        if (unit == TimeUnit::type::MICRO || unit == TimeUnit::type::NANO) {
            return arrow::Status::OK();
        } else {
            return arrow::Status::Invalid("TimeUnit for Time64 must be Microsecond or Nanosecond"); 
        }
    }
}
