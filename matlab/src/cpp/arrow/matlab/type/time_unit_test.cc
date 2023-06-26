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

#include <gtest/gtest.h>

#include "arrow/matlab/type/time_unit.h"

#include <string>
#include <sstream>

namespace arrow::matlab::test {
// TODO: Remove this placeholder test.

namespace {
    std::string timeUnitToString(arrow::TimeUnit::type unit) {
        switch (unit) {
            case arrow::TimeUnit::type::SECOND: 
                return "Second";
            case arrow::TimeUnit::type::MILLI: 
                return "Millisecond";
            case arrow::TimeUnit::type::MICRO: 
                return "Microsecond";
            case arrow::TimeUnit::type::NANO: 
                return "Nanosecond";
        }
    }

    std::string mismatch_error_message(const arrow::TimeUnit::type actual, const arrow::TimeUnit::type expected) { 
        std::stringstream stream;
        stream << "----------------------";
        stream << std::endl;
        stream << "Actual unit = ";
        stream << timeUnitToString(actual);
        stream << std::endl;
        stream << "Expected unit = ";
        stream << timeUnitToString(expected);
        stream << std::endl;
        stream << "----------------------";
        stream << std::endl;
        return stream.str();
    }

    void verifyTimeUnitFromString(const std::u16string& str, arrow::TimeUnit::type expected) {
       auto result = arrow::matlab::type::timeUnitFromString(str);
        ASSERT_TRUE(result.ok());
        const auto actual = *result;
        EXPECT_EQ(actual, expected) << mismatch_error_message(actual, expected);
    }
}

TEST(TimeUnitTestSuite, ValidTimeUnitToString) {
    // Verify timeUnitFromString returns a result with the 
    // expected arrow::TimeUnit::type.
    verifyTimeUnitFromString(u"Second", arrow::TimeUnit::type::SECOND);
    verifyTimeUnitFromString(u"Millisecond", arrow::TimeUnit::type::MILLI);
    verifyTimeUnitFromString(u"Microsecond", arrow::TimeUnit::type::MICRO);
    verifyTimeUnitFromString(u"Nanosecond", arrow::TimeUnit::type::NANO);
}

TEST(TimeUnitTestSuite, InvalidTimeUnitToString) {
    // Verify timeUnitFromString returns an invalid result if given
    // an unknown time unit string.
    auto result = arrow::matlab::type::timeUnitFromString(u"bad");
    ASSERT_FALSE(result.ok());
    const auto msg = result.status().message();
    EXPECT_EQ(msg, "Unknown time unit string: bad");
}

}  // namespace arrow::matlab::test
