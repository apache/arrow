// Tencent is pleased to support the open source community by making RapidJSON available.
// 
// Copyright (C) 2015 THL A29 Limited, a Tencent company, and Milo Yip.
//
// Licensed under the MIT License (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
// http://opensource.org/licenses/MIT
//
// Unless required by applicable law or agreed to in writing, software distributed 
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
// CONDITIONS OF ANY KIND, either express or implied. See the License for the 
// specific language governing permissions and limitations under the License.

#include "unittest.h"
#include "rapidjson/internal/dtoa.h"

#ifdef __GNUC__
RAPIDJSON_DIAG_PUSH
RAPIDJSON_DIAG_OFF(type-limits)
#endif

using namespace rapidjson::internal;

TEST(dtoa, normal) {
    char buffer[30];

#define TEST_DTOA(d, a)\
    *dtoa(d, buffer) = '\0';\
    EXPECT_STREQ(a, buffer)

    TEST_DTOA(0.0, "0.0");
    TEST_DTOA(-0.0, "-0.0");
    TEST_DTOA(1.0, "1.0");
    TEST_DTOA(-1.0, "-1.0");
    TEST_DTOA(1.2345, "1.2345");
    TEST_DTOA(1.2345678, "1.2345678");
    TEST_DTOA(0.123456789012, "0.123456789012");
    TEST_DTOA(1234567.8, "1234567.8");
    TEST_DTOA(-79.39773355813419, "-79.39773355813419");
    TEST_DTOA(-36.973846435546875, "-36.973846435546875");
    TEST_DTOA(0.000001, "0.000001");
    TEST_DTOA(0.0000001, "1e-7");
    TEST_DTOA(1e30, "1e30");
    TEST_DTOA(1.234567890123456e30, "1.234567890123456e30");
    TEST_DTOA(5e-324, "5e-324"); // Min subnormal positive double
    TEST_DTOA(2.225073858507201e-308, "2.225073858507201e-308"); // Max subnormal positive double
    TEST_DTOA(2.2250738585072014e-308, "2.2250738585072014e-308"); // Min normal positive double
    TEST_DTOA(1.7976931348623157e308, "1.7976931348623157e308"); // Max double

#undef TEST_DTOA
}

TEST(dtoa, maxDecimalPlaces) {
    char buffer[30];

#define TEST_DTOA(m, d, a)\
    *dtoa(d, buffer, m) = '\0';\
    EXPECT_STREQ(a, buffer)

    TEST_DTOA(3, 0.0, "0.0");
    TEST_DTOA(1, 0.0, "0.0");
    TEST_DTOA(3, -0.0, "-0.0");
    TEST_DTOA(3, 1.0, "1.0");
    TEST_DTOA(3, -1.0, "-1.0");
    TEST_DTOA(3, 1.2345, "1.234");
    TEST_DTOA(2, 1.2345, "1.23");
    TEST_DTOA(1, 1.2345, "1.2");
    TEST_DTOA(3, 1.2345678, "1.234");
    TEST_DTOA(3, 1.0001, "1.0");
    TEST_DTOA(2, 1.0001, "1.0");
    TEST_DTOA(1, 1.0001, "1.0");
    TEST_DTOA(3, 0.123456789012, "0.123");
    TEST_DTOA(2, 0.123456789012, "0.12");
    TEST_DTOA(1, 0.123456789012, "0.1");
    TEST_DTOA(4, 0.0001, "0.0001");
    TEST_DTOA(3, 0.0001, "0.0");
    TEST_DTOA(2, 0.0001, "0.0");
    TEST_DTOA(1, 0.0001, "0.0");
    TEST_DTOA(3, 1234567.8, "1234567.8");
    TEST_DTOA(3, 1e30, "1e30");
    TEST_DTOA(3, 5e-324, "0.0"); // Min subnormal positive double
    TEST_DTOA(3, 2.225073858507201e-308, "0.0"); // Max subnormal positive double
    TEST_DTOA(3, 2.2250738585072014e-308, "0.0"); // Min normal positive double
    TEST_DTOA(3, 1.7976931348623157e308, "1.7976931348623157e308"); // Max double
    TEST_DTOA(5, -0.14000000000000001, "-0.14");
    TEST_DTOA(4, -0.14000000000000001, "-0.14");
    TEST_DTOA(3, -0.14000000000000001, "-0.14");
    TEST_DTOA(3, -0.10000000000000001, "-0.1");
    TEST_DTOA(2, -0.10000000000000001, "-0.1");
    TEST_DTOA(1, -0.10000000000000001, "-0.1");

#undef TEST_DTOA
}


#ifdef __GNUC__
RAPIDJSON_DIAG_POP
#endif
