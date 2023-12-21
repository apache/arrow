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

#include "rapidjson/internal/strtod.h"

#ifdef __clang__
RAPIDJSON_DIAG_PUSH
RAPIDJSON_DIAG_OFF(unreachable-code)
#endif

#define BIGINTEGER_LITERAL(s) BigInteger(s, sizeof(s) - 1)

using namespace rapidjson::internal;

TEST(Strtod, CheckApproximationCase) {
    static const int kSignificandSize = 52;
    static const int kExponentBias = 0x3FF;
    static const uint64_t kExponentMask = RAPIDJSON_UINT64_C2(0x7FF00000, 0x00000000);
    static const uint64_t kSignificandMask = RAPIDJSON_UINT64_C2(0x000FFFFF, 0xFFFFFFFF);
    static const uint64_t kHiddenBit = RAPIDJSON_UINT64_C2(0x00100000, 0x00000000);

    // http://www.exploringbinary.com/using-integers-to-check-a-floating-point-approximation/
    // Let b = 0x1.465a72e467d88p-149
    //       = 5741268244528520 x 2^-201
    union {
        double d;
        uint64_t u;
    }u;
    u.u = 0x465a72e467d88 | ((static_cast<uint64_t>(-149 + kExponentBias)) << kSignificandSize);
    const double b = u.d;
    const uint64_t bInt = (u.u & kSignificandMask) | kHiddenBit;
    const int bExp = static_cast<int>(((u.u & kExponentMask) >> kSignificandSize) - kExponentBias - kSignificandSize);
    EXPECT_DOUBLE_EQ(1.7864e-45, b);
    EXPECT_EQ(RAPIDJSON_UINT64_C2(0x001465a7, 0x2e467d88), bInt);
    EXPECT_EQ(-201, bExp);

    // Let d = 17864 x 10-49
    const char dInt[] = "17864";
    const int dExp = -49;

    // Let h = 2^(bExp-1)
    const int hExp = bExp - 1;
    EXPECT_EQ(-202, hExp);

    int dS_Exp2 = 0;
    int dS_Exp5 = 0;
    int bS_Exp2 = 0;
    int bS_Exp5 = 0;
    int hS_Exp2 = 0;
    int hS_Exp5 = 0;

    // Adjust for decimal exponent
    if (dExp >= 0) {
        dS_Exp2 += dExp;
        dS_Exp5 += dExp;
    }
    else {
        bS_Exp2 -= dExp;
        bS_Exp5 -= dExp;
        hS_Exp2 -= dExp;
        hS_Exp5 -= dExp;
    }

    // Adjust for binary exponent
    if (bExp >= 0)
        bS_Exp2 += bExp;
    else {
        dS_Exp2 -= bExp;
        hS_Exp2 -= bExp;
    }

    // Adjust for half ulp exponent
    if (hExp >= 0)
        hS_Exp2 += hExp;
    else {
        dS_Exp2 -= hExp;
        bS_Exp2 -= hExp;
    }

    // Remove common power of two factor from all three scaled values
    int common_Exp2 = (std::min)(dS_Exp2, (std::min)(bS_Exp2, hS_Exp2));
    dS_Exp2 -= common_Exp2;
    bS_Exp2 -= common_Exp2;
    hS_Exp2 -= common_Exp2;

    EXPECT_EQ(153, dS_Exp2);
    EXPECT_EQ(0, dS_Exp5);
    EXPECT_EQ(1, bS_Exp2);
    EXPECT_EQ(49, bS_Exp5);
    EXPECT_EQ(0, hS_Exp2);
    EXPECT_EQ(49, hS_Exp5);

    BigInteger dS = BIGINTEGER_LITERAL(dInt);
    dS.MultiplyPow5(static_cast<unsigned>(dS_Exp5)) <<= static_cast<size_t>(dS_Exp2);

    BigInteger bS(bInt);
    bS.MultiplyPow5(static_cast<unsigned>(bS_Exp5)) <<= static_cast<size_t>(bS_Exp2);

    BigInteger hS(1);
    hS.MultiplyPow5(static_cast<unsigned>(hS_Exp5)) <<= static_cast<size_t>(hS_Exp2);

    EXPECT_TRUE(BIGINTEGER_LITERAL("203970822259994138521801764465966248930731085529088") == dS);
    EXPECT_TRUE(BIGINTEGER_LITERAL("203970822259994122305215569213032722473144531250000") == bS);
    EXPECT_TRUE(BIGINTEGER_LITERAL("17763568394002504646778106689453125") == hS);

    EXPECT_EQ(1, dS.Compare(bS));
    
    BigInteger delta(0);
    EXPECT_FALSE(dS.Difference(bS, &delta));
    EXPECT_TRUE(BIGINTEGER_LITERAL("16216586195252933526457586554279088") == delta);
    EXPECT_TRUE(bS.Difference(dS, &delta));
    EXPECT_TRUE(BIGINTEGER_LITERAL("16216586195252933526457586554279088") == delta);

    EXPECT_EQ(-1, delta.Compare(hS));
}

#ifdef __clang__
RAPIDJSON_DIAG_POP
#endif
