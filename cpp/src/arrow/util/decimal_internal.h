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

#include <array>
#include <cstdint>
#include <limits>
#include <type_traits>

#include "arrow/type_fwd.h"
#include "arrow/util/basic_decimal.h"
#include "arrow/util/endian.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

constexpr auto kInt64DecimalDigits =
    static_cast<size_t>(std::numeric_limits<int64_t>::digits10);

constexpr uint64_t kUInt64PowersOfTen[kInt64DecimalDigits + 1] = {
    // clang-format off
    1ULL,
    10ULL,
    100ULL,
    1000ULL,
    10000ULL,
    100000ULL,
    1000000ULL,
    10000000ULL,
    100000000ULL,
    1000000000ULL,
    10000000000ULL,
    100000000000ULL,
    1000000000000ULL,
    10000000000000ULL,
    100000000000000ULL,
    1000000000000000ULL,
    10000000000000000ULL,
    100000000000000000ULL,
    1000000000000000000ULL
    // clang-format on
};

// On the Windows R toolchain, INFINITY is double type instead of float
constexpr float kFloatInf = std::numeric_limits<float>::infinity();

// Attention: these pre-computed constants might not exactly represent their
// decimal counterparts:
//   >>> int(1e38)
//   99999999999999997748809823456034029568

constexpr int kPrecomputedPowersOfTen = 76;

constexpr float kFloatPowersOfTen[2 * kPrecomputedPowersOfTen + 1] = {
    0,         0,         0,         0,         0,         0,         0,
    0,         0,         0,         0,         0,         0,         0,
    0,         0,         0,         0,         0,         0,         0,
    0,         0,         0,         0,         0,         0,         0,
    0,         0,         0,         1e-45f,    1e-44f,    1e-43f,    1e-42f,
    1e-41f,    1e-40f,    1e-39f,    1e-38f,    1e-37f,    1e-36f,    1e-35f,
    1e-34f,    1e-33f,    1e-32f,    1e-31f,    1e-30f,    1e-29f,    1e-28f,
    1e-27f,    1e-26f,    1e-25f,    1e-24f,    1e-23f,    1e-22f,    1e-21f,
    1e-20f,    1e-19f,    1e-18f,    1e-17f,    1e-16f,    1e-15f,    1e-14f,
    1e-13f,    1e-12f,    1e-11f,    1e-10f,    1e-9f,     1e-8f,     1e-7f,
    1e-6f,     1e-5f,     1e-4f,     1e-3f,     1e-2f,     1e-1f,     1e0f,
    1e1f,      1e2f,      1e3f,      1e4f,      1e5f,      1e6f,      1e7f,
    1e8f,      1e9f,      1e10f,     1e11f,     1e12f,     1e13f,     1e14f,
    1e15f,     1e16f,     1e17f,     1e18f,     1e19f,     1e20f,     1e21f,
    1e22f,     1e23f,     1e24f,     1e25f,     1e26f,     1e27f,     1e28f,
    1e29f,     1e30f,     1e31f,     1e32f,     1e33f,     1e34f,     1e35f,
    1e36f,     1e37f,     1e38f,     kFloatInf, kFloatInf, kFloatInf, kFloatInf,
    kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf,
    kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf,
    kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf,
    kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf,
    kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf};

constexpr double kDoublePowersOfTen[2 * kPrecomputedPowersOfTen + 1] = {
    1e-76, 1e-75, 1e-74, 1e-73, 1e-72, 1e-71, 1e-70, 1e-69, 1e-68, 1e-67, 1e-66, 1e-65,
    1e-64, 1e-63, 1e-62, 1e-61, 1e-60, 1e-59, 1e-58, 1e-57, 1e-56, 1e-55, 1e-54, 1e-53,
    1e-52, 1e-51, 1e-50, 1e-49, 1e-48, 1e-47, 1e-46, 1e-45, 1e-44, 1e-43, 1e-42, 1e-41,
    1e-40, 1e-39, 1e-38, 1e-37, 1e-36, 1e-35, 1e-34, 1e-33, 1e-32, 1e-31, 1e-30, 1e-29,
    1e-28, 1e-27, 1e-26, 1e-25, 1e-24, 1e-23, 1e-22, 1e-21, 1e-20, 1e-19, 1e-18, 1e-17,
    1e-16, 1e-15, 1e-14, 1e-13, 1e-12, 1e-11, 1e-10, 1e-9,  1e-8,  1e-7,  1e-6,  1e-5,
    1e-4,  1e-3,  1e-2,  1e-1,  1e0,   1e1,   1e2,   1e3,   1e4,   1e5,   1e6,   1e7,
    1e8,   1e9,   1e10,  1e11,  1e12,  1e13,  1e14,  1e15,  1e16,  1e17,  1e18,  1e19,
    1e20,  1e21,  1e22,  1e23,  1e24,  1e25,  1e26,  1e27,  1e28,  1e29,  1e30,  1e31,
    1e32,  1e33,  1e34,  1e35,  1e36,  1e37,  1e38,  1e39,  1e40,  1e41,  1e42,  1e43,
    1e44,  1e45,  1e46,  1e47,  1e48,  1e49,  1e50,  1e51,  1e52,  1e53,  1e54,  1e55,
    1e56,  1e57,  1e58,  1e59,  1e60,  1e61,  1e62,  1e63,  1e64,  1e65,  1e66,  1e67,
    1e68,  1e69,  1e70,  1e71,  1e72,  1e73,  1e74,  1e75,  1e76};

constexpr BasicDecimal128 kDecimal128PowersOfTen[38 + 1] = {
    BasicDecimal128(1LL),
    BasicDecimal128(10LL),
    BasicDecimal128(100LL),
    BasicDecimal128(1000LL),
    BasicDecimal128(10000LL),
    BasicDecimal128(100000LL),
    BasicDecimal128(1000000LL),
    BasicDecimal128(10000000LL),
    BasicDecimal128(100000000LL),
    BasicDecimal128(1000000000LL),
    BasicDecimal128(10000000000LL),
    BasicDecimal128(100000000000LL),
    BasicDecimal128(1000000000000LL),
    BasicDecimal128(10000000000000LL),
    BasicDecimal128(100000000000000LL),
    BasicDecimal128(1000000000000000LL),
    BasicDecimal128(10000000000000000LL),
    BasicDecimal128(100000000000000000LL),
    BasicDecimal128(1000000000000000000LL),
    BasicDecimal128(0LL, 10000000000000000000ULL),
    BasicDecimal128(5LL, 7766279631452241920ULL),
    BasicDecimal128(54LL, 3875820019684212736ULL),
    BasicDecimal128(542LL, 1864712049423024128ULL),
    BasicDecimal128(5421LL, 200376420520689664ULL),
    BasicDecimal128(54210LL, 2003764205206896640ULL),
    BasicDecimal128(542101LL, 1590897978359414784ULL),
    BasicDecimal128(5421010LL, 15908979783594147840ULL),
    BasicDecimal128(54210108LL, 11515845246265065472ULL),
    BasicDecimal128(542101086LL, 4477988020393345024ULL),
    BasicDecimal128(5421010862LL, 7886392056514347008ULL),
    BasicDecimal128(54210108624LL, 5076944270305263616ULL),
    BasicDecimal128(542101086242LL, 13875954555633532928ULL),
    BasicDecimal128(5421010862427LL, 9632337040368467968ULL),
    BasicDecimal128(54210108624275LL, 4089650035136921600ULL),
    BasicDecimal128(542101086242752LL, 4003012203950112768ULL),
    BasicDecimal128(5421010862427522LL, 3136633892082024448ULL),
    BasicDecimal128(54210108624275221LL, 12919594847110692864ULL),
    BasicDecimal128(542101086242752217LL, 68739955140067328ULL),
    BasicDecimal128(5421010862427522170LL, 687399551400673280ULL)};

constexpr BasicDecimal128 kDecimal128HalfPowersOfTen[] = {
    BasicDecimal128(0ULL),
    BasicDecimal128(5ULL),
    BasicDecimal128(50ULL),
    BasicDecimal128(500ULL),
    BasicDecimal128(5000ULL),
    BasicDecimal128(50000ULL),
    BasicDecimal128(500000ULL),
    BasicDecimal128(5000000ULL),
    BasicDecimal128(50000000ULL),
    BasicDecimal128(500000000ULL),
    BasicDecimal128(5000000000ULL),
    BasicDecimal128(50000000000ULL),
    BasicDecimal128(500000000000ULL),
    BasicDecimal128(5000000000000ULL),
    BasicDecimal128(50000000000000ULL),
    BasicDecimal128(500000000000000ULL),
    BasicDecimal128(5000000000000000ULL),
    BasicDecimal128(50000000000000000ULL),
    BasicDecimal128(500000000000000000ULL),
    BasicDecimal128(5000000000000000000ULL),
    BasicDecimal128(2LL, 13106511852580896768ULL),
    BasicDecimal128(27LL, 1937910009842106368ULL),
    BasicDecimal128(271LL, 932356024711512064ULL),
    BasicDecimal128(2710LL, 9323560247115120640ULL),
    BasicDecimal128(27105LL, 1001882102603448320ULL),
    BasicDecimal128(271050LL, 10018821026034483200ULL),
    BasicDecimal128(2710505LL, 7954489891797073920ULL),
    BasicDecimal128(27105054LL, 5757922623132532736ULL),
    BasicDecimal128(271050543LL, 2238994010196672512ULL),
    BasicDecimal128(2710505431LL, 3943196028257173504ULL),
    BasicDecimal128(27105054312LL, 2538472135152631808ULL),
    BasicDecimal128(271050543121LL, 6937977277816766464ULL),
    BasicDecimal128(2710505431213LL, 14039540557039009792ULL),
    BasicDecimal128(27105054312137LL, 11268197054423236608ULL),
    BasicDecimal128(271050543121376LL, 2001506101975056384ULL),
    BasicDecimal128(2710505431213761LL, 1568316946041012224ULL),
    BasicDecimal128(27105054312137610LL, 15683169460410122240ULL),
    BasicDecimal128(271050543121376108LL, 9257742014424809472ULL),
    BasicDecimal128(2710505431213761085LL, 343699775700336640ULL)};

#if ARROW_LITTLE_ENDIAN
#define BasicDecimal256FromLE(v1, v2, v3, v4) \
  BasicDecimal256(std::array<uint64_t, 4>{v1, v2, v3, v4})
#else
#define BasicDecimal256FromLE(v1, v2, v3, v4) \
  BasicDecimal256(std::array<uint64_t, 4>{v4, v3, v2, v1})
#endif

constexpr BasicDecimal256 kDecimal256PowersOfTen[76 + 1] = {
    BasicDecimal256FromLE(1ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(10ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(100ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(1000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(10000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(100000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(1000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(10000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(100000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(1000000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(10000000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(100000000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(1000000000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(10000000000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(100000000000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(1000000000000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(10000000000000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(100000000000000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(1000000000000000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(10000000000000000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(7766279631452241920ULL, 5ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(3875820019684212736ULL, 54ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(1864712049423024128ULL, 542ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(200376420520689664ULL, 5421ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(2003764205206896640ULL, 54210ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(1590897978359414784ULL, 542101ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(15908979783594147840ULL, 5421010ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(11515845246265065472ULL, 54210108ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(4477988020393345024ULL, 542101086ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(7886392056514347008ULL, 5421010862ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(5076944270305263616ULL, 54210108624ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(13875954555633532928ULL, 542101086242ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(9632337040368467968ULL, 5421010862427ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(4089650035136921600ULL, 54210108624275ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(4003012203950112768ULL, 542101086242752ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(3136633892082024448ULL, 5421010862427522ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(12919594847110692864ULL, 54210108624275221ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(68739955140067328ULL, 542101086242752217ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(687399551400673280ULL, 5421010862427522170ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(6873995514006732800ULL, 17316620476856118468ULL, 2ULL, 0ULL),
    BasicDecimal256FromLE(13399722918938673152ULL, 7145508105175220139ULL, 29ULL, 0ULL),
    BasicDecimal256FromLE(4870020673419870208ULL, 16114848830623546549ULL, 293ULL, 0ULL),
    BasicDecimal256FromLE(11806718586779598848ULL, 13574535716559052564ULL, 2938ULL,
                          0ULL),
    BasicDecimal256FromLE(7386721425538678784ULL, 6618148649623664334ULL, 29387ULL, 0ULL),
    BasicDecimal256FromLE(80237960548581376ULL, 10841254275107988496ULL, 293873ULL, 0ULL),
    BasicDecimal256FromLE(802379605485813760ULL, 16178822382532126880ULL, 2938735ULL,
                          0ULL),
    BasicDecimal256FromLE(8023796054858137600ULL, 14214271235644855872ULL, 29387358ULL,
                          0ULL),
    BasicDecimal256FromLE(6450984253743169536ULL, 13015503840481697412ULL, 293873587ULL,
                          0ULL),
    BasicDecimal256FromLE(9169610316303040512ULL, 1027829888850112811ULL, 2938735877ULL,
                          0ULL),
    BasicDecimal256FromLE(17909126868192198656ULL, 10278298888501128114ULL,
                          29387358770ULL, 0ULL),
    BasicDecimal256FromLE(13070572018536022016ULL, 10549268516463523069ULL,
                          293873587705ULL, 0ULL),
    BasicDecimal256FromLE(1578511669393358848ULL, 13258964796087472617ULL,
                          2938735877055ULL, 0ULL),
    BasicDecimal256FromLE(15785116693933588480ULL, 3462439444907864858ULL,
                          29387358770557ULL, 0ULL),
    BasicDecimal256FromLE(10277214349659471872ULL, 16177650375369096972ULL,
                          293873587705571ULL, 0ULL),
    BasicDecimal256FromLE(10538423128046960640ULL, 14202551164014556797ULL,
                          2938735877055718ULL, 0ULL),
    BasicDecimal256FromLE(13150510911921848320ULL, 12898303124178706663ULL,
                          29387358770557187ULL, 0ULL),
    BasicDecimal256FromLE(2377900603251621888ULL, 18302566799529756941ULL,
                          293873587705571876ULL, 0ULL),
    BasicDecimal256FromLE(5332261958806667264ULL, 17004971331911604867ULL,
                          2938735877055718769ULL, 0ULL),
    BasicDecimal256FromLE(16429131440647569408ULL, 4029016655730084128ULL,
                          10940614696847636083ULL, 1ULL),
    BasicDecimal256FromLE(16717361816799281152ULL, 3396678409881738056ULL,
                          17172426599928602752ULL, 15ULL),
    BasicDecimal256FromLE(1152921504606846976ULL, 15520040025107828953ULL,
                          5703569335900062977ULL, 159ULL),
    BasicDecimal256FromLE(11529215046068469760ULL, 7626447661401876602ULL,
                          1695461137871974930ULL, 1593ULL),
    BasicDecimal256FromLE(4611686018427387904ULL, 2477500319180559562ULL,
                          16954611378719749304ULL, 15930ULL),
    BasicDecimal256FromLE(9223372036854775808ULL, 6328259118096044006ULL,
                          3525417123811528497ULL, 159309ULL),
    BasicDecimal256FromLE(0ULL, 7942358959831785217ULL, 16807427164405733357ULL,
                          1593091ULL),
    BasicDecimal256FromLE(0ULL, 5636613303479645706ULL, 2053574980671369030ULL,
                          15930919ULL),
    BasicDecimal256FromLE(0ULL, 1025900813667802212ULL, 2089005733004138687ULL,
                          159309191ULL),
    BasicDecimal256FromLE(0ULL, 10259008136678022120ULL, 2443313256331835254ULL,
                          1593091911ULL),
    BasicDecimal256FromLE(0ULL, 10356360998232463120ULL, 5986388489608800929ULL,
                          15930919111ULL),
    BasicDecimal256FromLE(0ULL, 11329889613776873120ULL, 4523652674959354447ULL,
                          159309191113ULL),
    BasicDecimal256FromLE(0ULL, 2618431695511421504ULL, 8343038602174441244ULL,
                          1593091911132ULL),
    BasicDecimal256FromLE(0ULL, 7737572881404663424ULL, 9643409726906205977ULL,
                          15930919111324ULL),
    BasicDecimal256FromLE(0ULL, 3588752519208427776ULL, 4200376900514301694ULL,
                          159309191113245ULL),
    BasicDecimal256FromLE(0ULL, 17440781118374726144ULL, 5110280857723913709ULL,
                          1593091911132452ULL),
    BasicDecimal256FromLE(0ULL, 8387114520361296896ULL, 14209320429820033867ULL,
                          15930919111324522ULL),
    BasicDecimal256FromLE(0ULL, 10084168908774762496ULL, 12965995782233477362ULL,
                          159309191113245227ULL),
    BasicDecimal256FromLE(0ULL, 8607968719199866880ULL, 532749306367912313ULL,
                          1593091911132452277ULL)};

constexpr BasicDecimal256 kDecimal256HalfPowersOfTen[] = {
    BasicDecimal256FromLE(0ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(5ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(50ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(500ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(5000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(50000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(500000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(5000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(50000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(500000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(5000000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(50000000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(500000000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(5000000000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(50000000000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(500000000000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(5000000000000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(50000000000000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(500000000000000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(5000000000000000000ULL, 0ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(13106511852580896768ULL, 2ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(1937910009842106368ULL, 27ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(932356024711512064ULL, 271ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(9323560247115120640ULL, 2710ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(1001882102603448320ULL, 27105ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(10018821026034483200ULL, 271050ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(7954489891797073920ULL, 2710505ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(5757922623132532736ULL, 27105054ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(2238994010196672512ULL, 271050543ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(3943196028257173504ULL, 2710505431ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(2538472135152631808ULL, 27105054312ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(6937977277816766464ULL, 271050543121ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(14039540557039009792ULL, 2710505431213ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(11268197054423236608ULL, 27105054312137ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(2001506101975056384ULL, 271050543121376ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(1568316946041012224ULL, 2710505431213761ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(15683169460410122240ULL, 27105054312137610ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(9257742014424809472ULL, 271050543121376108ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(343699775700336640ULL, 2710505431213761085ULL, 0ULL, 0ULL),
    BasicDecimal256FromLE(3436997757003366400ULL, 8658310238428059234ULL, 1ULL, 0ULL),
    BasicDecimal256FromLE(15923233496324112384ULL, 12796126089442385877ULL, 14ULL, 0ULL),
    BasicDecimal256FromLE(11658382373564710912ULL, 17280796452166549082ULL, 146ULL, 0ULL),
    BasicDecimal256FromLE(5903359293389799424ULL, 6787267858279526282ULL, 1469ULL, 0ULL),
    BasicDecimal256FromLE(3693360712769339392ULL, 12532446361666607975ULL, 14693ULL,
                          0ULL),
    BasicDecimal256FromLE(40118980274290688ULL, 14643999174408770056ULL, 146936ULL, 0ULL),
    BasicDecimal256FromLE(401189802742906880ULL, 17312783228120839248ULL, 1469367ULL,
                          0ULL),
    BasicDecimal256FromLE(4011898027429068800ULL, 7107135617822427936ULL, 14693679ULL,
                          0ULL),
    BasicDecimal256FromLE(3225492126871584768ULL, 15731123957095624514ULL, 146936793ULL,
                          0ULL),
    BasicDecimal256FromLE(13808177195006296064ULL, 9737286981279832213ULL, 1469367938ULL,
                          0ULL),
    BasicDecimal256FromLE(8954563434096099328ULL, 5139149444250564057ULL, 14693679385ULL,
                          0ULL),
    BasicDecimal256FromLE(15758658046122786816ULL, 14498006295086537342ULL,
                          146936793852ULL, 0ULL),
    BasicDecimal256FromLE(10012627871551455232ULL, 15852854434898512116ULL,
                          1469367938527ULL, 0ULL),
    BasicDecimal256FromLE(7892558346966794240ULL, 10954591759308708237ULL,
                          14693679385278ULL, 0ULL),
    BasicDecimal256FromLE(5138607174829735936ULL, 17312197224539324294ULL,
                          146936793852785ULL, 0ULL),
    BasicDecimal256FromLE(14492583600878256128ULL, 7101275582007278398ULL,
                          1469367938527859ULL, 0ULL),
    BasicDecimal256FromLE(15798627492815699968ULL, 15672523598944129139ULL,
                          14693679385278593ULL, 0ULL),
    BasicDecimal256FromLE(10412322338480586752ULL, 9151283399764878470ULL,
                          146936793852785938ULL, 0ULL),
    BasicDecimal256FromLE(11889503016258109440ULL, 17725857702810578241ULL,
                          1469367938527859384ULL, 0ULL),
    BasicDecimal256FromLE(8214565720323784704ULL, 11237880364719817872ULL,
                          14693679385278593849ULL, 0ULL),
    BasicDecimal256FromLE(8358680908399640576ULL, 1698339204940869028ULL,
                          17809585336819077184ULL, 7ULL),
    BasicDecimal256FromLE(9799832789158199296ULL, 16983392049408690284ULL,
                          12075156704804807296ULL, 79ULL),
    BasicDecimal256FromLE(5764607523034234880ULL, 3813223830700938301ULL,
                          10071102605790763273ULL, 796ULL),
    BasicDecimal256FromLE(2305843009213693952ULL, 1238750159590279781ULL,
                          8477305689359874652ULL, 7965ULL),
    BasicDecimal256FromLE(4611686018427387904ULL, 12387501595902797811ULL,
                          10986080598760540056ULL, 79654ULL),
    BasicDecimal256FromLE(9223372036854775808ULL, 13194551516770668416ULL,
                          17627085619057642486ULL, 796545ULL),
    BasicDecimal256FromLE(0ULL, 2818306651739822853ULL, 10250159527190460323ULL,
                          7965459ULL),
    BasicDecimal256FromLE(0ULL, 9736322443688676914ULL, 10267874903356845151ULL,
                          79654595ULL),
    BasicDecimal256FromLE(0ULL, 5129504068339011060ULL, 10445028665020693435ULL,
                          796545955ULL),
    BasicDecimal256FromLE(0ULL, 14401552535971007368ULL, 12216566281659176272ULL,
                          7965459555ULL),
    BasicDecimal256FromLE(0ULL, 14888316843743212368ULL, 11485198374334453031ULL,
                          79654595556ULL),
    BasicDecimal256FromLE(0ULL, 1309215847755710752ULL, 4171519301087220622ULL,
                          796545955566ULL),
    BasicDecimal256FromLE(0ULL, 13092158477557107520ULL, 4821704863453102988ULL,
                          7965459555662ULL),
    BasicDecimal256FromLE(0ULL, 1794376259604213888ULL, 11323560487111926655ULL,
                          79654595556622ULL),
    BasicDecimal256FromLE(0ULL, 17943762596042138880ULL, 2555140428861956854ULL,
                          796545955566226ULL),
    BasicDecimal256FromLE(0ULL, 13416929297035424256ULL, 7104660214910016933ULL,
                          7965459555662261ULL),
    BasicDecimal256FromLE(0ULL, 5042084454387381248ULL, 15706369927971514489ULL,
                          79654595556622613ULL),
    BasicDecimal256FromLE(0ULL, 13527356396454709248ULL, 9489746690038731964ULL,
                          796545955566226138ULL)};

#undef BasicDecimal256FromLE

// ceil(log2(10 ^ k)) for k in [0...76]
constexpr int kCeilLog2PowersOfTen[76 + 1] = {
    0,   4,   7,   10,  14,  17,  20,  24,  27,  30,  34,  37,  40,  44,  47,  50,
    54,  57,  60,  64,  67,  70,  74,  77,  80,  84,  87,  90,  94,  97,  100, 103,
    107, 110, 113, 117, 120, 123, 127, 130, 133, 137, 140, 143, 147, 150, 153, 157,
    160, 163, 167, 170, 173, 177, 180, 183, 187, 190, 193, 196, 200, 203, 206, 210,
    213, 216, 220, 223, 226, 230, 233, 236, 240, 243, 246, 250, 253};

template <typename Real>
struct RealTraits {};

template <>
struct RealTraits<float> {
  static constexpr const float* powers_of_ten() { return kFloatPowersOfTen; }

  static constexpr float two_to_64(float x) { return x * 1.8446744e+19f; }
  static constexpr float two_to_128(float x) { return x == 0 ? 0 : kFloatInf; }
  static constexpr float two_to_192(float x) { return x == 0 ? 0 : kFloatInf; }

  static constexpr int kMantissaBits = 24;
  // ceil(log10(2 ^ kMantissaBits))
  static constexpr int kMantissaDigits = 8;
  // Integers between zero and kMaxPreciseInteger can be precisely represented
  static constexpr uint64_t kMaxPreciseInteger = (1ULL << kMantissaBits) - 1;
};

template <>
struct RealTraits<double> {
  static constexpr const double* powers_of_ten() { return kDoublePowersOfTen; }

  static constexpr double two_to_64(double x) { return x * 1.8446744073709552e+19; }
  static constexpr double two_to_128(double x) { return x * 3.402823669209385e+38; }
  static constexpr double two_to_192(double x) { return x * 6.277101735386681e+57; }

  static constexpr int kMantissaBits = 53;
  // ceil(log10(2 ^ kMantissaBits))
  static constexpr int kMantissaDigits = 16;
  // Integers between zero and kMaxPreciseInteger can be precisely represented
  static constexpr uint64_t kMaxPreciseInteger = (1ULL << kMantissaBits) - 1;
};

template <typename DecimalType>
struct DecimalTraits {};

template <>
struct DecimalTraits<BasicDecimal128> {
  static constexpr const BasicDecimal128* powers_of_ten() {
    return kDecimal128PowersOfTen;
  }

  static constexpr int kMaxPrecision = BasicDecimal128::kMaxPrecision;
  static constexpr const char* kTypeName = "Decimal128";
};

template <>
struct DecimalTraits<BasicDecimal256> {
  static constexpr const BasicDecimal256* powers_of_ten() {
    return kDecimal256PowersOfTen;
  }

  static constexpr int kMaxPrecision = BasicDecimal128::kMaxPrecision;
  static constexpr const char* kTypeName = "Decimal256";
};

template <>
struct DecimalTraits<Decimal128> : public DecimalTraits<BasicDecimal128> {};
template <>
struct DecimalTraits<Decimal256> : public DecimalTraits<BasicDecimal256> {};

}  // namespace arrow
