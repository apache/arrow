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

#include "arrow/util/basic_decimal.h"

namespace arrow {

template<uint32_t width>
struct ScaleMultipliersAnyWidth {};

#define DECL_ANY_SCALE_MULTIPLIERS(width)                           \
template<>                                                          \
struct ScaleMultipliersAnyWidth<width> {                            \
  static const int##width##_t value[];                              \
};                                                                  \
const int##width##_t ScaleMultipliersAnyWidth<width>::value[] = {   \
  int##width##_t(1LL),                                              \
  int##width##_t(10LL),                                             \
  int##width##_t(100LL),                                            \
  int##width##_t(1000LL),                                           \
  int##width##_t(10000LL),                                          \
  int##width##_t(100000LL),                                         \
  int##width##_t(1000000LL),                                        \
  int##width##_t(10000000LL),                                       \
  int##width##_t(100000000LL),                                      \
  int##width##_t(1000000000LL),                                     \
  int##width##_t(10000000000LL),                                    \
  int##width##_t(100000000000LL),                                   \
  int##width##_t(1000000000000LL),                                  \
  int##width##_t(10000000000000LL),                                 \
  int##width##_t(100000000000000LL),                                \
  int##width##_t(1000000000000000LL),                               \
  int##width##_t(10000000000000000LL),                              \
  int##width##_t(100000000000000000LL),                             \
  int##width##_t(1000000000000000000LL)                             \
}; 

DECL_ANY_SCALE_MULTIPLIERS(16)
DECL_ANY_SCALE_MULTIPLIERS(32)
DECL_ANY_SCALE_MULTIPLIERS(64)

#undef DECL_ANY_SCALE_MULTIPLIERS

static const BasicDecimal128 ScaleMultipliers128[] = {
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


static const BasicDecimal128 ScaleMultipliersHalf128[] = {
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

} // namespace arrow
