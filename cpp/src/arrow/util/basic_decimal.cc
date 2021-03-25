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

#include "arrow/util/basic_decimal.h"

#include <algorithm>
#include <array>
#include <climits>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <limits>
#include <string>

#include "arrow/util/bit_util.h"
#include "arrow/util/endian.h"
#include "arrow/util/int128_internal.h"
#include "arrow/util/int_util_internal.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {

using internal::SafeLeftShift;
using internal::SafeSignedAdd;

static const BasicDecimal128 ScaleMultipliers[] = {
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

static const BasicDecimal128 ScaleMultipliersHalf[] = {
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

static const BasicDecimal256 ScaleMultipliersDecimal256[] = {
    BasicDecimal256({1ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({10ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({100ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({1000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({10000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({100000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({1000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({10000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({100000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({1000000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({10000000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({100000000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({1000000000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({10000000000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({100000000000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({1000000000000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({10000000000000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({100000000000000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({1000000000000000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({10000000000000000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({7766279631452241920ULL, 5ULL, 0ULL, 0ULL}),
    BasicDecimal256({3875820019684212736ULL, 54ULL, 0ULL, 0ULL}),
    BasicDecimal256({1864712049423024128ULL, 542ULL, 0ULL, 0ULL}),
    BasicDecimal256({200376420520689664ULL, 5421ULL, 0ULL, 0ULL}),
    BasicDecimal256({2003764205206896640ULL, 54210ULL, 0ULL, 0ULL}),
    BasicDecimal256({1590897978359414784ULL, 542101ULL, 0ULL, 0ULL}),
    BasicDecimal256({15908979783594147840ULL, 5421010ULL, 0ULL, 0ULL}),
    BasicDecimal256({11515845246265065472ULL, 54210108ULL, 0ULL, 0ULL}),
    BasicDecimal256({4477988020393345024ULL, 542101086ULL, 0ULL, 0ULL}),
    BasicDecimal256({7886392056514347008ULL, 5421010862ULL, 0ULL, 0ULL}),
    BasicDecimal256({5076944270305263616ULL, 54210108624ULL, 0ULL, 0ULL}),
    BasicDecimal256({13875954555633532928ULL, 542101086242ULL, 0ULL, 0ULL}),
    BasicDecimal256({9632337040368467968ULL, 5421010862427ULL, 0ULL, 0ULL}),
    BasicDecimal256({4089650035136921600ULL, 54210108624275ULL, 0ULL, 0ULL}),
    BasicDecimal256({4003012203950112768ULL, 542101086242752ULL, 0ULL, 0ULL}),
    BasicDecimal256({3136633892082024448ULL, 5421010862427522ULL, 0ULL, 0ULL}),
    BasicDecimal256({12919594847110692864ULL, 54210108624275221ULL, 0ULL, 0ULL}),
    BasicDecimal256({68739955140067328ULL, 542101086242752217ULL, 0ULL, 0ULL}),
    BasicDecimal256({687399551400673280ULL, 5421010862427522170ULL, 0ULL, 0ULL}),
    BasicDecimal256({6873995514006732800ULL, 17316620476856118468ULL, 2ULL, 0ULL}),
    BasicDecimal256({13399722918938673152ULL, 7145508105175220139ULL, 29ULL, 0ULL}),
    BasicDecimal256({4870020673419870208ULL, 16114848830623546549ULL, 293ULL, 0ULL}),
    BasicDecimal256({11806718586779598848ULL, 13574535716559052564ULL, 2938ULL, 0ULL}),
    BasicDecimal256({7386721425538678784ULL, 6618148649623664334ULL, 29387ULL, 0ULL}),
    BasicDecimal256({80237960548581376ULL, 10841254275107988496ULL, 293873ULL, 0ULL}),
    BasicDecimal256({802379605485813760ULL, 16178822382532126880ULL, 2938735ULL, 0ULL}),
    BasicDecimal256({8023796054858137600ULL, 14214271235644855872ULL, 29387358ULL, 0ULL}),
    BasicDecimal256(
        {6450984253743169536ULL, 13015503840481697412ULL, 293873587ULL, 0ULL}),
    BasicDecimal256(
        {9169610316303040512ULL, 1027829888850112811ULL, 2938735877ULL, 0ULL}),
    BasicDecimal256(
        {17909126868192198656ULL, 10278298888501128114ULL, 29387358770ULL, 0ULL}),
    BasicDecimal256(
        {13070572018536022016ULL, 10549268516463523069ULL, 293873587705ULL, 0ULL}),
    BasicDecimal256(
        {1578511669393358848ULL, 13258964796087472617ULL, 2938735877055ULL, 0ULL}),
    BasicDecimal256(
        {15785116693933588480ULL, 3462439444907864858ULL, 29387358770557ULL, 0ULL}),
    BasicDecimal256(
        {10277214349659471872ULL, 16177650375369096972ULL, 293873587705571ULL, 0ULL}),
    BasicDecimal256(
        {10538423128046960640ULL, 14202551164014556797ULL, 2938735877055718ULL, 0ULL}),
    BasicDecimal256(
        {13150510911921848320ULL, 12898303124178706663ULL, 29387358770557187ULL, 0ULL}),
    BasicDecimal256(
        {2377900603251621888ULL, 18302566799529756941ULL, 293873587705571876ULL, 0ULL}),
    BasicDecimal256(
        {5332261958806667264ULL, 17004971331911604867ULL, 2938735877055718769ULL, 0ULL}),
    BasicDecimal256(
        {16429131440647569408ULL, 4029016655730084128ULL, 10940614696847636083ULL, 1ULL}),
    BasicDecimal256({16717361816799281152ULL, 3396678409881738056ULL,
                     17172426599928602752ULL, 15ULL}),
    BasicDecimal256({1152921504606846976ULL, 15520040025107828953ULL,
                     5703569335900062977ULL, 159ULL}),
    BasicDecimal256({11529215046068469760ULL, 7626447661401876602ULL,
                     1695461137871974930ULL, 1593ULL}),
    BasicDecimal256({4611686018427387904ULL, 2477500319180559562ULL,
                     16954611378719749304ULL, 15930ULL}),
    BasicDecimal256({9223372036854775808ULL, 6328259118096044006ULL,
                     3525417123811528497ULL, 159309ULL}),
    BasicDecimal256({0ULL, 7942358959831785217ULL, 16807427164405733357ULL, 1593091ULL}),
    BasicDecimal256({0ULL, 5636613303479645706ULL, 2053574980671369030ULL, 15930919ULL}),
    BasicDecimal256({0ULL, 1025900813667802212ULL, 2089005733004138687ULL, 159309191ULL}),
    BasicDecimal256(
        {0ULL, 10259008136678022120ULL, 2443313256331835254ULL, 1593091911ULL}),
    BasicDecimal256(
        {0ULL, 10356360998232463120ULL, 5986388489608800929ULL, 15930919111ULL}),
    BasicDecimal256(
        {0ULL, 11329889613776873120ULL, 4523652674959354447ULL, 159309191113ULL}),
    BasicDecimal256(
        {0ULL, 2618431695511421504ULL, 8343038602174441244ULL, 1593091911132ULL}),
    BasicDecimal256(
        {0ULL, 7737572881404663424ULL, 9643409726906205977ULL, 15930919111324ULL}),
    BasicDecimal256(
        {0ULL, 3588752519208427776ULL, 4200376900514301694ULL, 159309191113245ULL}),
    BasicDecimal256(
        {0ULL, 17440781118374726144ULL, 5110280857723913709ULL, 1593091911132452ULL}),
    BasicDecimal256(
        {0ULL, 8387114520361296896ULL, 14209320429820033867ULL, 15930919111324522ULL}),
    BasicDecimal256(
        {0ULL, 10084168908774762496ULL, 12965995782233477362ULL, 159309191113245227ULL}),
    BasicDecimal256(
        {0ULL, 8607968719199866880ULL, 532749306367912313ULL, 1593091911132452277ULL})};

static const BasicDecimal256 ScaleMultipliersHalfDecimal256[] = {
    BasicDecimal256({0ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({5ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({50ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({500ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({5000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({50000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({500000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({5000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({50000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({500000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({5000000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({50000000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({500000000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({5000000000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({50000000000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({500000000000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({5000000000000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({50000000000000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({500000000000000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({5000000000000000000ULL, 0ULL, 0ULL, 0ULL}),
    BasicDecimal256({13106511852580896768ULL, 2ULL, 0ULL, 0ULL}),
    BasicDecimal256({1937910009842106368ULL, 27ULL, 0ULL, 0ULL}),
    BasicDecimal256({932356024711512064ULL, 271ULL, 0ULL, 0ULL}),
    BasicDecimal256({9323560247115120640ULL, 2710ULL, 0ULL, 0ULL}),
    BasicDecimal256({1001882102603448320ULL, 27105ULL, 0ULL, 0ULL}),
    BasicDecimal256({10018821026034483200ULL, 271050ULL, 0ULL, 0ULL}),
    BasicDecimal256({7954489891797073920ULL, 2710505ULL, 0ULL, 0ULL}),
    BasicDecimal256({5757922623132532736ULL, 27105054ULL, 0ULL, 0ULL}),
    BasicDecimal256({2238994010196672512ULL, 271050543ULL, 0ULL, 0ULL}),
    BasicDecimal256({3943196028257173504ULL, 2710505431ULL, 0ULL, 0ULL}),
    BasicDecimal256({2538472135152631808ULL, 27105054312ULL, 0ULL, 0ULL}),
    BasicDecimal256({6937977277816766464ULL, 271050543121ULL, 0ULL, 0ULL}),
    BasicDecimal256({14039540557039009792ULL, 2710505431213ULL, 0ULL, 0ULL}),
    BasicDecimal256({11268197054423236608ULL, 27105054312137ULL, 0ULL, 0ULL}),
    BasicDecimal256({2001506101975056384ULL, 271050543121376ULL, 0ULL, 0ULL}),
    BasicDecimal256({1568316946041012224ULL, 2710505431213761ULL, 0ULL, 0ULL}),
    BasicDecimal256({15683169460410122240ULL, 27105054312137610ULL, 0ULL, 0ULL}),
    BasicDecimal256({9257742014424809472ULL, 271050543121376108ULL, 0ULL, 0ULL}),
    BasicDecimal256({343699775700336640ULL, 2710505431213761085ULL, 0ULL, 0ULL}),
    BasicDecimal256({3436997757003366400ULL, 8658310238428059234ULL, 1ULL, 0ULL}),
    BasicDecimal256({15923233496324112384ULL, 12796126089442385877ULL, 14ULL, 0ULL}),
    BasicDecimal256({11658382373564710912ULL, 17280796452166549082ULL, 146ULL, 0ULL}),
    BasicDecimal256({5903359293389799424ULL, 6787267858279526282ULL, 1469ULL, 0ULL}),
    BasicDecimal256({3693360712769339392ULL, 12532446361666607975ULL, 14693ULL, 0ULL}),
    BasicDecimal256({40118980274290688ULL, 14643999174408770056ULL, 146936ULL, 0ULL}),
    BasicDecimal256({401189802742906880ULL, 17312783228120839248ULL, 1469367ULL, 0ULL}),
    BasicDecimal256({4011898027429068800ULL, 7107135617822427936ULL, 14693679ULL, 0ULL}),
    BasicDecimal256(
        {3225492126871584768ULL, 15731123957095624514ULL, 146936793ULL, 0ULL}),
    BasicDecimal256(
        {13808177195006296064ULL, 9737286981279832213ULL, 1469367938ULL, 0ULL}),
    BasicDecimal256(
        {8954563434096099328ULL, 5139149444250564057ULL, 14693679385ULL, 0ULL}),
    BasicDecimal256(
        {15758658046122786816ULL, 14498006295086537342ULL, 146936793852ULL, 0ULL}),
    BasicDecimal256(
        {10012627871551455232ULL, 15852854434898512116ULL, 1469367938527ULL, 0ULL}),
    BasicDecimal256(
        {7892558346966794240ULL, 10954591759308708237ULL, 14693679385278ULL, 0ULL}),
    BasicDecimal256(
        {5138607174829735936ULL, 17312197224539324294ULL, 146936793852785ULL, 0ULL}),
    BasicDecimal256(
        {14492583600878256128ULL, 7101275582007278398ULL, 1469367938527859ULL, 0ULL}),
    BasicDecimal256(
        {15798627492815699968ULL, 15672523598944129139ULL, 14693679385278593ULL, 0ULL}),
    BasicDecimal256(
        {10412322338480586752ULL, 9151283399764878470ULL, 146936793852785938ULL, 0ULL}),
    BasicDecimal256(
        {11889503016258109440ULL, 17725857702810578241ULL, 1469367938527859384ULL, 0ULL}),
    BasicDecimal256(
        {8214565720323784704ULL, 11237880364719817872ULL, 14693679385278593849ULL, 0ULL}),
    BasicDecimal256(
        {8358680908399640576ULL, 1698339204940869028ULL, 17809585336819077184ULL, 7ULL}),
    BasicDecimal256({9799832789158199296ULL, 16983392049408690284ULL,
                     12075156704804807296ULL, 79ULL}),
    BasicDecimal256({5764607523034234880ULL, 3813223830700938301ULL,
                     10071102605790763273ULL, 796ULL}),
    BasicDecimal256({2305843009213693952ULL, 1238750159590279781ULL,
                     8477305689359874652ULL, 7965ULL}),
    BasicDecimal256({4611686018427387904ULL, 12387501595902797811ULL,
                     10986080598760540056ULL, 79654ULL}),
    BasicDecimal256({9223372036854775808ULL, 13194551516770668416ULL,
                     17627085619057642486ULL, 796545ULL}),
    BasicDecimal256({0ULL, 2818306651739822853ULL, 10250159527190460323ULL, 7965459ULL}),
    BasicDecimal256({0ULL, 9736322443688676914ULL, 10267874903356845151ULL, 79654595ULL}),
    BasicDecimal256(
        {0ULL, 5129504068339011060ULL, 10445028665020693435ULL, 796545955ULL}),
    BasicDecimal256(
        {0ULL, 14401552535971007368ULL, 12216566281659176272ULL, 7965459555ULL}),
    BasicDecimal256(
        {0ULL, 14888316843743212368ULL, 11485198374334453031ULL, 79654595556ULL}),
    BasicDecimal256(
        {0ULL, 1309215847755710752ULL, 4171519301087220622ULL, 796545955566ULL}),
    BasicDecimal256(
        {0ULL, 13092158477557107520ULL, 4821704863453102988ULL, 7965459555662ULL}),
    BasicDecimal256(
        {0ULL, 1794376259604213888ULL, 11323560487111926655ULL, 79654595556622ULL}),
    BasicDecimal256(
        {0ULL, 17943762596042138880ULL, 2555140428861956854ULL, 796545955566226ULL}),
    BasicDecimal256(
        {0ULL, 13416929297035424256ULL, 7104660214910016933ULL, 7965459555662261ULL}),
    BasicDecimal256(
        {0ULL, 5042084454387381248ULL, 15706369927971514489ULL, 79654595556622613ULL}),
    BasicDecimal256(
        {0ULL, 13527356396454709248ULL, 9489746690038731964ULL, 796545955566226138ULL})};

#ifdef ARROW_USE_NATIVE_INT128
static constexpr uint64_t kInt64Mask = 0xFFFFFFFFFFFFFFFF;
#else
static constexpr uint64_t kInt32Mask = 0xFFFFFFFF;
#endif

// same as ScaleMultipliers[38] - 1
static constexpr BasicDecimal128 kMaxValue =
    BasicDecimal128(5421010862427522170LL, 687399551400673280ULL - 1);

#if ARROW_LITTLE_ENDIAN
BasicDecimal128::BasicDecimal128(const uint8_t* bytes)
    : BasicDecimal128(reinterpret_cast<const int64_t*>(bytes)[1],
                      reinterpret_cast<const uint64_t*>(bytes)[0]) {}
#else
BasicDecimal128::BasicDecimal128(const uint8_t* bytes)
    : BasicDecimal128(reinterpret_cast<const int64_t*>(bytes)[0],
                      reinterpret_cast<const uint64_t*>(bytes)[1]) {}
#endif

std::array<uint8_t, 16> BasicDecimal128::ToBytes() const {
  std::array<uint8_t, 16> out{{0}};
  ToBytes(out.data());
  return out;
}

void BasicDecimal128::ToBytes(uint8_t* out) const {
  DCHECK_NE(out, nullptr);
#if ARROW_LITTLE_ENDIAN
  reinterpret_cast<uint64_t*>(out)[0] = low_bits_;
  reinterpret_cast<int64_t*>(out)[1] = high_bits_;
#else
  reinterpret_cast<int64_t*>(out)[0] = high_bits_;
  reinterpret_cast<uint64_t*>(out)[1] = low_bits_;
#endif
}

BasicDecimal128& BasicDecimal128::Negate() {
  low_bits_ = ~low_bits_ + 1;
  high_bits_ = ~high_bits_;
  if (low_bits_ == 0) {
    high_bits_ = SafeSignedAdd<int64_t>(high_bits_, 1);
  }
  return *this;
}

BasicDecimal128& BasicDecimal128::Abs() { return *this < 0 ? Negate() : *this; }

BasicDecimal128 BasicDecimal128::Abs(const BasicDecimal128& in) {
  BasicDecimal128 result(in);
  return result.Abs();
}

bool BasicDecimal128::FitsInPrecision(int32_t precision) const {
  DCHECK_GT(precision, 0);
  DCHECK_LE(precision, 38);
  return BasicDecimal128::Abs(*this) < ScaleMultipliers[precision];
}

BasicDecimal128& BasicDecimal128::operator+=(const BasicDecimal128& right) {
  const uint64_t sum = low_bits_ + right.low_bits_;
  high_bits_ = SafeSignedAdd<int64_t>(high_bits_, right.high_bits_);
  if (sum < low_bits_) {
    high_bits_ = SafeSignedAdd<int64_t>(high_bits_, 1);
  }
  low_bits_ = sum;
  return *this;
}

BasicDecimal128& BasicDecimal128::operator-=(const BasicDecimal128& right) {
  const uint64_t diff = low_bits_ - right.low_bits_;
  high_bits_ -= right.high_bits_;
  if (diff > low_bits_) {
    --high_bits_;
  }
  low_bits_ = diff;
  return *this;
}

BasicDecimal128& BasicDecimal128::operator/=(const BasicDecimal128& right) {
  BasicDecimal128 remainder;
  auto s = Divide(right, this, &remainder);
  DCHECK_EQ(s, DecimalStatus::kSuccess);
  return *this;
}

BasicDecimal128& BasicDecimal128::operator|=(const BasicDecimal128& right) {
  low_bits_ |= right.low_bits_;
  high_bits_ |= right.high_bits_;
  return *this;
}

BasicDecimal128& BasicDecimal128::operator&=(const BasicDecimal128& right) {
  low_bits_ &= right.low_bits_;
  high_bits_ &= right.high_bits_;
  return *this;
}

BasicDecimal128& BasicDecimal128::operator<<=(uint32_t bits) {
  if (bits != 0) {
    if (bits < 64) {
      high_bits_ = SafeLeftShift(high_bits_, bits);
      high_bits_ |= (low_bits_ >> (64 - bits));
      low_bits_ <<= bits;
    } else if (bits < 128) {
      high_bits_ = static_cast<int64_t>(low_bits_) << (bits - 64);
      low_bits_ = 0;
    } else {
      high_bits_ = 0;
      low_bits_ = 0;
    }
  }
  return *this;
}

BasicDecimal128& BasicDecimal128::operator>>=(uint32_t bits) {
  if (bits != 0) {
    if (bits < 64) {
      low_bits_ >>= bits;
      low_bits_ |= static_cast<uint64_t>(high_bits_ << (64 - bits));
      high_bits_ = static_cast<int64_t>(static_cast<uint64_t>(high_bits_) >> bits);
    } else if (bits < 128) {
      low_bits_ = static_cast<uint64_t>(high_bits_ >> (bits - 64));
      high_bits_ = static_cast<int64_t>(high_bits_ >= 0L ? 0L : -1L);
    } else {
      high_bits_ = static_cast<int64_t>(high_bits_ >= 0L ? 0L : -1L);
      low_bits_ = static_cast<uint64_t>(high_bits_);
    }
  }
  return *this;
}

namespace {

// Convenience wrapper type over 128 bit unsigned integers. We opt not to
// replace the uint128_t type in int128_internal.h because it would require
// significantly more implementation work to be done. This class merely
// provides the minimum necessary set of functions to perform 128+ bit
// multiplication operations when there may or may not be native support.
#ifdef ARROW_USE_NATIVE_INT128
struct uint128_t {
  uint128_t() {}
  uint128_t(uint64_t hi, uint64_t lo) : val_((static_cast<__uint128_t>(hi) << 64) | lo) {}
  explicit uint128_t(const BasicDecimal128& decimal) {
    val_ = (static_cast<__uint128_t>(decimal.high_bits()) << 64) | decimal.low_bits();
  }

  explicit uint128_t(uint64_t value) : val_(value) {}

  uint64_t hi() { return val_ >> 64; }
  uint64_t lo() { return val_ & kInt64Mask; }

  uint128_t& operator+=(const uint128_t& other) {
    val_ += other.val_;
    return *this;
  }

  uint128_t& operator*=(const uint128_t& other) {
    val_ *= other.val_;
    return *this;
  }

  __uint128_t val_;
};

#else
// Multiply two 64 bit word components into a 128 bit result, with high bits
// stored in hi and low bits in lo.
inline void ExtendAndMultiply(uint64_t x, uint64_t y, uint64_t* hi, uint64_t* lo) {
  // Perform multiplication on two 64 bit words x and y into a 128 bit result
  // by splitting up x and y into 32 bit high/low bit components,
  // allowing us to represent the multiplication as
  // x * y = x_lo * y_lo + x_hi * y_lo * 2^32 + y_hi * x_lo * 2^32
  // + x_hi * y_hi * 2^64
  //
  // Now, consider the final output as lo_lo || lo_hi || hi_lo || hi_hi
  // Therefore,
  // lo_lo is (x_lo * y_lo)_lo,
  // lo_hi is ((x_lo * y_lo)_hi + (x_hi * y_lo)_lo + (x_lo * y_hi)_lo)_lo,
  // hi_lo is ((x_hi * y_hi)_lo + (x_hi * y_lo)_hi + (x_lo * y_hi)_hi)_hi,
  // hi_hi is (x_hi * y_hi)_hi
  const uint64_t x_lo = x & kInt32Mask;
  const uint64_t y_lo = y & kInt32Mask;
  const uint64_t x_hi = x >> 32;
  const uint64_t y_hi = y >> 32;

  const uint64_t t = x_lo * y_lo;
  const uint64_t t_lo = t & kInt32Mask;
  const uint64_t t_hi = t >> 32;

  const uint64_t u = x_hi * y_lo + t_hi;
  const uint64_t u_lo = u & kInt32Mask;
  const uint64_t u_hi = u >> 32;

  const uint64_t v = x_lo * y_hi + u_lo;
  const uint64_t v_hi = v >> 32;

  *hi = x_hi * y_hi + u_hi + v_hi;
  *lo = (v << 32) + t_lo;
}

struct uint128_t {
  uint128_t() {}
  uint128_t(uint64_t hi, uint64_t lo) : hi_(hi), lo_(lo) {}
  explicit uint128_t(const BasicDecimal128& decimal) {
    hi_ = decimal.high_bits();
    lo_ = decimal.low_bits();
  }

  uint64_t hi() const { return hi_; }
  uint64_t lo() const { return lo_; }

  uint128_t& operator+=(const uint128_t& other) {
    // To deduce the carry bit, we perform "65 bit" addition on the low bits and
    // seeing if the resulting high bit is 1. This is accomplished by shifting the
    // low bits to the right by 1 (chopping off the lowest bit), then adding 1 if the
    // result of adding the two chopped bits would have produced a carry.
    uint64_t carry = (((lo_ & other.lo_) & 1) + (lo_ >> 1) + (other.lo_ >> 1)) >> 63;
    hi_ += other.hi_ + carry;
    lo_ += other.lo_;
    return *this;
  }

  uint128_t& operator*=(const uint128_t& other) {
    uint128_t r;
    ExtendAndMultiply(lo_, other.lo_, &r.hi_, &r.lo_);
    r.hi_ += (hi_ * other.lo_) + (lo_ * other.hi_);
    *this = r;
    return *this;
  }

  uint64_t hi_;
  uint64_t lo_;
};
#endif

// Multiplies two N * 64 bit unsigned integer types, represented by a uint64_t
// array into a same sized output. Elements in the array should be in
// little endian order, and output will be the same. Overflow in multiplication
// will result in the lower N * 64 bits of the result being set.
template <int N>
inline void MultiplyUnsignedArray(const std::array<uint64_t, N>& lh,
                                  const std::array<uint64_t, N>& rh,
                                  std::array<uint64_t, N>* result) {
  for (int j = 0; j < N; ++j) {
    uint64_t carry = 0;
    for (int i = 0; i < N - j; ++i) {
      uint128_t tmp(lh[i]);
      tmp *= uint128_t(rh[j]);
      tmp += uint128_t((*result)[i + j]);
      tmp += uint128_t(carry);
      (*result)[i + j] = tmp.lo();
      carry = tmp.hi();
    }
  }
}

}  // namespace

BasicDecimal128& BasicDecimal128::operator*=(const BasicDecimal128& right) {
  // Since the max value of BasicDecimal128 is supposed to be 1e38 - 1 and the
  // min the negation taking the absolute values here should always be safe.
  const bool negate = Sign() != right.Sign();
  BasicDecimal128 x = BasicDecimal128::Abs(*this);
  BasicDecimal128 y = BasicDecimal128::Abs(right);
  uint128_t r(x);
  r *= uint128_t{y};
  high_bits_ = r.hi();
  low_bits_ = r.lo();
  if (negate) {
    Negate();
  }
  return *this;
}

/// Expands the given little endian array of uint64_t into a big endian array of
/// uint32_t. The value of input array is expected to be non-negative. The result_array
/// will remove leading zeros from the input array.
/// \param value_array a little endian array to represent the value
/// \param result_array a big endian array of length N*2 to set with the value
/// \result the output length of the array
template <size_t N>
static int64_t FillInArray(const std::array<uint64_t, N>& value_array,
                           uint32_t* result_array) {
  int64_t next_index = 0;
  // 1st loop to find out 1st non-negative value in input
  int64_t i = N - 1;
  for (; i >= 0; i--) {
    if (value_array[i] != 0) {
      if (value_array[i] <= std::numeric_limits<uint32_t>::max()) {
        result_array[next_index++] = static_cast<uint32_t>(value_array[i]);
        i--;
      }
      break;
    }
  }
  // 2nd loop to fill in the rest of the array.
  for (int64_t j = i; j >= 0; j--) {
    result_array[next_index++] = static_cast<uint32_t>(value_array[j] >> 32);
    result_array[next_index++] = static_cast<uint32_t>(value_array[j]);
  }
  return next_index;
}

/// Expands the given value into a big endian array of ints so that we can work on
/// it. The array will be converted to an absolute value and the was_negative
/// flag will be set appropriately. The array will remove leading zeros from
/// the value.
/// \param array a big endian array of length 4 to set with the value
/// \param was_negative a flag for whether the value was original negative
/// \result the output length of the array
static int64_t FillInArray(const BasicDecimal128& value, uint32_t* array,
                           bool& was_negative) {
  BasicDecimal128 abs_value = BasicDecimal128::Abs(value);
  was_negative = value.high_bits() < 0;
  uint64_t high = static_cast<uint64_t>(abs_value.high_bits());
  uint64_t low = abs_value.low_bits();

  // FillInArray(std::array<uint64_t, N>& value_array, uint32_t* result_array) is not
  // called here as the following code has better performance, to avoid regression on
  // BasicDecimal128 Division.
  if (high != 0) {
    if (high > std::numeric_limits<uint32_t>::max()) {
      array[0] = static_cast<uint32_t>(high >> 32);
      array[1] = static_cast<uint32_t>(high);
      array[2] = static_cast<uint32_t>(low >> 32);
      array[3] = static_cast<uint32_t>(low);
      return 4;
    }

    array[0] = static_cast<uint32_t>(high);
    array[1] = static_cast<uint32_t>(low >> 32);
    array[2] = static_cast<uint32_t>(low);
    return 3;
  }

  if (low > std::numeric_limits<uint32_t>::max()) {
    array[0] = static_cast<uint32_t>(low >> 32);
    array[1] = static_cast<uint32_t>(low);
    return 2;
  }

  if (low == 0) {
    return 0;
  }

  array[0] = static_cast<uint32_t>(low);
  return 1;
}

/// Expands the given value into a big endian array of ints so that we can work on
/// it. The array will be converted to an absolute value and the was_negative
/// flag will be set appropriately. The array will remove leading zeros from
/// the value.
/// \param array a big endian array of length 8 to set with the value
/// \param was_negative a flag for whether the value was original negative
/// \result the output length of the array
static int64_t FillInArray(const BasicDecimal256& value, uint32_t* array,
                           bool& was_negative) {
  BasicDecimal256 positive_value = value;
  was_negative = false;
  if (positive_value.IsNegative()) {
    positive_value.Negate();
    was_negative = true;
  }
  return FillInArray<4>(positive_value.little_endian_array(), array);
}

/// Shift the number in the array left by bits positions.
/// \param array the number to shift, must have length elements
/// \param length the number of entries in the array
/// \param bits the number of bits to shift (0 <= bits < 32)
static void ShiftArrayLeft(uint32_t* array, int64_t length, int64_t bits) {
  if (length > 0 && bits != 0) {
    for (int64_t i = 0; i < length - 1; ++i) {
      array[i] = (array[i] << bits) | (array[i + 1] >> (32 - bits));
    }
    array[length - 1] <<= bits;
  }
}

/// Shift the number in the array right by bits positions.
/// \param array the number to shift, must have length elements
/// \param length the number of entries in the array
/// \param bits the number of bits to shift (0 <= bits < 32)
static inline void ShiftArrayRight(uint32_t* array, int64_t length, int64_t bits) {
  if (length > 0 && bits != 0) {
    for (int64_t i = length - 1; i > 0; --i) {
      array[i] = (array[i] >> bits) | (array[i - 1] << (32 - bits));
    }
    array[0] >>= bits;
  }
}

/// \brief Fix the signs of the result and remainder at the end of the division based on
/// the signs of the dividend and divisor.
template <class DecimalClass>
static inline void FixDivisionSigns(DecimalClass* result, DecimalClass* remainder,
                                    bool dividend_was_negative,
                                    bool divisor_was_negative) {
  if (dividend_was_negative != divisor_was_negative) {
    result->Negate();
  }

  if (dividend_was_negative) {
    remainder->Negate();
  }
}

/// \brief Build a little endian array of uint64_t from a big endian array of uint32_t.
template <size_t N>
static DecimalStatus BuildFromArray(std::array<uint64_t, N>* result_array,
                                    const uint32_t* array, int64_t length) {
  for (int64_t i = length - 2 * N - 1; i >= 0; i--) {
    if (array[i] != 0) {
      return DecimalStatus::kOverflow;
    }
  }
  int64_t next_index = length - 1;
  size_t i = 0;
  for (; i < N && next_index >= 0; i++) {
    uint64_t lower_bits = array[next_index--];
    (*result_array)[i] =
        (next_index < 0)
            ? lower_bits
            : ((static_cast<uint64_t>(array[next_index--]) << 32) + lower_bits);
  }
  for (; i < N; i++) {
    (*result_array)[i] = 0;
  }
  return DecimalStatus::kSuccess;
}

/// \brief Build a BasicDecimal128 from a big endian array of uint32_t.
static DecimalStatus BuildFromArray(BasicDecimal128* value, const uint32_t* array,
                                    int64_t length) {
  std::array<uint64_t, 2> result_array;
  auto status = BuildFromArray(&result_array, array, length);
  if (status != DecimalStatus::kSuccess) {
    return status;
  }
  *value = {static_cast<int64_t>(result_array[1]), result_array[0]};
  return DecimalStatus::kSuccess;
}

/// \brief Build a BasicDecimal256 from a big endian array of uint32_t.
static DecimalStatus BuildFromArray(BasicDecimal256* value, const uint32_t* array,
                                    int64_t length) {
  std::array<uint64_t, 4> result_array;
  auto status = BuildFromArray(&result_array, array, length);
  if (status != DecimalStatus::kSuccess) {
    return status;
  }
  *value = result_array;
  return DecimalStatus::kSuccess;
}

/// \brief Do a division where the divisor fits into a single 32 bit value.
template <class DecimalClass>
static inline DecimalStatus SingleDivide(const uint32_t* dividend,
                                         int64_t dividend_length, uint32_t divisor,
                                         DecimalClass* remainder,
                                         bool dividend_was_negative,
                                         bool divisor_was_negative,
                                         DecimalClass* result) {
  uint64_t r = 0;
  constexpr int64_t kDecimalArrayLength = DecimalClass::bit_width / sizeof(uint32_t) + 1;
  uint32_t result_array[kDecimalArrayLength];
  for (int64_t j = 0; j < dividend_length; j++) {
    r <<= 32;
    r += dividend[j];
    result_array[j] = static_cast<uint32_t>(r / divisor);
    r %= divisor;
  }
  auto status = BuildFromArray(result, result_array, dividend_length);
  if (status != DecimalStatus::kSuccess) {
    return status;
  }

  *remainder = static_cast<int64_t>(r);
  FixDivisionSigns(result, remainder, dividend_was_negative, divisor_was_negative);
  return DecimalStatus::kSuccess;
}

/// \brief Do a decimal division with remainder.
template <class DecimalClass>
static inline DecimalStatus DecimalDivide(const DecimalClass& dividend,
                                          const DecimalClass& divisor,
                                          DecimalClass* result, DecimalClass* remainder) {
  constexpr int64_t kDecimalArrayLength = DecimalClass::bit_width / sizeof(uint32_t);
  // Split the dividend and divisor into integer pieces so that we can
  // work on them.
  uint32_t dividend_array[kDecimalArrayLength + 1];
  uint32_t divisor_array[kDecimalArrayLength];
  bool dividend_was_negative;
  bool divisor_was_negative;
  // leave an extra zero before the dividend
  dividend_array[0] = 0;
  int64_t dividend_length =
      FillInArray(dividend, dividend_array + 1, dividend_was_negative) + 1;
  int64_t divisor_length = FillInArray(divisor, divisor_array, divisor_was_negative);

  // Handle some of the easy cases.
  if (dividend_length <= divisor_length) {
    *remainder = dividend;
    *result = 0;
    return DecimalStatus::kSuccess;
  }

  if (divisor_length == 0) {
    return DecimalStatus::kDivideByZero;
  }

  if (divisor_length == 1) {
    return SingleDivide(dividend_array, dividend_length, divisor_array[0], remainder,
                        dividend_was_negative, divisor_was_negative, result);
  }

  int64_t result_length = dividend_length - divisor_length;
  uint32_t result_array[kDecimalArrayLength];
  DCHECK_LE(result_length, kDecimalArrayLength);

  // Normalize by shifting both by a multiple of 2 so that
  // the digit guessing is better. The requirement is that
  // divisor_array[0] is greater than 2**31.
  int64_t normalize_bits = BitUtil::CountLeadingZeros(divisor_array[0]);
  ShiftArrayLeft(divisor_array, divisor_length, normalize_bits);
  ShiftArrayLeft(dividend_array, dividend_length, normalize_bits);

  // compute each digit in the result
  for (int64_t j = 0; j < result_length; ++j) {
    // Guess the next digit. At worst it is two too large
    uint32_t guess = std::numeric_limits<uint32_t>::max();
    const auto high_dividend =
        static_cast<uint64_t>(dividend_array[j]) << 32 | dividend_array[j + 1];
    if (dividend_array[j] != divisor_array[0]) {
      guess = static_cast<uint32_t>(high_dividend / divisor_array[0]);
    }

    // catch all of the cases where guess is two too large and most of the
    // cases where it is one too large
    auto rhat = static_cast<uint32_t>(high_dividend -
                                      guess * static_cast<uint64_t>(divisor_array[0]));
    while (static_cast<uint64_t>(divisor_array[1]) * guess >
           (static_cast<uint64_t>(rhat) << 32) + dividend_array[j + 2]) {
      --guess;
      rhat += divisor_array[0];
      if (static_cast<uint64_t>(rhat) < divisor_array[0]) {
        break;
      }
    }

    // subtract off the guess * divisor from the dividend
    uint64_t mult = 0;
    for (int64_t i = divisor_length - 1; i >= 0; --i) {
      mult += static_cast<uint64_t>(guess) * divisor_array[i];
      uint32_t prev = dividend_array[j + i + 1];
      dividend_array[j + i + 1] -= static_cast<uint32_t>(mult);
      mult >>= 32;
      if (dividend_array[j + i + 1] > prev) {
        ++mult;
      }
    }
    uint32_t prev = dividend_array[j];
    dividend_array[j] -= static_cast<uint32_t>(mult);

    // if guess was too big, we add back divisor
    if (dividend_array[j] > prev) {
      --guess;
      uint32_t carry = 0;
      for (int64_t i = divisor_length - 1; i >= 0; --i) {
        const auto sum =
            static_cast<uint64_t>(divisor_array[i]) + dividend_array[j + i + 1] + carry;
        dividend_array[j + i + 1] = static_cast<uint32_t>(sum);
        carry = static_cast<uint32_t>(sum >> 32);
      }
      dividend_array[j] += carry;
    }

    result_array[j] = guess;
  }

  // denormalize the remainder
  ShiftArrayRight(dividend_array, dividend_length, normalize_bits);

  // return result and remainder
  auto status = BuildFromArray(result, result_array, result_length);
  if (status != DecimalStatus::kSuccess) {
    return status;
  }
  status = BuildFromArray(remainder, dividend_array, dividend_length);
  if (status != DecimalStatus::kSuccess) {
    return status;
  }

  FixDivisionSigns(result, remainder, dividend_was_negative, divisor_was_negative);
  return DecimalStatus::kSuccess;
}

DecimalStatus BasicDecimal128::Divide(const BasicDecimal128& divisor,
                                      BasicDecimal128* result,
                                      BasicDecimal128* remainder) const {
  return DecimalDivide(*this, divisor, result, remainder);
}

bool operator==(const BasicDecimal128& left, const BasicDecimal128& right) {
  return left.high_bits() == right.high_bits() && left.low_bits() == right.low_bits();
}

bool operator!=(const BasicDecimal128& left, const BasicDecimal128& right) {
  return !operator==(left, right);
}

bool operator<(const BasicDecimal128& left, const BasicDecimal128& right) {
  return left.high_bits() < right.high_bits() ||
         (left.high_bits() == right.high_bits() && left.low_bits() < right.low_bits());
}

bool operator<=(const BasicDecimal128& left, const BasicDecimal128& right) {
  return !operator>(left, right);
}

bool operator>(const BasicDecimal128& left, const BasicDecimal128& right) {
  return operator<(right, left);
}

bool operator>=(const BasicDecimal128& left, const BasicDecimal128& right) {
  return !operator<(left, right);
}

BasicDecimal128 operator-(const BasicDecimal128& operand) {
  BasicDecimal128 result(operand.high_bits(), operand.low_bits());
  return result.Negate();
}

BasicDecimal128 operator~(const BasicDecimal128& operand) {
  BasicDecimal128 result(~operand.high_bits(), ~operand.low_bits());
  return result;
}

BasicDecimal128 operator+(const BasicDecimal128& left, const BasicDecimal128& right) {
  BasicDecimal128 result(left.high_bits(), left.low_bits());
  result += right;
  return result;
}

BasicDecimal128 operator-(const BasicDecimal128& left, const BasicDecimal128& right) {
  BasicDecimal128 result(left.high_bits(), left.low_bits());
  result -= right;
  return result;
}

BasicDecimal128 operator*(const BasicDecimal128& left, const BasicDecimal128& right) {
  BasicDecimal128 result(left.high_bits(), left.low_bits());
  result *= right;
  return result;
}

BasicDecimal128 operator/(const BasicDecimal128& left, const BasicDecimal128& right) {
  BasicDecimal128 remainder;
  BasicDecimal128 result;
  auto s = left.Divide(right, &result, &remainder);
  DCHECK_EQ(s, DecimalStatus::kSuccess);
  return result;
}

BasicDecimal128 operator%(const BasicDecimal128& left, const BasicDecimal128& right) {
  BasicDecimal128 remainder;
  BasicDecimal128 result;
  auto s = left.Divide(right, &result, &remainder);
  DCHECK_EQ(s, DecimalStatus::kSuccess);
  return remainder;
}

template <class DecimalClass>
static bool RescaleWouldCauseDataLoss(const DecimalClass& value, int32_t delta_scale,
                                      const DecimalClass& multiplier,
                                      DecimalClass* result) {
  if (delta_scale < 0) {
    DCHECK_NE(multiplier, 0);
    DecimalClass remainder;
    auto status = value.Divide(multiplier, result, &remainder);
    DCHECK_EQ(status, DecimalStatus::kSuccess);
    return remainder != 0;
  }

  *result = value * multiplier;
  return (value < 0) ? *result > value : *result < value;
}

template <class DecimalClass>
DecimalStatus DecimalRescale(const DecimalClass& value, int32_t original_scale,
                             int32_t new_scale, DecimalClass* out) {
  DCHECK_NE(out, nullptr);

  if (original_scale == new_scale) {
    *out = value;
    return DecimalStatus::kSuccess;
  }

  const int32_t delta_scale = new_scale - original_scale;
  const int32_t abs_delta_scale = std::abs(delta_scale);

  DecimalClass multiplier = DecimalClass::GetScaleMultiplier(abs_delta_scale);

  const bool rescale_would_cause_data_loss =
      RescaleWouldCauseDataLoss(value, delta_scale, multiplier, out);

  // Fail if we overflow or truncate
  if (ARROW_PREDICT_FALSE(rescale_would_cause_data_loss)) {
    return DecimalStatus::kRescaleDataLoss;
  }

  return DecimalStatus::kSuccess;
}

DecimalStatus BasicDecimal128::Rescale(int32_t original_scale, int32_t new_scale,
                                       BasicDecimal128* out) const {
  return DecimalRescale(*this, original_scale, new_scale, out);
}

void BasicDecimal128::GetWholeAndFraction(int scale, BasicDecimal128* whole,
                                          BasicDecimal128* fraction) const {
  DCHECK_GE(scale, 0);
  DCHECK_LE(scale, 38);

  BasicDecimal128 multiplier(ScaleMultipliers[scale]);
  auto s = Divide(multiplier, whole, fraction);
  DCHECK_EQ(s, DecimalStatus::kSuccess);
}

const BasicDecimal128& BasicDecimal128::GetScaleMultiplier(int32_t scale) {
  DCHECK_GE(scale, 0);
  DCHECK_LE(scale, 38);

  return ScaleMultipliers[scale];
}

const BasicDecimal128& BasicDecimal128::GetMaxValue() { return kMaxValue; }

BasicDecimal128 BasicDecimal128::IncreaseScaleBy(int32_t increase_by) const {
  DCHECK_GE(increase_by, 0);
  DCHECK_LE(increase_by, 38);

  return (*this) * ScaleMultipliers[increase_by];
}

BasicDecimal128 BasicDecimal128::ReduceScaleBy(int32_t reduce_by, bool round) const {
  DCHECK_GE(reduce_by, 0);
  DCHECK_LE(reduce_by, 38);

  if (reduce_by == 0) {
    return *this;
  }

  BasicDecimal128 divisor(ScaleMultipliers[reduce_by]);
  BasicDecimal128 result;
  BasicDecimal128 remainder;
  auto s = Divide(divisor, &result, &remainder);
  DCHECK_EQ(s, DecimalStatus::kSuccess);
  if (round) {
    auto divisor_half = ScaleMultipliersHalf[reduce_by];
    if (remainder.Abs() >= divisor_half) {
      if (result > 0) {
        result += 1;
      } else {
        result -= 1;
      }
    }
  }
  return result;
}

int32_t BasicDecimal128::CountLeadingBinaryZeros() const {
  DCHECK_GE(*this, BasicDecimal128(0));

  if (high_bits_ == 0) {
    return BitUtil::CountLeadingZeros(low_bits_) + 64;
  } else {
    return BitUtil::CountLeadingZeros(static_cast<uint64_t>(high_bits_));
  }
}

#if ARROW_LITTLE_ENDIAN
BasicDecimal256::BasicDecimal256(const uint8_t* bytes)
    : little_endian_array_(
          std::array<uint64_t, 4>({reinterpret_cast<const uint64_t*>(bytes)[0],
                                   reinterpret_cast<const uint64_t*>(bytes)[1],
                                   reinterpret_cast<const uint64_t*>(bytes)[2],
                                   reinterpret_cast<const uint64_t*>(bytes)[3]})) {}
#else
BasicDecimal256::BasicDecimal256(const uint8_t* bytes)
    : little_endian_array_(
          std::array<uint64_t, 4>({reinterpret_cast<const uint64_t*>(bytes)[3],
                                   reinterpret_cast<const uint64_t*>(bytes)[2],
                                   reinterpret_cast<const uint64_t*>(bytes)[1],
                                   reinterpret_cast<const uint64_t*>(bytes)[0]})) {}
#endif

BasicDecimal256& BasicDecimal256::Negate() {
  uint64_t carry = 1;
  for (uint64_t& elem : little_endian_array_) {
    elem = ~elem + carry;
    carry &= (elem == 0);
  }
  return *this;
}

BasicDecimal256& BasicDecimal256::Abs() { return *this < 0 ? Negate() : *this; }

BasicDecimal256 BasicDecimal256::Abs(const BasicDecimal256& in) {
  BasicDecimal256 result(in);
  return result.Abs();
}

BasicDecimal256& BasicDecimal256::operator+=(const BasicDecimal256& right) {
  uint64_t carry = 0;
  for (size_t i = 0; i < little_endian_array_.size(); i++) {
    const uint64_t right_value = right.little_endian_array_[i];
    uint64_t sum = right_value + carry;
    carry = 0;
    if (sum < right_value) {
      carry += 1;
    }
    sum += little_endian_array_[i];
    if (sum < little_endian_array_[i]) {
      carry += 1;
    }
    little_endian_array_[i] = sum;
  }
  return *this;
}

BasicDecimal256& BasicDecimal256::operator-=(const BasicDecimal256& right) {
  *this += -right;
  return *this;
}

BasicDecimal256& BasicDecimal256::operator<<=(uint32_t bits) {
  if (bits == 0) {
    return *this;
  }
  int cross_word_shift = bits / 64;
  if (static_cast<size_t>(cross_word_shift) >= little_endian_array_.size()) {
    little_endian_array_ = {0, 0, 0, 0};
    return *this;
  }
  uint32_t in_word_shift = bits % 64;
  for (int i = static_cast<int>(little_endian_array_.size() - 1); i >= cross_word_shift;
       i--) {
    // Account for shifts larger then 64 bits
    little_endian_array_[i] = little_endian_array_[i - cross_word_shift];
    little_endian_array_[i] <<= in_word_shift;
    if (in_word_shift != 0 && i >= cross_word_shift + 1) {
      little_endian_array_[i] |=
          little_endian_array_[i - (cross_word_shift + 1)] >> (64 - in_word_shift);
    }
  }
  for (int i = cross_word_shift - 1; i >= 0; i--) {
    little_endian_array_[i] = 0;
  }
  return *this;
}

std::array<uint8_t, 32> BasicDecimal256::ToBytes() const {
  std::array<uint8_t, 32> out{{0}};
  ToBytes(out.data());
  return out;
}

void BasicDecimal256::ToBytes(uint8_t* out) const {
  DCHECK_NE(out, nullptr);
#if ARROW_LITTLE_ENDIAN
  reinterpret_cast<int64_t*>(out)[0] = little_endian_array_[0];
  reinterpret_cast<int64_t*>(out)[1] = little_endian_array_[1];
  reinterpret_cast<int64_t*>(out)[2] = little_endian_array_[2];
  reinterpret_cast<int64_t*>(out)[3] = little_endian_array_[3];
#else
  reinterpret_cast<int64_t*>(out)[0] = little_endian_array_[3];
  reinterpret_cast<int64_t*>(out)[1] = little_endian_array_[2];
  reinterpret_cast<int64_t*>(out)[2] = little_endian_array_[1];
  reinterpret_cast<int64_t*>(out)[3] = little_endian_array_[0];
#endif
}

BasicDecimal256& BasicDecimal256::operator*=(const BasicDecimal256& right) {
  // Since the max value of BasicDecimal256 is supposed to be 1e76 - 1 and the
  // min the negation taking the absolute values here should always be safe.
  const bool negate = Sign() != right.Sign();
  BasicDecimal256 x = BasicDecimal256::Abs(*this);
  BasicDecimal256 y = BasicDecimal256::Abs(right);

  uint128_t r_hi;
  uint128_t r_lo;
  std::array<uint64_t, 4> res{0, 0, 0, 0};
  MultiplyUnsignedArray<4>(x.little_endian_array_, y.little_endian_array_, &res);
  little_endian_array_ = res;
  if (negate) {
    Negate();
  }
  return *this;
}

DecimalStatus BasicDecimal256::Divide(const BasicDecimal256& divisor,
                                      BasicDecimal256* result,
                                      BasicDecimal256* remainder) const {
  return DecimalDivide(*this, divisor, result, remainder);
}

DecimalStatus BasicDecimal256::Rescale(int32_t original_scale, int32_t new_scale,
                                       BasicDecimal256* out) const {
  return DecimalRescale(*this, original_scale, new_scale, out);
}

BasicDecimal256 BasicDecimal256::IncreaseScaleBy(int32_t increase_by) const {
  DCHECK_GE(increase_by, 0);
  DCHECK_LE(increase_by, 76);

  return (*this) * ScaleMultipliersDecimal256[increase_by];
}

BasicDecimal256 BasicDecimal256::ReduceScaleBy(int32_t reduce_by, bool round) const {
  DCHECK_GE(reduce_by, 0);
  DCHECK_LE(reduce_by, 76);

  if (reduce_by == 0) {
    return *this;
  }

  BasicDecimal256 divisor(ScaleMultipliersDecimal256[reduce_by]);
  BasicDecimal256 result;
  BasicDecimal256 remainder;
  auto s = Divide(divisor, &result, &remainder);
  DCHECK_EQ(s, DecimalStatus::kSuccess);
  if (round) {
    auto divisor_half = ScaleMultipliersHalfDecimal256[reduce_by];
    if (remainder.Abs() >= divisor_half) {
      if (result > 0) {
        result += 1;
      } else {
        result -= 1;
      }
    }
  }
  return result;
}

bool BasicDecimal256::FitsInPrecision(int32_t precision) const {
  DCHECK_GT(precision, 0);
  DCHECK_LE(precision, 76);
  return BasicDecimal256::Abs(*this) < ScaleMultipliersDecimal256[precision];
}

const BasicDecimal256& BasicDecimal256::GetScaleMultiplier(int32_t scale) {
  DCHECK_GE(scale, 0);
  DCHECK_LE(scale, 76);

  return ScaleMultipliersDecimal256[scale];
}

BasicDecimal256 operator*(const BasicDecimal256& left, const BasicDecimal256& right) {
  BasicDecimal256 result = left;
  result *= right;
  return result;
}

bool operator<(const BasicDecimal256& left, const BasicDecimal256& right) {
  const std::array<uint64_t, 4>& lhs = left.little_endian_array();
  const std::array<uint64_t, 4>& rhs = right.little_endian_array();
  return lhs[3] != rhs[3]
             ? static_cast<int64_t>(lhs[3]) < static_cast<int64_t>(rhs[3])
             : lhs[2] != rhs[2] ? lhs[2] < rhs[2]
                                : lhs[1] != rhs[1] ? lhs[1] < rhs[1] : lhs[0] < rhs[0];
}

BasicDecimal256 operator-(const BasicDecimal256& operand) {
  BasicDecimal256 result(operand);
  return result.Negate();
}

BasicDecimal256 operator~(const BasicDecimal256& operand) {
  const std::array<uint64_t, 4>& arr = operand.little_endian_array();
  BasicDecimal256 result({~arr[0], ~arr[1], ~arr[2], ~arr[3]});
  return result;
}

BasicDecimal256& BasicDecimal256::operator/=(const BasicDecimal256& right) {
  BasicDecimal256 remainder;
  auto s = Divide(right, this, &remainder);
  DCHECK_EQ(s, DecimalStatus::kSuccess);
  return *this;
}

BasicDecimal256 operator+(const BasicDecimal256& left, const BasicDecimal256& right) {
  BasicDecimal256 sum = left;
  sum += right;
  return sum;
}

BasicDecimal256 operator/(const BasicDecimal256& left, const BasicDecimal256& right) {
  BasicDecimal256 remainder;
  BasicDecimal256 result;
  auto s = left.Divide(right, &result, &remainder);
  DCHECK_EQ(s, DecimalStatus::kSuccess);
  return result;
}

}  // namespace arrow
