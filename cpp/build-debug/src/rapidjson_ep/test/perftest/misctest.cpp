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

#include "perftest.h"

#if TEST_MISC

#define __STDC_FORMAT_MACROS
#include "rapidjson/stringbuffer.h"

#define protected public
#include "rapidjson/writer.h"
#undef private

class Misc : public PerfTest {
};

// Copyright (c) 2008-2010 Bjoern Hoehrmann <bjoern@hoehrmann.de>
// See http://bjoern.hoehrmann.de/utf-8/decoder/dfa/ for details.

#define UTF8_ACCEPT 0
#define UTF8_REJECT 12

static const unsigned char utf8d[] = {
    // The first part of the table maps bytes to character classes that
    // to reduce the size of the transition table and create bitmasks.
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
    1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,  9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,
    7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,  7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,
    8,8,2,2,2,2,2,2,2,2,2,2,2,2,2,2,  2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,
    10,3,3,3,3,3,3,3,3,3,3,3,3,4,3,3, 11,6,6,6,5,8,8,8,8,8,8,8,8,8,8,8,

    // The second part is a transition table that maps a combination
    // of a state of the automaton and a character class to a state.
    0,12,24,36,60,96,84,12,12,12,48,72, 12,12,12,12,12,12,12,12,12,12,12,12,
    12, 0,12,12,12,12,12, 0,12, 0,12,12, 12,24,12,12,12,12,12,24,12,24,12,12,
    12,12,12,12,12,12,12,24,12,12,12,12, 12,24,12,12,12,12,12,12,12,24,12,12,
    12,12,12,12,12,12,12,36,12,36,12,12, 12,36,12,12,12,12,12,36,12,36,12,12,
    12,36,12,12,12,12,12,12,12,12,12,12, 
};

static unsigned inline decode(unsigned* state, unsigned* codep, unsigned byte) {
    unsigned type = utf8d[byte];

    *codep = (*state != UTF8_ACCEPT) ?
        (byte & 0x3fu) | (*codep << 6) :
    (0xff >> type) & (byte);

    *state = utf8d[256 + *state + type];
    return *state;
}

static bool IsUTF8(unsigned char* s) {
    unsigned codepoint, state = 0;

    while (*s)
        decode(&state, &codepoint, *s++);

    return state == UTF8_ACCEPT;
}

TEST_F(Misc, Hoehrmann_IsUTF8) {
    for (size_t i = 0; i < kTrialCount; i++) {
        EXPECT_TRUE(IsUTF8((unsigned char*)json_));
    }
}

////////////////////////////////////////////////////////////////////////////////
// CountDecimalDigit: Count number of decimal places

inline unsigned CountDecimalDigit_naive(unsigned n) {
    unsigned count = 1;
    while (n >= 10) {
        n /= 10;
        count++;
    }
    return count;
}

inline unsigned CountDecimalDigit_enroll4(unsigned n) {
    unsigned count = 1;
    while (n >= 10000) {
        n /= 10000u;
        count += 4;
    }
    if (n < 10) return count;
    if (n < 100) return count + 1;
    if (n < 1000) return count + 2;
    return count + 3;
}

inline unsigned CountDecimalDigit64_enroll4(uint64_t n) {
    unsigned count = 1;
    while (n >= 10000) {
        n /= 10000u;
        count += 4;
    }
    if (n < 10) return count;
    if (n < 100) return count + 1;
    if (n < 1000) return count + 2;
    return count + 3;
}

inline unsigned CountDecimalDigit_fast(unsigned n) {
    static const uint32_t powers_of_10[] = {
        0,
        10,
        100,
        1000,
        10000,
        100000,
        1000000,
        10000000,
        100000000,
        1000000000
    };

#if defined(_M_IX86) || defined(_M_X64)
    unsigned long i = 0;
    _BitScanReverse(&i, n | 1);
    uint32_t t = (i + 1) * 1233 >> 12;
#elif defined(__GNUC__)
    uint32_t t = (32 - __builtin_clz(n | 1)) * 1233 >> 12;
#else
#error
#endif
    return t - (n < powers_of_10[t]) + 1;
}

inline unsigned CountDecimalDigit64_fast(uint64_t n) {
    static const uint64_t powers_of_10[] = {
        0,
        10,
        100,
        1000,
        10000,
        100000,
        1000000,
        10000000,
        100000000,
        1000000000,
        10000000000,
        100000000000,
        1000000000000,
        10000000000000,
        100000000000000,
        1000000000000000,
        10000000000000000,
        100000000000000000,
        1000000000000000000,
        10000000000000000000U
    };

#if defined(_M_IX86)
    uint64_t m = n | 1;
    unsigned long i = 0;
    if (_BitScanReverse(&i, m >> 32))
        i += 32;
    else
        _BitScanReverse(&i, m & 0xFFFFFFFF);
    uint32_t t = (i + 1) * 1233 >> 12;
#elif defined(_M_X64)
    unsigned long i = 0;
    _BitScanReverse64(&i, n | 1);
    uint32_t t = (i + 1) * 1233 >> 12;
#elif defined(__GNUC__)
    uint32_t t = (64 - __builtin_clzll(n | 1)) * 1233 >> 12;
#else
#error
#endif

    return t - (n < powers_of_10[t]) + 1;
}

#if 0
// Exhaustive, very slow
TEST_F(Misc, CountDecimalDigit_Verify) {
    unsigned i = 0;
    do {
        if (i % (65536 * 256) == 0)
            printf("%u\n", i);
        ASSERT_EQ(CountDecimalDigit_enroll4(i), CountDecimalDigit_fast(i));
        i++;
    } while (i != 0);
}

static const unsigned kDigits10Trial = 1000000000u;
TEST_F(Misc, CountDecimalDigit_naive) {
    unsigned sum = 0;
    for (unsigned i = 0; i < kDigits10Trial; i++)
        sum += CountDecimalDigit_naive(i);
    printf("%u\n", sum);
}

TEST_F(Misc, CountDecimalDigit_enroll4) {
    unsigned sum = 0;
    for (unsigned i = 0; i < kDigits10Trial; i++)
        sum += CountDecimalDigit_enroll4(i);
    printf("%u\n", sum);
}

TEST_F(Misc, CountDecimalDigit_fast) {
    unsigned sum = 0;
    for (unsigned i = 0; i < kDigits10Trial; i++)
        sum += CountDecimalDigit_fast(i);
    printf("%u\n", sum);
}
#endif

TEST_F(Misc, CountDecimalDigit64_VerifyFast) {
    uint64_t i = 1, j;
    do {
        //printf("%" PRIu64 "\n", i);
        ASSERT_EQ(CountDecimalDigit64_enroll4(i), CountDecimalDigit64_fast(i));
        j = i;
        i *= 3;
    } while (j < i);
}

////////////////////////////////////////////////////////////////////////////////
// integer-to-string conversion

// https://gist.github.com/anonymous/7179097
static const int randval[] ={
     936116,  369532,  453755,  -72860,  209713,  268347,  435278, -360266, -416287, -182064,
    -644712,  944969,  640463, -366588,  471577,  -69401, -744294, -505829,  923883,  831785,
    -601136, -636767, -437054,  591718,  100758,  231907, -719038,  973540, -605220,  506659,
    -871653,  462533,  764843, -919138,  404305, -630931, -288711, -751454, -173726, -718208,
     432689, -281157,  360737,  659827,   19174, -376450,  769984, -858198,  439127,  734703,
    -683426,       7,  386135,  186997, -643900, -744422, -604708, -629545,   42313, -933592,
    -635566,  182308,  439024, -367219,  -73924, -516649,  421935, -470515,  413507,  -78952,
    -427917, -561158,  737176,   94538,  572322,  405217,  709266, -357278, -908099, -425447,
     601119,  750712, -862285, -177869,  900102,  384877,  157859, -641680,  503738, -702558,
     278225,  463290,  268378, -212840,  580090,  347346, -473985, -950968, -114547, -839893,
    -738032, -789424,  409540,  493495,  432099,  119755,  905004, -174834,  338266,  234298,
      74641, -965136, -754593,  685273,  466924,  920560,  385062,  796402,  -67229,  994864,
     376974,  299869, -647540, -128724,  469890, -163167, -547803, -743363,  486463, -621028,
     612288,   27459, -514224,  126342,  -66612,  803409, -777155, -336453, -284002,  472451,
     342390, -163630,  908356, -456147, -825607,  268092, -974715,  287227,  227890, -524101,
     616370, -782456,  922098, -624001, -813690,  171605, -192962,  796151,  707183,  -95696,
     -23163, -721260,  508892,  430715,  791331,  482048, -996102,  863274,  275406,   -8279,
    -556239, -902076,  268647, -818565,  260069, -798232, -172924, -566311, -806503, -885992,
     813969,  -78468,  956632,  304288,  494867, -508784,  381751,  151264,  762953,   76352,
     594902,  375424,  271700, -743062,  390176,  924237,  772574,  676610,  435752, -153847,
       3959, -971937, -294181, -538049, -344620, -170136,   19120, -703157,  868152, -657961,
    -818631,  219015, -872729, -940001, -956570,  880727, -345910,  942913, -942271, -788115,
     225294,  701108, -517736, -416071,  281940,  488730,  942698,  711494,  838382, -892302,
    -533028,  103052,  528823,  901515,  949577,  159364,  718227, -241814, -733661, -462928,
    -495829,  165170,  513580, -629188, -509571, -459083,  198437,   77198, -644612,  811276,
    -422298, -860842,  -52584,  920369,  686424, -530667, -243476,   49763,  345866, -411960,
    -114863,  470810, -302860,  683007, -509080,       2, -174981, -772163,  -48697,  447770,
    -268246,  213268,  269215,   78810, -236340, -639140, -864323,  505113, -986569, -325215,
     541859,  163070, -819998, -645161, -583336,  573414,  696417, -132375,       3, -294501,
     320435,  682591,  840008,  351740,  426951,  609354,  898154, -943254,  227321, -859793,
    -727993,   44137, -497965, -782239,   14955, -746080, -243366,    9837, -233083,  606507,
    -995864, -615287, -994307,  602715,  770771, -315040,  610860,  446102, -307120,  710728,
    -590392, -230474, -762625, -637525,  134963, -202700, -766902, -985541,  218163,  682009,
     926051,  525156,  -61195,  403211, -810098,  245539, -431733,  179998, -806533,  745943,
     447597,  131973, -187130,  826019,  286107, -937230, -577419,   20254,  681802, -340500,
     323080,  266283, -667617,  309656,  416386,  611863,  759991, -534257,  523112, -634892,
    -169913, -204905, -909867, -882185, -944908,  741811, -717675,  967007, -317396,  407230,
    -412805,  792905,  994873,  744793, -456797,  713493,  355232,  116900, -945199,  880539,
     342505, -580824, -262273,  982968, -349497, -735488,  311767, -455191,  570918,  389734,
    -958386,   10262,  -99267,  155481,  304210,  204724,  704367, -144893, -233664, -671441,
     896849,  408613,  762236,  322697,  981321,  688476,   13663, -970704, -379507,  896412,
     977084,  348869,  875948,  341348,  318710,  512081,    6163,  669044,  833295,  811883,
     708756, -802534, -536057,  608413, -389625, -694603,  541106, -110037,  720322, -540581,
     645420,   32980,   62442,  510157, -981870,  -87093, -325960, -500494, -718291,  -67889,
     991501,  374804,  769026, -978869,  294747,  714623,  413327, -199164,  671368,  804789,
    -362507,  798196, -170790, -568895, -869379,   62020, -316693, -837793,  644994,  -39341,
    -417504, -243068, -957756,   99072,  622234, -739992,  225668,    8863, -505910,   82483,
    -559244,  241572,    1315,  -36175,  -54990,  376813,     -11,  162647, -688204, -486163,
     -54934, -197470,  744223, -762707,  732540,  996618,  351561, -445933, -898491,  486531,
     456151,   15276,  290186, -817110,  -52995,  313046, -452533,  -96267,   94470, -500176,
    -818026, -398071, -810548, -143325, -819741,    1338, -897676, -101577, -855445,   37309,
     285742,  953804, -777927, -926962, -811217, -936744, -952245, -802300, -490188, -964953,
    -552279,  329142, -570048, -505756,  682898, -381089,  -14352,  175138,  152390, -582268,
    -485137,  717035,  805329,  239572, -730409,  209643, -184403, -385864,  675086,  819648,
     629058, -527109, -488666, -171981,  532788,  552441,  174666,  984921,  766514,  758787,
     716309,  338801, -978004, -412163,  876079, -734212,  789557, -160491, -522719,   56644,
       -991, -286038,  -53983,  663740,  809812,  919889, -717502, -137704,  220511,  184396,
    -825740, -588447,  430870,  124309,  135956,  558662, -307087, -788055, -451328,  812260,
     931601,  324347, -482989, -117858, -278861,  189068, -172774,  929057,  293787,  198161,
    -342386,  -47173,  906555, -759955,  -12779,  777604,  -97869,  899320,  927486,  -25284,
    -848550,  259450, -485856,  -17820,      88,  171400,  235492, -326783, -340793,  886886,
     112428, -246280,    5979,  648444, -114982,  991013,  -56489,   -9497,  419706,  632820,
    -341664,  393926, -848977,  -22538,  257307,  773731, -905319,  491153,  734883, -868212,
    -951053,  644458, -580758,  764735,  584316,  297077,   28852, -397710, -953669,  201772,
     879050, -198237, -588468,  448102, -116837,  770007, -231812,  642906, -582166, -885828,
          9,  305082, -996577,  303559,   75008, -772956, -447960,  599825, -295552,  870739,
    -386278, -950300,  485359, -457081,  629461, -850276,  550496, -451755, -620841,  -11766,
    -950137,  832337,   28711, -273398, -507197,   91921, -271360, -705991, -753220, -388968,
     967945,  340434, -320883, -662793, -554617, -574568,  477946,   -6148, -129519,  689217,
     920020, -656315, -974523, -212525,   80921, -612532,  645096,  545655,  655713, -591631,
    -307385, -816688, -618823, -113713,  526430,  673063,  735916, -809095, -850417,  639004,
     432281, -388185,  270708,  860146,  -39902, -786157, -258180, -246169, -966720, -264957,
     548072, -306010,  -57367, -635665,  933824,   70553, -989936, -488741,   72411, -452509,
     529831,  956277,  449019, -577850, -360986, -803418,   48833,  296073,  203430,  609591,
     715483,  470964,  658106, -718254,  -96424,  790163,  334739,  181070, -373578,       5,
    -435088,  329841,  330939, -256602,  394355,  912412,  231910,  927278, -661933,  788539,
    -769664, -893274,  -96856,  298205,  901043, -608122, -527430,  183618, -553963,  -35246,
    -393924,  948832, -483198,  594501,   35460, -407007,   93494, -336881, -634072,  984205,
    -812161,  944664,  -31062,  753872,  823933,  -69566,   50445,  290147,   85134,   34706,
     551902,  405202, -991246,  -84642,  154341,  316432, -695101, -651588,   -5030,  137564,
    -294665,  332541,  528307,  -90572, -344923,  523766, -758498, -968047,  339028,  494578,
     593129, -725773,   31834, -718406, -208638,  159665,   -2043,  673344, -442767,   75816,
     755442,  769257, -158730, -410272,  691688,  589550, -878398, -184121,  460679,  346312,
     294163, -544602,  653308,  254167, -276979,   52073, -892684,  887653,  -41222,  983065,
     -68258, -408799,  -99069, -674069, -863635,  -32890,  622757, -743862,   40872,   -4837,
    -967228,  522370, -903951, -818669,  524459,  514702,  925801,   20007, -299229,  579348,
     626021,  430089,  348139, -562692, -607728, -130606, -928451, -424793, -458647, -448892,
    -312230,  143337,  109746,  880042, -339658, -785614,  938995,  540916,  118429,  661351,
    -402967,  404729,  -40918, -976535,  743230,  713110,  440182, -381314, -499252,   74613,
     193652,  912717,  491323,  583633,  324691,  459397,  281253,  195540,   -2764, -888651,
     892449,  132663, -478373, -430002, -314551,  527826,  247165,  557966,  554778,  481531,
    -946634,  431685, -769059, -348371,  174046,  184597, -354867,  584422,  227390, -850397,
    -542924, -849093, -737769,  325359,  736314,  269101,  767940,  674809,   81413, -447458,
     445076,  189072,  906218,  502688, -718476, -863827, -731381,  100660,  623249,  710008,
     572060,  922203,  685740,   55096,  263394, -243695, -353910, -516788,  388471,  455165,
     844103, -643772,  363976,  268875, -899450,  104470,  104029, -238874, -274659,  732969,
    -676443,  953291, -916289, -861849, -242344,  958083, -479593, -970395,  799831,  277841,
    -243236, -283462, -201510,  166263, -259105, -575706,  878926,  891064,  895297,  655262,
     -34807, -809833,  -89281,  342585,  554920,       1,  902141, -333425,  139703,  852318,
    -618438,  329498, -932596, -692836, -513372,  733656, -523411,   85779,  500478, -682697,
    -502836,  138776,  156341, -420037, -557964, -556378,  710993,  -50383, -877159,  916334,
     132996,  583516, -603392, -111615,  -12288, -780214,  476780,  123327,  137607,  519956,
     745837,   17358, -158581,  -53490
};
static const size_t randvalCount = sizeof(randval) / sizeof(randval[0]);
static const size_t kItoaTrialCount = 10000;

static const char digits[201] =
"0001020304050607080910111213141516171819"
"2021222324252627282930313233343536373839"
"4041424344454647484950515253545556575859"
"6061626364656667686970717273747576777879"
"8081828384858687888990919293949596979899";

// Prevent code being optimized out
//#define OUTPUT_LENGTH(length) printf("", length)
#define OUTPUT_LENGTH(length) printf("%u\n", (unsigned)length)

template<typename OutputStream>
class Writer1 {
public:
    Writer1() : os_() {}
    Writer1(OutputStream& os) : os_(&os) {}

    void Reset(OutputStream& os) {
        os_ = &os;
    }

    bool WriteInt(int i) {
        if (i < 0) {
            os_->Put('-');
            i = -i;
        }
        return WriteUint((unsigned)i);
    }

    bool WriteUint(unsigned u) {
        char buffer[10];
        char *p = buffer;
        do {
            *p++ = char(u % 10) + '0';
            u /= 10;
        } while (u > 0);

        do {
            --p;
            os_->Put(*p);
        } while (p != buffer);
        return true;
    }

    bool WriteInt64(int64_t i64) {
        if (i64 < 0) {
            os_->Put('-');
            i64 = -i64;
        }
        WriteUint64((uint64_t)i64);
        return true;
    }

    bool WriteUint64(uint64_t u64) {
        char buffer[20];
        char *p = buffer;
        do {
            *p++ = char(u64 % 10) + '0';
            u64 /= 10;
        } while (u64 > 0);

        do {
            --p;
            os_->Put(*p);
        } while (p != buffer);
        return true;
    }

private:
    OutputStream* os_;
};

template<>
bool Writer1<rapidjson::StringBuffer>::WriteUint(unsigned u) {
    char buffer[10];
    char* p = buffer;
    do {
        *p++ = char(u % 10) + '0';
        u /= 10;
    } while (u > 0);

    char* d = os_->Push(p - buffer);
    do {
        --p;
        *d++ = *p;
    } while (p != buffer);
    return true;
}

// Using digits LUT to reduce division/modulo
template<typename OutputStream>
class Writer2 {
public:
    Writer2() : os_() {}
    Writer2(OutputStream& os) : os_(&os) {}

    void Reset(OutputStream& os) {
        os_ = &os;
    }

    bool WriteInt(int i) {
        if (i < 0) {
            os_->Put('-');
            i = -i;
        }
        return WriteUint((unsigned)i);
    }

    bool WriteUint(unsigned u) {
        char buffer[10];
        char* p = buffer;
        while (u >= 100) {
            const unsigned i = (u % 100) << 1;
            u /= 100;
            *p++ = digits[i + 1];
            *p++ = digits[i];
        }
        if (u < 10)
            *p++ = char(u) + '0';
        else {
            const unsigned i = u << 1;
            *p++ = digits[i + 1];
            *p++ = digits[i];
        }

        do {
            --p;
            os_->Put(*p);
        } while (p != buffer);
        return true;
    }

    bool WriteInt64(int64_t i64) {
        if (i64 < 0) {
            os_->Put('-');
            i64 = -i64;
        }
        WriteUint64((uint64_t)i64);
        return true;
    }

    bool WriteUint64(uint64_t u64) {
        char buffer[20];
        char* p = buffer;
        while (u64 >= 100) {
            const unsigned i = static_cast<unsigned>(u64 % 100) << 1;
            u64 /= 100;
            *p++ = digits[i + 1];
            *p++ = digits[i];
        }
        if (u64 < 10)
            *p++ = char(u64) + '0';
        else {
            const unsigned i = static_cast<unsigned>(u64) << 1;
            *p++ = digits[i + 1];
            *p++ = digits[i];
        }

        do {
            --p;
            os_->Put(*p);
        } while (p != buffer);
        return true;
    }

private:
    OutputStream* os_;
};

// First pass to count digits
template<typename OutputStream>
class Writer3 {
public:
    Writer3() : os_() {}
    Writer3(OutputStream& os) : os_(&os) {}

    void Reset(OutputStream& os) {
        os_ = &os;
    }

    bool WriteInt(int i) {
        if (i < 0) {
            os_->Put('-');
            i = -i;
        }
        return WriteUint((unsigned)i);
    }

    bool WriteUint(unsigned u) {
        char buffer[10];
        char *p = buffer;
        do {
            *p++ = char(u % 10) + '0';
            u /= 10;
        } while (u > 0);

        do {
            --p;
            os_->Put(*p);
        } while (p != buffer);
        return true;
    }

    bool WriteInt64(int64_t i64) {
        if (i64 < 0) {
            os_->Put('-');
            i64 = -i64;
        }
        WriteUint64((uint64_t)i64);
        return true;
    }

    bool WriteUint64(uint64_t u64) {
        char buffer[20];
        char *p = buffer;
        do {
            *p++ = char(u64 % 10) + '0';
            u64 /= 10;
        } while (u64 > 0);

        do {
            --p;
            os_->Put(*p);
        } while (p != buffer);
        return true;
    }

private:
    void WriteUintReverse(char* d, unsigned u) {
        do {
            *--d = char(u % 10) + '0';
            u /= 10;
        } while (u > 0);
    }

    void WriteUint64Reverse(char* d, uint64_t u) {
        do {
            *--d = char(u % 10) + '0';
            u /= 10;
        } while (u > 0);
    }

    OutputStream* os_;
};

template<>
inline bool Writer3<rapidjson::StringBuffer>::WriteUint(unsigned u) {
    unsigned digit = CountDecimalDigit_fast(u);
    WriteUintReverse(os_->Push(digit) + digit, u);
    return true;
}

template<>
inline bool Writer3<rapidjson::InsituStringStream>::WriteUint(unsigned u) {
    unsigned digit = CountDecimalDigit_fast(u);
    WriteUintReverse(os_->Push(digit) + digit, u);
    return true;
}

template<>
inline bool Writer3<rapidjson::StringBuffer>::WriteUint64(uint64_t u) {
    unsigned digit = CountDecimalDigit64_fast(u);
    WriteUint64Reverse(os_->Push(digit) + digit, u);
    return true;
}

template<>
inline bool Writer3<rapidjson::InsituStringStream>::WriteUint64(uint64_t u) {
    unsigned digit = CountDecimalDigit64_fast(u);
    WriteUint64Reverse(os_->Push(digit) + digit, u);
    return true;
}

// Using digits LUT to reduce division/modulo, two passes
template<typename OutputStream>
class Writer4 {
public:
    Writer4() : os_() {}
    Writer4(OutputStream& os) : os_(&os) {}

    void Reset(OutputStream& os) {
        os_ = &os;
    }

    bool WriteInt(int i) {
        if (i < 0) {
            os_->Put('-');
            i = -i;
        }
        return WriteUint((unsigned)i);
    }

    bool WriteUint(unsigned u) {
        char buffer[10];
        char* p = buffer;
        while (u >= 100) {
            const unsigned i = (u % 100) << 1;
            u /= 100;
            *p++ = digits[i + 1];
            *p++ = digits[i];
        }
        if (u < 10)
            *p++ = char(u) + '0';
        else {
            const unsigned i = u << 1;
            *p++ = digits[i + 1];
            *p++ = digits[i];
        }

        do {
            --p;
            os_->Put(*p);
        } while (p != buffer);
        return true;
    }

    bool WriteInt64(int64_t i64) {
        if (i64 < 0) {
            os_->Put('-');
            i64 = -i64;
        }
        WriteUint64((uint64_t)i64);
        return true;
    }

    bool WriteUint64(uint64_t u64) {
        char buffer[20];
        char* p = buffer;
        while (u64 >= 100) {
            const unsigned i = static_cast<unsigned>(u64 % 100) << 1;
            u64 /= 100;
            *p++ = digits[i + 1];
            *p++ = digits[i];
        }
        if (u64 < 10)
            *p++ = char(u64) + '0';
        else {
            const unsigned i = static_cast<unsigned>(u64) << 1;
            *p++ = digits[i + 1];
            *p++ = digits[i];
        }

        do {
            --p;
            os_->Put(*p);
        } while (p != buffer);
        return true;
    }

private:
    void WriteUintReverse(char* d, unsigned u) {
        while (u >= 100) {
            const unsigned i = (u % 100) << 1;
            u /= 100;
            *--d = digits[i + 1];
            *--d = digits[i];
        }
        if (u < 10) {
            *--d = char(u) + '0';
        }
        else {
            const unsigned i = u << 1;
            *--d = digits[i + 1];
            *--d = digits[i];
        }
    }

    void WriteUint64Reverse(char* d, uint64_t u) {
        while (u >= 100) {
            const unsigned i = (u % 100) << 1;
            u /= 100;
            *--d = digits[i + 1];
            *--d = digits[i];
        }
        if (u < 10) {
            *--d = char(u) + '0';
        }
        else {
            const unsigned i = u << 1;
            *--d = digits[i + 1];
            *--d = digits[i];
        }
    }

    OutputStream* os_;
};

template<>
inline bool Writer4<rapidjson::StringBuffer>::WriteUint(unsigned u) {
    unsigned digit = CountDecimalDigit_fast(u);
    WriteUintReverse(os_->Push(digit) + digit, u);
    return true;
}

template<>
inline bool Writer4<rapidjson::InsituStringStream>::WriteUint(unsigned u) {
    unsigned digit = CountDecimalDigit_fast(u);
    WriteUintReverse(os_->Push(digit) + digit, u);
    return true;
}

template<>
inline bool Writer4<rapidjson::StringBuffer>::WriteUint64(uint64_t u) {
    unsigned digit = CountDecimalDigit64_fast(u);
    WriteUint64Reverse(os_->Push(digit) + digit, u);
    return true;
}

template<>
inline bool Writer4<rapidjson::InsituStringStream>::WriteUint64(uint64_t u) {
    unsigned digit = CountDecimalDigit64_fast(u);
    WriteUint64Reverse(os_->Push(digit) + digit, u);
    return true;
}

template <typename Writer>
void itoa_Writer_StringBufferVerify() {
    rapidjson::StringBuffer sb;
    Writer writer(sb);
    for (size_t j = 0; j < randvalCount; j++) {
        char buffer[32];
        sprintf(buffer, "%d", randval[j]);
        writer.WriteInt(randval[j]);
        ASSERT_STREQ(buffer, sb.GetString());
        sb.Clear();
    }
}

template <typename Writer>
void itoa_Writer_InsituStringStreamVerify() {
    Writer writer;
    for (size_t j = 0; j < randvalCount; j++) {
        char buffer[32];
        sprintf(buffer, "%d", randval[j]);
        char buffer2[32];
        rapidjson::InsituStringStream ss(buffer2);
        writer.Reset(ss);
        char* begin = ss.PutBegin();
        writer.WriteInt(randval[j]);
        ss.Put('\0');
        ss.PutEnd(begin);
        ASSERT_STREQ(buffer, buffer2);
    }
}

template <typename Writer>
void itoa_Writer_StringBuffer() {
    size_t length = 0;

    rapidjson::StringBuffer sb;
    Writer writer(sb);

    for (size_t i = 0; i < kItoaTrialCount; i++) {
        for (size_t j = 0; j < randvalCount; j++) {
            writer.WriteInt(randval[j]);
            length += sb.GetSize();
            sb.Clear();
        }
    }
    OUTPUT_LENGTH(length);
}

template <typename Writer>
void itoa_Writer_InsituStringStream() {
    size_t length = 0;

    char buffer[32];
    Writer writer;
    for (size_t i = 0; i < kItoaTrialCount; i++) {
        for (size_t j = 0; j < randvalCount; j++) {
            rapidjson::InsituStringStream ss(buffer);
            writer.Reset(ss);
            char* begin = ss.PutBegin();
            writer.WriteInt(randval[j]);
            length += ss.PutEnd(begin);
        }
    }
    OUTPUT_LENGTH(length);
};

template <typename Writer>
void itoa64_Writer_StringBufferVerify() {
    rapidjson::StringBuffer sb;
    Writer writer(sb);
    for (size_t j = 0; j < randvalCount; j++) {
        char buffer[32];
        int64_t x = randval[j] * randval[j];
        sprintf(buffer, "%" PRIi64, x);
        writer.WriteInt64(x);
        ASSERT_STREQ(buffer, sb.GetString());
        sb.Clear();
    }
}

template <typename Writer>
void itoa64_Writer_InsituStringStreamVerify() {
    Writer writer;
    for (size_t j = 0; j < randvalCount; j++) {
        char buffer[32];
        int64_t x = randval[j] * randval[j];
        sprintf(buffer, "%" PRIi64, x);
        char buffer2[32];
        rapidjson::InsituStringStream ss(buffer2);
        writer.Reset(ss);
        char* begin = ss.PutBegin();
        writer.WriteInt64(x);
        ss.Put('\0');
        ss.PutEnd(begin);
        ASSERT_STREQ(buffer, buffer2);
    }
}

template <typename Writer>
void itoa64_Writer_StringBuffer() {
    size_t length = 0;

    rapidjson::StringBuffer sb;
    Writer writer(sb);

    for (size_t i = 0; i < kItoaTrialCount; i++) {
        for (size_t j = 0; j < randvalCount; j++) {
            writer.WriteInt64(randval[j] * randval[j]);
            length += sb.GetSize();
            sb.Clear();
        }
    }
    OUTPUT_LENGTH(length);
}

template <typename Writer>
void itoa64_Writer_InsituStringStream() {
    size_t length = 0;

    char buffer[32];
    Writer writer;
    for (size_t i = 0; i < kItoaTrialCount; i++) {
        for (size_t j = 0; j < randvalCount; j++) {
            rapidjson::InsituStringStream ss(buffer);
            writer.Reset(ss);
            char* begin = ss.PutBegin();
            writer.WriteInt64(randval[j] * randval[j]);
            length += ss.PutEnd(begin);
        }
    }
    OUTPUT_LENGTH(length);
};

// Full specialization for InsituStringStream to prevent memory copying 
// (normally we will not use InsituStringStream for writing, just for testing)

namespace rapidjson {

template<>
bool rapidjson::Writer<InsituStringStream>::WriteInt(int i) {
    char *buffer = os_->Push(11);
    const char* end = internal::i32toa(i, buffer);
    os_->Pop(11 - (end - buffer));
    return true;
}

template<>
bool Writer<InsituStringStream>::WriteUint(unsigned u) {
    char *buffer = os_->Push(10);
    const char* end = internal::u32toa(u, buffer);
    os_->Pop(10 - (end - buffer));
    return true;
}

template<>
bool Writer<InsituStringStream>::WriteInt64(int64_t i64) {
    char *buffer = os_->Push(21);
    const char* end = internal::i64toa(i64, buffer);
    os_->Pop(21 - (end - buffer));
    return true;
}

template<>
bool Writer<InsituStringStream>::WriteUint64(uint64_t u) {
    char *buffer = os_->Push(20);
    const char* end = internal::u64toa(u, buffer);
    os_->Pop(20 - (end - buffer));
    return true;
}

} // namespace rapidjson

TEST_F(Misc, itoa_Writer_StringBufferVerify) { itoa_Writer_StringBufferVerify<rapidjson::Writer<rapidjson::StringBuffer> >(); }
TEST_F(Misc, itoa_Writer1_StringBufferVerify) { itoa_Writer_StringBufferVerify<Writer1<rapidjson::StringBuffer> >(); }
TEST_F(Misc, itoa_Writer2_StringBufferVerify) { itoa_Writer_StringBufferVerify<Writer2<rapidjson::StringBuffer> >(); }
TEST_F(Misc, itoa_Writer3_StringBufferVerify) { itoa_Writer_StringBufferVerify<Writer3<rapidjson::StringBuffer> >(); }
TEST_F(Misc, itoa_Writer4_StringBufferVerify) { itoa_Writer_StringBufferVerify<Writer4<rapidjson::StringBuffer> >(); }
TEST_F(Misc, itoa_Writer_InsituStringStreamVerify) { itoa_Writer_InsituStringStreamVerify<rapidjson::Writer<rapidjson::InsituStringStream> >(); }
TEST_F(Misc, itoa_Writer1_InsituStringStreamVerify) { itoa_Writer_InsituStringStreamVerify<Writer1<rapidjson::InsituStringStream> >(); }
TEST_F(Misc, itoa_Writer2_InsituStringStreamVerify) { itoa_Writer_InsituStringStreamVerify<Writer2<rapidjson::InsituStringStream> >(); }
TEST_F(Misc, itoa_Writer3_InsituStringStreamVerify) { itoa_Writer_InsituStringStreamVerify<Writer3<rapidjson::InsituStringStream> >(); }
TEST_F(Misc, itoa_Writer4_InsituStringStreamVerify) { itoa_Writer_InsituStringStreamVerify<Writer4<rapidjson::InsituStringStream> >(); }
TEST_F(Misc, itoa_Writer_StringBuffer) { itoa_Writer_StringBuffer<rapidjson::Writer<rapidjson::StringBuffer> >(); }
TEST_F(Misc, itoa_Writer1_StringBuffer) { itoa_Writer_StringBuffer<Writer1<rapidjson::StringBuffer> >(); }
TEST_F(Misc, itoa_Writer2_StringBuffer) { itoa_Writer_StringBuffer<Writer2<rapidjson::StringBuffer> >(); }
TEST_F(Misc, itoa_Writer3_StringBuffer) { itoa_Writer_StringBuffer<Writer3<rapidjson::StringBuffer> >(); }
TEST_F(Misc, itoa_Writer4_StringBuffer) { itoa_Writer_StringBuffer<Writer4<rapidjson::StringBuffer> >(); }
TEST_F(Misc, itoa_Writer_InsituStringStream) { itoa_Writer_InsituStringStream<rapidjson::Writer<rapidjson::InsituStringStream> >(); }
TEST_F(Misc, itoa_Writer1_InsituStringStream) { itoa_Writer_InsituStringStream<Writer1<rapidjson::InsituStringStream> >(); }
TEST_F(Misc, itoa_Writer2_InsituStringStream) { itoa_Writer_InsituStringStream<Writer2<rapidjson::InsituStringStream> >(); }
TEST_F(Misc, itoa_Writer3_InsituStringStream) { itoa_Writer_InsituStringStream<Writer3<rapidjson::InsituStringStream> >(); }
TEST_F(Misc, itoa_Writer4_InsituStringStream) { itoa_Writer_InsituStringStream<Writer4<rapidjson::InsituStringStream> >(); }

TEST_F(Misc, itoa64_Writer_StringBufferVerify) { itoa64_Writer_StringBufferVerify<rapidjson::Writer<rapidjson::StringBuffer> >(); }
TEST_F(Misc, itoa64_Writer1_StringBufferVerify) { itoa64_Writer_StringBufferVerify<Writer1<rapidjson::StringBuffer> >(); }
TEST_F(Misc, itoa64_Writer2_StringBufferVerify) { itoa64_Writer_StringBufferVerify<Writer2<rapidjson::StringBuffer> >(); }
TEST_F(Misc, itoa64_Writer3_StringBufferVerify) { itoa64_Writer_StringBufferVerify<Writer3<rapidjson::StringBuffer> >(); }
TEST_F(Misc, itoa64_Writer4_StringBufferVerify) { itoa64_Writer_StringBufferVerify<Writer4<rapidjson::StringBuffer> >(); }
TEST_F(Misc, itoa64_Writer_InsituStringStreamVerify) { itoa64_Writer_InsituStringStreamVerify<rapidjson::Writer<rapidjson::InsituStringStream> >(); }
TEST_F(Misc, itoa64_Writer1_InsituStringStreamVerify) { itoa64_Writer_InsituStringStreamVerify<Writer1<rapidjson::InsituStringStream> >(); }
TEST_F(Misc, itoa64_Writer2_InsituStringStreamVerify) { itoa64_Writer_InsituStringStreamVerify<Writer2<rapidjson::InsituStringStream> >(); }
TEST_F(Misc, itoa64_Writer3_InsituStringStreamVerify) { itoa64_Writer_InsituStringStreamVerify<Writer3<rapidjson::InsituStringStream> >(); }
TEST_F(Misc, itoa64_Writer4_InsituStringStreamVerify) { itoa64_Writer_InsituStringStreamVerify<Writer4<rapidjson::InsituStringStream> >(); }
TEST_F(Misc, itoa64_Writer_StringBuffer) { itoa64_Writer_StringBuffer<rapidjson::Writer<rapidjson::StringBuffer> >(); }
TEST_F(Misc, itoa64_Writer1_StringBuffer) { itoa64_Writer_StringBuffer<Writer1<rapidjson::StringBuffer> >(); }
TEST_F(Misc, itoa64_Writer2_StringBuffer) { itoa64_Writer_StringBuffer<Writer2<rapidjson::StringBuffer> >(); }
TEST_F(Misc, itoa64_Writer3_StringBuffer) { itoa64_Writer_StringBuffer<Writer3<rapidjson::StringBuffer> >(); }
TEST_F(Misc, itoa64_Writer4_StringBuffer) { itoa64_Writer_StringBuffer<Writer4<rapidjson::StringBuffer> >(); }
TEST_F(Misc, itoa64_Writer_InsituStringStream) { itoa64_Writer_InsituStringStream<rapidjson::Writer<rapidjson::InsituStringStream> >(); }
TEST_F(Misc, itoa64_Writer1_InsituStringStream) { itoa64_Writer_InsituStringStream<Writer1<rapidjson::InsituStringStream> >(); }
TEST_F(Misc, itoa64_Writer2_InsituStringStream) { itoa64_Writer_InsituStringStream<Writer2<rapidjson::InsituStringStream> >(); }
TEST_F(Misc, itoa64_Writer3_InsituStringStream) { itoa64_Writer_InsituStringStream<Writer3<rapidjson::InsituStringStream> >(); }
TEST_F(Misc, itoa64_Writer4_InsituStringStream) { itoa64_Writer_InsituStringStream<Writer4<rapidjson::InsituStringStream> >(); }

#endif // TEST_MISC
