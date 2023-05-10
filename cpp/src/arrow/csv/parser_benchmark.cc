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

#include "benchmark/benchmark.h"

#include <memory>
#include <sstream>
#include <string>
#include <string_view>

#include "arrow/csv/chunker.h"
#include "arrow/csv/options.h"
#include "arrow/csv/parser.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace csv {

struct Example {
  int32_t num_rows;
  const char* csv_rows;
};

const Example quoted_example{1, "abc,\"d,f\",12.34,\n"};
const Example escaped_example{1, "abc,d\\,f,12.34,\n"};

const Example flights_example{
    8,
    R"(2015,1,1,4,AA,2336,N3KUAA,LAX,PBI,0010,0002,-8,12,0014,280,279,263,2330,0737,4,0750,0741,-9,0,0,,,,,,
2015,1,1,4,US,840,N171US,SFO,CLT,0020,0018,-2,16,0034,286,293,266,2296,0800,11,0806,0811,5,0,0,,,,,,
2015,1,1,4,AA,258,N3HYAA,LAX,MIA,0020,0015,-5,15,0030,285,281,258,2342,0748,8,0805,0756,-9,0,0,,,,,,
2015,1,1,4,AS,135,N527AS,SEA,ANC,0025,0024,-1,11,0035,235,215,199,1448,0254,5,0320,0259,-21,0,0,,,,,,
2015,1,1,4,DL,806,N3730B,SFO,MSP,0025,0020,-5,18,0038,217,230,206,1589,0604,6,0602,0610,8,0,0,,,,,,
2015,1,1,4,NK,612,N635NK,LAS,MSP,0025,0019,-6,11,0030,181,170,154,1299,0504,5,0526,0509,-17,0,0,,,,,,
2015,1,1,4,US,2013,N584UW,LAX,CLT,0030,0044,14,13,0057,273,249,228,2125,0745,8,0803,0753,-10,0,0,,,,,,
2015,1,1,4,AA,1112,N3LAAA,SFO,DFW,0030,0019,-11,17,0036,195,193,173,1464,0529,3,0545,0532,-13,0,0,,,,,,
)"};

// NOTE: quoted
const Example vehicles_example{
    2,
    R"(7088743681,https://greensboro.craigslist.org/ctd/d/cary-2004-honda-element-lx-4dr-suv/7088743681.html,greensboro,https://greensboro.craigslist.org,3995,2004,honda,element,,,gas,212526,clean,automatic,5J6YH18314L006498,fwd,,SUV,orange,https://images.craigslist.org/00E0E_eAUnhFF86M4_600x450.jpg,"2004 Honda Element LX 4dr SUV     Offered by: Best Import Auto Sales Inc — (919) 800-0650 — $3,995     EXCELLENT SHAPE INSIDE AND OUT FULLY SERVICED AND READY TO GO ,RUNS AND DRIVES PERFECT ,PLEASE CALL OR TEXT 919 454 4848 OR CALL 919 380 0380 IF INTERESTED.   Best Import Auto Sales Inc    Year: 2004 Make: Honda Model: Element Series: LX 4dr SUV VIN: 5J6YH18314L006498 Stock #: 4L006498 Condition: Used Mileage: 212,526  Exterior: Orange Interior: Black Body: SUV Transmission: Automatic 4-Speed Engine: 2.4L I4      **** Best Import Auto Sales Inc. 🚘 Raleigh Auto Dealer *****  ⚡️⚡️⚡️ Call Or Text (919) 800-0650 ⚡️⚡️⚡️  ✅ - We can arrange Financing Options with most banks and credit unions!!!!     ✅ Extended Warranties Available on most vehicles!! ""Call To Inquire""  ✅ Full Service ASE-Certified Shop Onsite!       More vehicle details: best-import-auto-sales-inc.hammerwebsites.net/v/3kE08kSD     Address: 1501 Buck Jones Rd Raleigh, NC 27606   Phone: (919) 800-0650     Website: www.bestimportsonline.com      📲 ☎️ Call or text (919) 800-0650 for quick answers to your questions about this Honda Element Your message will always be answered by a real human — never an automated system.     Disclaimer: Best Import Auto Sales Inc will never sell, share, or spam your mobile number. Standard text messaging rates may apply.       2004 Honda Element LX 4dr SUV   6fbc204ebd7e4a32a30dcf2c8c3bcdea",,nc,35.7636,-78.7443
  7088744126,https://greensboro.craigslist.org/cto/d/greensboro-2011-jaguar-xf-premier/7088744126.html,greensboro,https://greensboro.craigslist.org,9500,2011,jaguar,xf,excellent,,gas,85000,clean,automatic,,,,,blue,https://images.craigslist.org/00505_f22HGItCRpc_600x450.jpg,"2011 jaguar XF premium - estate sale. Retired lady executive. Like new, garaged and maintained. Very nice leather, heated seats, electric sunroof, metallic blue paint. 85K miles bumper-to-bumper warranty. Premium radio sound system. Built-in phone connection. Please call  show contact info  cell or  show contact info .  Asking Price $9500",,nc,36.1032,-79.8794
)"};

const Example stocks_example{
    3,
    R"(2,2010-01-27 00:00:00,002204,华锐铸钢,536498.0,135378.0,2652784.2001924426,14160629.45,5.382023337513902,5.288274712474071,5.382023337513902,5.341540976701248,,5.338025403262254,1.01364599,0.21306505690870553
3,2010-02-05 00:00:00,600266,北京城建,1122615.0,1122615.0,8102476.086666377,57695471.0,7.236029036381633,7.025270909108382,7.170459841229955,7.095523618199466,,7.120720923193468,2.3025570905818964,0.4683513939405588
4,2010-01-04 00:00:00,600289,亿阳信通,602926.359,602926.359,16393247.138998777,167754890.0,10.381817699665978,9.960037526145015,10.092597009251604,10.321563389162982,,10.233170315655089,4.436963485334562,0.6025431050299465
)"};

static constexpr int32_t kNumRows = 10000;

static std::string BuildCSVData(const Example& example) {
  std::stringstream ss;
  for (int32_t i = 0; i < kNumRows; i += example.num_rows) {
    ss << example.csv_rows;
  }
  return ss.str();
}

static void BenchmarkCSVChunking(benchmark::State& state,  // NOLINT non-const reference
                                 const std::string& csv, ParseOptions options) {
  auto chunker = MakeChunker(options);
  auto block = std::make_shared<Buffer>(std::string_view(csv));

  while (state.KeepRunning()) {
    std::shared_ptr<Buffer> whole, partial;
    ABORT_NOT_OK(chunker->Process(block, &whole, &partial));
    auto size = whole->size();
    benchmark::DoNotOptimize(size);
  }

  state.SetBytesProcessed(state.iterations() * csv.length());
}

static void BenchmarkCSVChunking(benchmark::State& state,  // NOLINT non-const reference
                                 const Example& example, ParseOptions options) {
  auto csv = BuildCSVData(example);
  BenchmarkCSVChunking(state, csv, options);
}

static void ChunkCSVQuotedBlock(benchmark::State& state) {  // NOLINT non-const reference
  auto csv = BuildCSVData(quoted_example);
  auto options = ParseOptions::Defaults();
  options.quoting = true;
  options.escaping = false;
  options.newlines_in_values = true;

  BenchmarkCSVChunking(state, csv, options);
}

static void ChunkCSVEscapedBlock(benchmark::State& state) {  // NOLINT non-const reference
  auto csv = BuildCSVData(escaped_example);
  auto options = ParseOptions::Defaults();
  options.quoting = false;
  options.escaping = true;
  options.newlines_in_values = true;

  BenchmarkCSVChunking(state, csv, options);
}

static void ChunkCSVNoNewlinesBlock(
    benchmark::State& state) {  // NOLINT non-const reference
  auto csv = BuildCSVData(escaped_example);
  auto options = ParseOptions::Defaults();
  options.quoting = true;
  options.escaping = false;
  options.newlines_in_values = false;

  BenchmarkCSVChunking(state, csv, options);
  // Provides better regression stability with timings rather than bogus
  // bandwidth.
  state.SetBytesProcessed(0);
}

static void ChunkCSVFlightsExample(
    benchmark::State& state) {  // NOLINT non-const reference
  auto options = ParseOptions::Defaults();
  options.newlines_in_values = true;

  BenchmarkCSVChunking(state, flights_example, options);
}

static void ChunkCSVVehiclesExample(
    benchmark::State& state) {  // NOLINT non-const reference
  auto options = ParseOptions::Defaults();
  options.quoting = true;
  options.escaping = false;
  options.newlines_in_values = true;

  BenchmarkCSVChunking(state, vehicles_example, options);
}

static void ChunkCSVStocksExample(
    benchmark::State& state) {  // NOLINT non-const reference
  auto options = ParseOptions::Defaults();
  options.newlines_in_values = true;

  BenchmarkCSVChunking(state, stocks_example, options);
}

static void BenchmarkCSVParsing(benchmark::State& state,  // NOLINT non-const reference
                                const std::string& csv, int32_t num_rows,
                                ParseOptions options) {
  BlockParser parser(options, -1, num_rows + 1);

  while (state.KeepRunning()) {
    uint32_t parsed_size = 0;
    ABORT_NOT_OK(parser.Parse(std::string_view(csv), &parsed_size));

    // Include performance of visiting the parsed values, as that might
    // vary depending on the parser's internal data structures.
    bool dummy_quoted = false;
    uint32_t dummy_size = 0;
    auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) {
      dummy_size += size;
      dummy_quoted ^= quoted;
      return Status::OK();
    };
    for (int32_t col = 0; col < parser.num_cols(); ++col) {
      ABORT_NOT_OK(parser.VisitColumn(col, visit));
      benchmark::DoNotOptimize(dummy_size);
      benchmark::DoNotOptimize(dummy_quoted);
    }
  }

  state.SetBytesProcessed(state.iterations() * csv.size());
}

static void BenchmarkCSVParsing(benchmark::State& state,  // NOLINT non-const reference
                                const Example& example, ParseOptions options) {
  auto csv = BuildCSVData(example);
  BenchmarkCSVParsing(state, csv, kNumRows, options);
}

static void ParseCSVQuotedBlock(benchmark::State& state) {  // NOLINT non-const reference
  auto options = ParseOptions::Defaults();
  options.quoting = true;
  options.escaping = false;

  BenchmarkCSVParsing(state, quoted_example, options);
}

static void ParseCSVEscapedBlock(benchmark::State& state) {  // NOLINT non-const reference
  auto options = ParseOptions::Defaults();
  options.quoting = false;
  options.escaping = true;

  BenchmarkCSVParsing(state, escaped_example, options);
}

static void ParseCSVFlightsExample(
    benchmark::State& state) {  // NOLINT non-const reference
  BenchmarkCSVParsing(state, flights_example, ParseOptions::Defaults());
}

static void ParseCSVVehiclesExample(
    benchmark::State& state) {  // NOLINT non-const reference
  auto options = ParseOptions::Defaults();
  options.quoting = true;
  options.escaping = false;

  BenchmarkCSVParsing(state, vehicles_example, options);
}

static void ParseCSVStocksExample(
    benchmark::State& state) {  // NOLINT non-const reference
  BenchmarkCSVParsing(state, stocks_example, ParseOptions::Defaults());
}

BENCHMARK(ChunkCSVQuotedBlock);
BENCHMARK(ChunkCSVEscapedBlock);
BENCHMARK(ChunkCSVNoNewlinesBlock);
BENCHMARK(ChunkCSVFlightsExample);
BENCHMARK(ChunkCSVVehiclesExample);
BENCHMARK(ChunkCSVStocksExample);

BENCHMARK(ParseCSVQuotedBlock);
BENCHMARK(ParseCSVEscapedBlock);
BENCHMARK(ParseCSVFlightsExample);
BENCHMARK(ParseCSVVehiclesExample);
BENCHMARK(ParseCSVStocksExample);

}  // namespace csv
}  // namespace arrow
