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

#include "gandiva/convert_timezone_holder.h"

namespace gandiva {
// Map of all Timezones abbreviations in sys.timezone_abbrevs to its offsets.
std::unordered_map<std::string, std::string> ConvertTimezoneHolder::abbrv_tz(
    {{"ACDT", "+10:30"},  {"ACSST", "+10:30"},  {"ACST", "+09:30"},
     {"ACT", "-05:00"},   {"ACWST", "+08:45"},  {"ADT", "-03:00"},
     {"AEDT", "+11:00"},  {"AESST", "+11:00"},  {"AEST", "+10:00"},
     {"AFT", "+04:30"},   {"AKDT", "-08:00"},   {"AKST", "-09:00"},
     {"ALMST", "+07:00"}, {"ALMT", "+06:00"},   {"AMST", "+04:00"},
     {"AMT", "-04:00"},   {"ANAST", "+12:00"},  {"ANAT", "+12:00"},
     {"ARST", "-03:00"},  {"ART", "-03:00"},    {"AST", "-04:00"},
     {"AWSST", "+09:00"}, {"AWST", "+08:00"},   {"AZOST", "+00:00"},
     {"AZOT", "-01:00"},  {"AZST", "+04:00"},   {"AZT", "+04:00"},
     {"BDST", "+02:00"},  {"BDT", "+06:00"},    {"BNT", "+08:00"},
     {"BORT", "+08:00"},  {"BOT", "-04:00"},    {"BRA", "-03:00"},
     {"BRST", "-02:00"},  {"BRT", "-03:00"},    {"BST", "+01:00"},
     {"BTT", "+06:00"},   {"CADT", "+10:30"},   {"CAST", "+09:30"},
     {"CCT", "+08:00"},   {"CDT", "-05:00"},    {"CEST", "+02:00"},
     {"CET", "+01:00"},   {"CETDST", "+02:00"}, {"CHADT", "+13:45"},
     {"CHAST", "+12:45"}, {"CHUT", "+10:00"},   {"CKT", "-10:00"},
     {"CLST", "-03:00"},  {"CLT", "-04:00"},    {"COT", "-05:00"},
     {"CST", "-06:00"},   {"CXT", "+07:00"},    {"DAVT", "+07:00"},
     {"DDUT", "+10:00"},  {"EASST", "-06:00"},  {"EAST", "-06:00"},
     {"EAT", "+03:00"},   {"EDT", "-04:00"},    {"EEST", "+03:00"},
     {"EET", "+02:00"},   {"EETDST", "+03:00"}, {"EGST", "+00:00"},
     {"EGT", "-01:00"},   {"EST", "-05:00"},    {"FET", "+03:00"},
     {"FJST", "+13:00"},  {"FJT", "+12:00"},    {"FKST", "-03:00"},
     {"FKT", "-03:00"},   {"FNST", "-01:00"},   {"FNT", "-02:00"},
     {"GALT", "-06:00"},  {"GAMT", "-09:00"},   {"GEST", "+04:00"},
     {"GET", "+04:00"},   {"GFT", "-03:00"},    {"GILT", "+12:00"},
     {"GMT", "+00:00"},   {"GYT", "-04:00"},    {"HKT", "+08:00"},
     {"HST", "-10:00"},   {"ICT", "+07:00"},    {"IDT", "+03:00"},
     {"IOT", "+06:00"},   {"IRKST", "+08:00"},  {"IRKT", "+08:00"},
     {"IRT", "+03:30"},   {"IST", "+02:00"},    {"JAYT", "+09:00"},
     {"JST", "+09:00"},   {"KDT", "+10:00"},    {"KGST", "+06:00"},
     {"KGT", "+06:00"},   {"KOST", "+11:00"},   {"KRAST", "+07:00"},
     {"KRAT", "+07:00"},  {"KST", "+09:00"},    {"LHDT", "+10:30"},
     {"LHST", "+10:30"},  {"LIGT", "+10:00"},   {"LINT", "+14:00"},
     {"LKT", "+05:30"},   {"MAGST", "+11:00"},  {"MAGT", "+11:00"},
     {"MART", "-09:30"},  {"MAWT", "+05:00"},   {"MDT", "-06:00"},
     {"MEST", "+02:00"},  {"MET", "+01:00"},    {"METDST", "+02:00"},
     {"MEZ", "+01:00"},   {"MHT", "+12:00"},    {"MMT", "+06:30"},
     {"MPT", "+10:00"},   {"MSD", "+04:00"},    {"MSK", "+03:00"},
     {"MST", "-07:00"},   {"MUST", "+05:00"},   {"MUT", "+04:00"},
     {"MVT", "+05:00"},   {"MYT", "+08:00"},    {"NDT", "-02:30"},
     {"NFT", "-03:30"},   {"NOVST", "+07:00"},  {"NOVT", "+07:00"},
     {"NPT", "+05:45"},   {"NST", "-03:30"},    {"NUT", "-11:00"},
     {"NZDT", "+13:00"},  {"NZST", "+12:00"},   {"NZT", "+12:00"},
     {"OMSST", "+06:00"}, {"OMST", "+06:00"},   {"PDT", "-07:00"},
     {"PET", "-05:00"},   {"PETST", "+12:00"},  {"PETT", "+12:00"},
     {"PGT", "+10:00"},   {"PHOT", "+13:00"},   {"PHT", "+08:00"},
     {"PKST", "+06:00"},  {"PMDT", "-02:00"},   {"PKT", "+05:00"},
     {"PMDT", "-02:00"},  {"PMST", "-03:00"},   {"PONT", "+11:00"},
     {"PST", "-08:00"},   {"PWT", "+09:00"},    {"SCT", "+04:00"},
     {"SAST", "+02:00"},  {"SADT", "+10:30"},   {"RET", "+04:00"},
     {"PYT", "-04:00"},   {"PYST", "-03:00"},   {"ULAT", "+08:00"},
     {"ULAST", "+09:00"}, {"UCT", "+00:00"},    {"TVT", "+12:00"},
     {"TRUT", "+10:00"},  {"TOT", "+13:00"},    {"TMT", "+05:00"},
     {"TKT", "+13:00"},   {"TFT", "+05:00"},    {"TAHT", "-10:00"},
     {"SGT", "+08:00"},   {"VUT", "+11:00"},    {"VOLT", "+03:00"},
     {"VLAT", "+10:00"},  {"VLAST", "+10:00"},  {"VET", "-04:00"},
     {"UZT", "+05:00"},   {"UZST", "+06:00"},   {"UYT", "-03:00"},
     {"UYST", "-02:00"},  {"UTC", "+00:00"},    {"UT", "+00:00"},
     {"WGST", "-02:00"},  {"WFT", "+12:00"},    {"WETDST", "+01:00"},
     {"WET", "+00:00"},   {"WDT", "+09:00"},    {"WAT", "+01:00"},
     {"WAST", "+07:00"},  {"WAKT", "+12:00"},   {"WADT", "+08:00"},
     {"ZULU", "+00:00"},  {"Z", "+00:00"},      {"YEKT", "+05:00"},
     {"YEKST", "+06:00"}, {"YAPT", "+10:00"},   {"YAKT", "+09:00"},
     {"YAKST", "+09:00"}, {"XJT", "+06:00"},    {"WGT", "-03:00"}});

Status ConvertTimezoneHolder::Make(const FunctionNode& node,
                                   std::shared_ptr<ConvertTimezoneHolder>* holder) {
  ARROW_RETURN_IF(
      node.children().size() != 3,
      Status::Invalid("'convert_timezone' function requires three parameters"));

  auto srcliteral = dynamic_cast<LiteralNode*>(node.children().at(1).get());
  auto dstliteral = dynamic_cast<LiteralNode*>(node.children().at(2).get());
  ARROW_RETURN_IF(srcliteral == nullptr || srcliteral == nullptr,
                  Status::Invalid("'convert_timezone' function requires literals as "
                                  "first and second parameters"));

  return Make(arrow::util::get<std::string>(srcliteral->holder()),
              arrow::util::get<std::string>(dstliteral->holder()), holder);
}

Status ConvertTimezoneHolder::Make(const std::string& srcTz, const std::string& destTz,
                                   std::shared_ptr<ConvertTimezoneHolder>* holder) {
  auto tzholder =
      std::shared_ptr<ConvertTimezoneHolder>(new ConvertTimezoneHolder(srcTz, destTz));
  ARROW_RETURN_IF(
      !tzholder->ok,
      Status::Invalid("Couldn't find one of the timezones given or it's invalid."));

  *holder = tzholder;
  return Status::OK();
}
}  // namespace gandiva