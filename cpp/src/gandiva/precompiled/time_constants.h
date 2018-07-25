// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef TIME_CONSTANTS_H
#define TIME_CONSTANTS_H

#define MILLIS_IN_SEC (1000)
#define MILLIS_IN_MIN (60 * MILLIS_IN_SEC)
#define MILLIS_IN_HOUR (60 * MILLIS_IN_MIN)
#define MILLIS_IN_DAY (24 * MILLIS_IN_HOUR)
#define MILLIS_IN_WEEK (7 * MILLIS_IN_DAY)

#define MILLIS_TO_SEC(millis) ((millis) / MILLIS_IN_SEC)
#define MILLIS_TO_MINS(millis) ((millis) / MILLIS_IN_MIN)
#define MILLIS_TO_HOUR(millis) ((millis) / MILLIS_IN_HOUR)
#define MILLIS_TO_DAY(millis) ((millis) / MILLIS_IN_DAY)
#define MILLIS_TO_WEEK(millis) ((millis) / MILLIS_IN_WEEK)

#endif  // TIME_CONSTANTS_H
