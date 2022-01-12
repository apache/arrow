# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.



#' Round a temporal object
#'
#' @param x Scalar or Array to be rounded
#' @param unit string specifying the calendar unit (default = "second")
#'
#' @return Scalar or Array containing rounded times
#' @export
#'
#' @examples
#' timey_wimeys <- Array$create(Sys.time() + rnorm(10, sd = 360))
#' round_temporal(timey_wimeys)
#' round_temporal(timey_wimeys, unit = "minute")
round_temporal <- function(x, unit = "second") {
  unit <- lookup_calendar_unit(unit)
  call_function("round_temporal", x, options = list(unit = unit))
}

lookup_calendar_unit <- function(unit) {
  switch(unit,
    "nanosecond" = 0L,
    "microsecond" = 1L,
    "millisecond" = 2L,
    "second" = 3L,
    "minute" = 4L,
    "hour" = 5L,
    "day" = 6L,
    "week" = 7L,
    "month" = 8L,
    "quarter" = 9L,
    "year" = 10L
  )
}



