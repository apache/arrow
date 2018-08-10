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

#' @export
`$.arrow-enum` <- function(x, y){
  structure(unclass(x)[[y]], class = class(x))
}

#' @export
`print.arrow-enum` <- function(x, ...){
  NextMethod()
}

enum <- function(class, ...){
  structure(list(...), class = c( class, "arrow-enum" ))
}


#' @export
TimeUnit <- enum("arrow::TimeUnit::type", SECOND = 0L, MILLI = 1L, MICRO = 2L, NANO = 3L)

#' @export
DateUnit <- enum("arrow::DateUnit", DAY = 0L, MILLI = 1L)
