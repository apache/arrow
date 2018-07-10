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
timestamp <- function(unit, timezone){
  if( missing(timezone)){
    timestamp1(unit)
  } else {
    timestamp2(unit, timezone)
  }
}

#' @importFrom glue glue
#' @export
`print.arrow::DataType` <- function(x, ...){
  cat( glue( "DataType({s})", s = DataType_ToString(x)))
  invisible(x)
}

#' @export
`print.arrow::StructType` <- function(x, ...){
  cat( glue( "StructType({s})", s = DataType_ToString(x)))
  invisible(x)
}

#' @export
`print.arrow::ListType` <- function(x, ...){
  cat( glue( "ListType({s})", s = DataType_ToString(x)))
  invisible(x)
}

#' @export
`print.arrow::Schema` <- function(x, ...){
  cat( glue( "{s}", s = Schema_ToString(x)))
  invisible(x)
}
