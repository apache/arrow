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

oxford_paste <- function(x, conjunction = "and") {
  if (is.character(x)) {
    x <- paste0('"', x, '"')
  }
  if (length(x) < 2) {
    return(x)
  }
  x[length(x)] <- paste(conjunction, x[length(x)])
  if (length(x) > 2) {
    return(paste(x, collapse = ", "))
  } else {
    return(paste(x, collapse = " "))
  }
}

assert_is <- function(object, class) {
  msg <- paste(substitute(object), "must be a", oxford_paste(class, "or"))
  assert_that(inherits(object, class), msg = msg)
}
