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

args <- commandArgs(TRUE)
VERSION <- args[1]
if(!file.exists(sprintf("windows/arrow-%s/include/arrow/api.h", VERSION))){
  if(length(args) > 1){
    # Arg 2 would be the path/to/lib.zip
    localfile <- args[2]
    cat(sprintf("*** Using RWINLIB_LOCAL %s\n", localfile))
    if(!file.exists(localfile)){
      cat(sprintf("*** %s does not exist; build will fail\n", localfile))
    }
    file.copy(localfile, "lib.zip")
  } else {
    # Download static arrow from rwinlib
    if(getRversion() < "3.3.0") setInternet2()
    download.file(sprintf("https://github.com/rwinlib/arrow/archive/v%s.zip", VERSION), "lib.zip", quiet = TRUE)
  }
  dir.create("windows", showWarnings = FALSE)
  unzip("lib.zip", exdir = "windows")
  unlink("lib.zip")
}
