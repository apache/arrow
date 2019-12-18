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
dst_dir <- paste0("libarrow/arrow-", VERSION)
if (!file.exists(paste0(dst_dir, "/include/arrow/api.h"))) {
  if (length(args) > 1) {
    # Arg 2 would be the path/to/lib.zip
    localfile <- args[2]
    cat(sprintf("*** Using LOCAL_LIBARROW %s\n", localfile))
    if(!file.exists(localfile)){
      cat(sprintf("*** %s does not exist; build will fail\n", localfile))
    }
    file.copy(localfile, "lib.zip")
  } else if (file.exists("../cpp/src/arrow/api.h")) {
    # We're in a git checkout of arrow. Build the C++ library
    cat("*** Building C++ libraries\n")
    # We'll need to compile R bindings with these libs, so delete any .o files
    system("rm src/*.o")
    system(sprintf("SOURCE_DIR=../cpp BUILD_DIR=libarrow/build DEST_DIR=%s inst/build_arrow_static.sh", dst_dir))
  } else {
    # Download static arrow
    # TODO: this should vary by distro/version
    # try(download.file("https://dl.bintray.com/nealrichardson/pyarrow-dev/libarrow.zip", "lib.zip", quiet = TRUE), silent = TRUE)
  }
  # Finish up for the cases where we got a zip file of the libs
  if (file.exists("lib.zip")) {
    dir.create(dst_dir, showWarnings = FALSE, recursive = TRUE)
    unzip("lib.zip", exdir = dst_dir)
    unlink("lib.zip")
  }
}
