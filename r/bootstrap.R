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



# exclude_patterns are regular expressions so use ^ to match top-level
# directories only. Otherwise 'dir' will match 'src/dir' and 'dir/file'.
rsync <- function(src_dir, dest_dir, exclude_patterns) {
  all_files <- list.files(src_dir, recursive = TRUE)

  # Filter out excluded patterns
  for (pattern in exclude_patterns) {
    all_files <- all_files[!grepl(pattern, all_files)]
  }

  files_to_vendor_src <- file.path(src_dir, all_files)
  files_to_vendor_dst <- file.path(dest_dir, all_files)

  # Recreate the directory structure
  dst_dirs <- unique(dirname(files_to_vendor_dst))
  for (dst_dir in dst_dirs) {
    if (!dir.exists(dst_dir)) {
      dir.create(dst_dir, recursive = TRUE)
    }
  }

  # Copy the files
  if (all(file.copy(files_to_vendor_src, files_to_vendor_dst, overwrite = TRUE))) {
    cat("All files successfully copied to tools/cpp\n")
  } else {
    stop("Failed to vendor all files")
  }
}


if (dir.exists("../cpp")) {
  unlink("tools/cpp", recursive = TRUE)
  rsync("../cpp",
    "tools/cpp",
    exclude_patterns = c(
      "^apidoc",
      "^build/",
      "^build-support/boost-",
      "^examples",
      "^submodules",
      "^src/gandiva",
      "^src/jni",
      "^src/skyhook",
      "^src/arrow/flight/sql/odbc",
      "_test\\.cc"
    )
  )

  # The thirdparty cmake expects .env, NOTICE.txt, and LICENSE.txt to be available one
  # level up from cpp/ we must rename .env to dotenv and then replace references to it
  # in cpp/CMakeLists.txt, because R CMD will produce a Note otherwise.
  file.copy(from = c("../NOTICE.txt", to = "../LICENSE.txt"), "tools/", overwrite = TRUE)
  file.copy("../.env", "tools/dotenv", overwrite = TRUE)

  cmake_lists <- readLines("tools/cpp/CMakeLists.txt")
  cmake_lists <- gsub("\\.env", "dotenv", cmake_lists)
  writeLines(cmake_lists, "tools/cpp/CMakeLists.txt")
} else {
  cli::cli_alert_warning("Arrow C++ sources not found, skipping bootstrap.")
}
