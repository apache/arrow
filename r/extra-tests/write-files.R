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

library(arrow)

if (!dir.exists("extra-tests/files")) {
  dir.create("extra-tests/files")
}

source("tests/testthat/helper-data.R")

write_parquet(example_with_metadata, "extra-tests/files/ex_data.parquet")

for (comp in c("lz4", "uncompressed", "zstd")) {
  if (!codec_is_available(comp)) break

  name <- paste0("extra-tests/files/ex_data_", comp, ".feather")
  write_feather(example_with_metadata, name, compression = comp)
}

example_with_metadata_v1 <- example_with_metadata
example_with_metadata_v1$c <- NULL
write_feather(example_with_metadata_v1, "extra-tests/files/ex_data_v1.feather", version = 1)

write_ipc_stream(example_with_metadata, "extra-tests/files/ex_data.stream")

write_parquet(example_with_extra_metadata, "extra-tests/files/ex_data_extra_metadata.parquet")
