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

library(tidyverse)
library(arrow)

# meta data
(t1 <- int32())
(t2 <- utf8())
(t5 <- timestamp(unit = TimeUnit$MILLI))

# lists
list_of(t1)

# shema
schema(x = int32(), y = float64())

# :scream_cat:
#
# pa.schema(
#   [
#      pa.field('x', pa.int32()),
#      pa.field('y', pa.float64())
#   ]
# )
#

schema(x = int32(), y = list_of(float64()))

#------- arrays

# arr = pa.array([1, 2, 3])
arr <- array(1:3, 5:80)
arr
arr$as_vector()

#------- read_arrow / write_arrow
tbl <- tibble(x=1:10, y=rnorm(10))
write_arrow(tbl, "/tmp/test.arrow")
readr::write_rds(tbl, "/tmp/test.rds")
fs::file_info(c("/tmp/test.arrow", "/tmp/test.rds"))

(data <- read_arrow("/tmp/test.arrow"))

# tibble <-> arrow::RecordBatch
(batch <- record_batch(tbl))
batch$num_columns()
batch$num_rows()
batch$to_file("/tmp/test")
readBin("/tmp/test", what = raw(), n = 1000)
batch$schema()
all.equal(tbl, data)

batch <- read_record_batch("/tmp/test")
batch$schema()
batch$column(0)
batch$column(0)$as_vector()

as_tibble(batch)

# tibble <-> arrow::Table
tab <- arrow::table(tbl)
tab
tab$schema()
tab$num_columns()
tab$num_rows()

# read_arrow, write_arrow
tbl <- tibble(x = rnorm(20), y = seq_len(20))
write_arrow(tbl, tf)

