---
layout: post
title: "Speeding up R and Apache Spark using Apache Arrow"
date: "2019-01-25 00:00:00 -0600"
author: javierluraschi
categories: [application]
---
<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

*[Javier Luraschi][1] is a software engineer at [RStudio][2]*

Support for Apache Arrow in Apache Spark with R is currently under active
development in the [sparklyr][3] and [SparkR][4] projects. This post explores early, yet
promising, performance improvements achieved when using R with [Apache Spark][5],
Arrow and `sparklyr`.

# Setup

Since this work is under active development, install `sparklyr` and
`arrow` from GitHub as follows:

```r
devtools::install_github("apache/arrow", subdir = "r", ref = "apache-arrow-0.12.0")
devtools::install_github("rstudio/sparklyr", ref = "apache-arrow-0.12.0")
```

In this benchmark, we will use [dplyr][6], but similar improvements can
be  expected from using [DBI][7], or [Spark DataFrames][8] in `sparklyr`.
The local Spark connection and dataframe with 10M numeric rows was
initialized as follows:

```r
library(sparklyr)
library(dplyr)

sc <- spark_connect(master = "local", config = list("sparklyr.shell.driver-memory" = "6g"))
data <- data.frame(y = runif(10^7, 0, 1))
```

# Copying

Currently, copying data to Spark using `sparklyr` is performed by persisting
data on-disk from R and reading it back from Spark. This was meant to be used
for small datasets since there are better tools to transfer data into
distributed storage systems. Nevertheless, many users have requested support to
transfer more data at fast speeds into Spark.

Using `arrow` with `sparklyr`, we can transfer data directly from R to
Spark without having to serialize this data in R or persist in disk.

The following example copies 10M rows from R into Spark using `sparklyr`
with and without `arrow`, there is close to a 16x improvement using `arrow`.

This benchmark uses the [microbenchmark][9] R package, which runs code
multiple times, provides stats on total execution time and plots each
excecution time to understand the distribution over each iteration.

```r
microbenchmark::microbenchmark(
  setup = library(arrow),
  arrow_on = {
    sparklyr_df <<- copy_to(sc, data, overwrite = T)
    count(sparklyr_df) %>% collect()
  },
  arrow_off = {
    if ("arrow" %in% .packages()) detach("package:arrow")
    sparklyr_df <<- copy_to(sc, data, overwrite = T)
    count(sparklyr_df) %>% collect()
  },
  times = 10
) %T>% print() %>% ggplot2::autoplot()
```
```
 Unit: seconds
      expr       min        lq       mean    median         uq       max neval
  arrow_on  3.011515  4.250025   7.257739  7.273011   8.974331  14.23325    10
 arrow_off 50.051947 68.523081 119.946947 71.898908 138.743419 390.44028    10
```

<div align="center">
<img src="{{ site.baseurl }}/img/arrow-r-spark-copying.png"
     alt="Copying data with R into Spark with and without Arrow"
     width="60%" class="img-responsive">
</div>

# Collecting

Similarly, `arrow` with `sparklyr` can now avoid deserializing data in R
while collecting data from Spark into R. These improvements are not as
significant as copying data since, `sparklyr` already collects data in
columnar format.

The following benchmark collects 10M rows from Spark into R and shows that
`arrow` can bring 3x improvements.

```r
microbenchmark::microbenchmark(
  setup = library(arrow),
  arrow_on = {
    collect(sparklyr_df)
  },
  arrow_off = {
    if ("arrow" %in% .packages()) detach("package:arrow")
    collect(sparklyr_df)
  },
  times = 10
) %T>% print() %>% ggplot2::autoplot()
```
```
Unit: seconds
      expr      min        lq      mean    median        uq       max neval
  arrow_on 4.520593  5.609812  6.154509  5.928099  6.217447  9.432221    10
 arrow_off 7.882841 13.358113 16.670708 16.127704 21.051382 24.373331    10
```

<div align="center">
<img src="{{ site.baseurl }}/img/arrow-r-spark-collecting.png"
     alt="Collecting data with R from Spark with and without Arrow"
     width="60%" class="img-responsive">
</div>

# Transforming

Today, custom transformations of data using R functions are performed in
`sparklyr` by moving data in row-format from Spark into an R process through a
socket connection, transferring data in row-format is inefficient since
multiple data types need to be deserialized over each row, then the data gets
converted to columnar format (R was originally designed to use columnar data),
once R finishes this computation, data is again converted to row-format,
serialized row-by-row and then sent back to Spark over the socket connection.

By adding support for `arrow` in `sparklyr`, it makes Spark perform the
row-format to column-format conversion in parallel in Spark. Data
is then transferred through the socket but no custom serialization takes place.
All the R process needs to do is copy this data from the socket into its heap,
transform it and copy it back to the socket connection.

The following example transforms 100K rows with and without `arrow` enabled,
`arrow` makes transformation with R functions close to 41x faster.

```r
microbenchmark::microbenchmark(
  setup = library(arrow),
  arrow_on = {
    sample_n(sparklyr_df, 10^5) %>% spark_apply(~ .x / 2) %>% count()
  },
  arrow_off = {
    if ("arrow" %in% .packages()) detach("package:arrow")
    sample_n(sparklyr_df, 10^5) %>% spark_apply(~ .x / 2) %>% count()
  },
  times = 10
) %T>% print() %>% ggplot2::autoplot()
```
```
Unit: seconds
      expr        min         lq       mean     median         uq        max neval
  arrow_on   3.881293   4.038376   5.136604   4.772739   5.759082   7.873711    10
 arrow_off 178.605733 183.654887 213.296238 227.182018 233.601885 238.877341    10
 ```

<div align="center">
<img src="{{ site.base-url }}/img/arrow-r-spark-transforming.png"
     alt="Transforming data with R in Spark with and without Arrow"
     width="60%" class="img-responsive">
</div>

Additional benchmarks and fine-tuning parameters can be found under `sparklyr`
[/rstudio/sparklyr/pull/1611][10] and `SparkR` [/apache/spark/pull/22954][11]. Looking forward to bringing this feature
to the Spark, Arrow and R communities.

[1]: https://github.com/javierluraschi
[2]: https://rstudio.com
[3]: https://github.com/rstudio/sparklyr
[4]: https://spark.apache.org/docs/latest/sparkr.html
[5]: https://spark.apache.org
[6]: https://dplyr.tidyverse.org
[7]: https://cran.r-project.org/package=DBI
[8]: https://spark.rstudio.com/reference/#section-spark-dataframes
[9]: https://CRAN.R-project.org/package=microbenchmark
[10]: https://github.com/rstudio/sparklyr/pull/1611
[11]: https://github.com/apache/spark/pull/22954
