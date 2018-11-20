---
layout: post
title: "Speeding up R with Spark using Apache Arrow"
date: "2018-11-29 08:00:00 -0800"
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

Support for Apache Arrow in Apache Spark with R is currently under
active development through [sparklyr][3]. This post explores early, yet
promising, performance improvements achieved when using R with [Apache
Spark][4] and Arrow.

# Setup

Since this work is under active development, install `sparklyr` and
`arrow` from GitHub as follows:

```r
devtools::install_github("apache/arrow", subdir = "r", ref = "dc5df8f")
devtools::install_github("rstudio/sparklyr")
```

In this benchmark, we will use [dplyr][5], but similar improvements can
be  expected from using [DBI][6], or [Spark DataFrames][7] in `sparklyr`. 
The local Spark connection and dataframe with 1M numeric rows was
initialized as follows:

```r
library(sparklyr)
library(dplyr)

sc <- spark_connect(master = "local")
data <- data.frame(y = runif(10^6, 0, 1))
```

# Copying

The following benchmark using [microbenchmark][8], copies 1M rows from
R into Spark using `sparklyr` with and without `arrow`, there is close
to a 10x improvement using `arrow`.


```r
microbenchmark::microbenchmark(
  setup = library(arrow),
  arrow_on = {
    library(arrow)
    sparklyr_df <<- copy_to(sc, data, overwrite = T)
    count(sparklyr_df)
  },
  arrow_off = {
    if ("arrow" %in% .packages()) detach("package:arrow")
    sparklyr_df <<- copy_to(sc, data, overwrite = T)
    count(sparklyr_df)
  },
  times = 10
) %>% ggplot2::autoplot()
```

<div align="center">
<img src="{{ site.base-url }}/img/arrow-r-spark-copying.png"
     alt="Copying data with R into Spark with and without Arrow"
     width="60%" class="img-responsive">
</div>

# Collecting

The following benchmark collects 1M rows from Spark into R and shows that `arrow`
can bring 2x improvements. The collection improvements are not as significant as
copying data since, `sparklyr` already collects data in columnar format.

```r
microbenchmark::microbenchmark(
  setup = library(arrow),
  arrow_on = {
    dplyr::collect(sparklyr_df)
  },
  arrow_off = {
    if ("arrow" %in% .packages()) detach("package:arrow")
    dplyr::collect(sparklyr_df)
  },
  times = 10
) %>% ggplot2::autoplot()
```

<div align="center">
<img src="{{ site.base-url }}/img/arrow-r-spark-collecting.png"
     alt="Collecting data with R from Spark with and without Arrow"
     width="60%" class="img-responsive">
</div>

# Transforming

Custom transformations of data using R functions are about 100X faster using `arrow`.
This improvement was significant since transforming data in R was copying
and collecting data and was not optimized to be collected in columnar format.
Therefore, `arrow` will be strongly encouraged to perform custom R transformations
in Spark. The following example transforms 100K rows with and without `arrow` enabled.

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
) %>% ggplot2::autoplot()
```

<div align="center">
<img src="{{ site.base-url }}/img/arrow-r-spark-transforming.png"
     alt="Transforming data with R in Spark with and without Arrow"
     width="60%" class="img-responsive">
</div>

Additional benchmarks and fine-tuning parameters can be found under `sparklyr`
[/rstudio/sparklyr/pull/1611][9]. Looking forward to bringing this feature
to the Spark, Arrow and R communities.

[1]: https://github.com/javierluraschi
[2]: https://rstudio.com
[3]: https://github.com/rstudio/sparklyr
[4]: https://spark.apache.org
[5]: https://dplyr.tidyverse.org
[6]: https://cran.r-project.org/package=DBI
[7]: https://spark.rstudio.com/reference/#section-spark-dataframes
[8]: https://CRAN.R-project.org/package=microbenchmark
[9]: https://github.com/rstudio/sparklyr/pull/1611
