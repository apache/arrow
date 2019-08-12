---
layout: post
title: "Apache Arrow R Package On CRAN"
date: "2019-08-08 06:00:00 -0600"
author: npr
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

We are very excited to announce that the `arrow` R package is now available on
[CRAN](https://cran.r-project.org/).

[Apache Arrow](https://arrow.apache.org/) is a cross-language development
platform for in-memory data that specifies a standardized columnar memory
format for flat and hierarchical data, organized for efficient analytic
operations on modern hardware. The `arrow` package provides an R interface to
the Arrow C++ library, including support for working with Parquet and Feather
files, as well as lower-level access to Arrow memory and messages.

You can install the package from CRAN with

```r
install.packages("arrow")
```

On macOS and Windows, installing a binary package from CRAN will generally
handle Arrow's C++ dependencies for you. However, the macOS CRAN binaries are
unfortunately incomplete for this version, so to install 0.14.1, you'll first
need to use Homebrew to get the Arrow C++ library (`brew install
apache-arrow`), and then from R you can `install.packages("arrow", type =
"source")`.

Windows binaries are not yet available on CRAN but should be published soon.

On Linux, you'll need to first install the C++ library. See the [Arrow project
installation page](https://arrow.apache.org/install/) to find pre-compiled
binary packages for some common Linux distributions, including Debian, Ubuntu,
and CentOS. You'll need to install `libparquet-dev` on Debian and Ubuntu, or
`parquet-devel` on CentOS. This will also automatically install the Arrow C++
library as a dependency. Other Linux distributions must install the C++ library
from source.

If you install the `arrow` R package from source and the C++ library is not
found, the R package functions will notify you that Arrow is not
available. Call

```r
arrow::install_arrow()
```

for version- and platform-specific guidance on installing the Arrow C++
library.

## Parquet files

This release introduces basic read and write support for the [Apache
Parquet](https://parquet.apache.org/) columnar data file format. Prior to this
release, options for accessing Parquet data in R were limited; the most common
recommendation was to use Apache Spark. The `arrow` package greatly simplifies
this access and lets you go from a Parquet file to a `data.frame` and back
easily, without having to set up a database.

```r
library(arrow)
df <- read_parquet("path/to/file.parquet")
```

This function, along with the other readers in the package, takes an optional
`col_select` argument, inspired by the
[`vroom`](https://vroom.r-lib.org/reference/vroom.html) package. This argument
lets you use the ["tidyselect" helper
functions](https://tidyselect.r-lib.org/reference/select_helpers.html), as you
can do in `dplyr::select()`, to specify that you only want to keep certain
columns. By narrowing your selection at read time, you can load a `data.frame`
with less memory overhead.

For example, suppose you had written the `iris` dataset to Parquet. You could
read a `data.frame` with only the columns `c("Sepal.Length", "Sepal.Width")` by
doing

```r
df <- read_parquet("iris.parquet", col_select = starts_with("Sepal"))
```

Just as you can read, you can write Parquet files:

```r
write_parquet(df, "path/to/different_file.parquet")
```

Note that this read and write support for Parquet files in R is in its early
stages of development. The Python Arrow library
([pyarrow](https://arrow.apache.org/docs/python/)) still has much richer
support for Parquet files, including working with multi-file datasets. We
intend to reach feature equivalency between the R and Python packages in the
future.

## Feather files

This release also includes a faster and more robust implementation of the
Feather file format, providing `read_feather()` and
`write_feather()`. [Feather](https://github.com/wesm/feather) was one of the
initial applications of Apache Arrow for Python and R, providing an efficient,
common file format language-agnostic data frame storage, along with
implementations in R and Python.

As Arrow progressed, development of Feather moved to the
[`apache/arrow`](https://github.com/apache/arrow) project, and for the last two
years, the Python implementation of Feather has just been a wrapper around
`pyarrow`. This meant that as Arrow progressed and bugs were fixed, the Python
version of Feather got the improvements but sadly R did not.

With this release, the R implementation of Feather catches up and now depends
on the same underlying C++ library as the Python version does. This should
result in more reliable and consistent behavior across the two languages, as
well as [improved
performance](https://wesmckinney.com/blog/feather-arrow-future/).

We encourage all R users of `feather` to switch to using
`arrow::read_feather()` and `arrow::write_feather()`.

Note that both Feather and Parquet are columnar data formats that allow sharing
data frames across R, Pandas, and other tools. When should you use Feather and
when should you use Parquet? Parquet balances space-efficiency with
deserialization costs, making it an ideal choice for remote storage systems
like HDFS or Amazon S3. Feather is designed for fast local reads, particularly
with solid-state drives, and is not intended for use with remote storage
systems. Feather files can be memory-mapped and accessed as Arrow columnar data
in-memory without any deserialization while Parquet files always must be
decompressed and decoded. See the [Arrow project
FAQ](https://arrow.apache.org/faq/) for more.

## Other capabilities

In addition to these readers and writers, the `arrow` package has wrappers for
other readers in the C++ library; see `?read_csv_arrow` and
`?read_json_arrow`. These readers are being developed to optimize for the
memory layout of the Arrow columnar format and are not intended as a direct
replacement for existing R CSV readers (`base::read.csv`, `readr::read_csv`,
`data.table::fread`) that return an R `data.frame`.

It also provides many lower-level bindings to the C++ library, which enable you
to access and manipulate Arrow objects. You can use these to build connectors
to other applications and services that use Arrow. One example is Spark: the
[`sparklyr`](https://spark.rstudio.com/) package has support for using Arrow to
move data to and from Spark, yielding [significant performance
gains](http://arrow.apache.org/blog/2019/01/25/r-spark-improvements/).

## Acknowledgements

In addition to the work on wiring the R package up to the Arrow Parquet C++
library, a lot of effort went into building and packaging Arrow for R users,
ensuring its ease of installation across platforms. We'd like to thank the
support of Jeroen Ooms, Javier Luraschi, JJ Allaire, Davis Vaughan, the CRAN
team, and many others in the Apache Arrow community for helping us get to this
point.
