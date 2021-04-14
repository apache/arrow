# arrow

[![cran](https://www.r-pkg.org/badges/version-last-release/arrow)](https://cran.r-project.org/package=arrow)
[![CI](https://github.com/apache/arrow/workflows/R/badge.svg?event=push)](https://github.com/apache/arrow/actions?query=workflow%3AR+branch%3Amaster+event%3Apush)
[![conda-forge](https://img.shields.io/conda/vn/conda-forge/r-arrow.svg)](https://anaconda.org/conda-forge/r-arrow)

[Apache Arrow](https://arrow.apache.org/) is a cross-language
development platform for in-memory data. It specifies a standardized
language-independent columnar memory format for flat and hierarchical
data, organized for efficient analytic operations on modern hardware. It
also provides computational libraries and zero-copy streaming messaging
and interprocess communication.

The `arrow` package exposes an interface to the Arrow C++ library,
enabling access to many of its features in R. It provides low-level
access to the Arrow C++ library API as well as higher-level access
through a `dplyr` backend and many R-flavored functions.

## What can `arrow` do?

-   Read and write Parquet files (`read_parquet()`, `write_parquet()`)
-   Read and write Feather files (`read_feather()`, `write_feather()`)
-   Read or write large, multi-file datasets with a single function call
    (`open_dataset()`, `write_dataset()`)
-   Read very large CSV and JSON files with excellent speed and
    efficiency (`read_csv_arrow()`, `read_json_arrow()`)
-   Read and write files in Amazon S3 buckets
-   Exercise full control over the data types of columns when reading
    and writing data files
-   Use compression codecs including Snappy, gzip, Brotli, Zstandard,
    LZ4, LZO, and bzip2 when reading and writing data
-   Manipulate and analyze larger-than-memory datasets with `dplyr`
    verbs
-   Pass data between R and Python in the same process
-   Connect to Arrow Flight RPC servers to send and receive large
    datasets over networks
-   Access and manipulate Arrow objects through low-level bindings to
    the C++ library
-   Provide a toolkit for building connectors to other applications and
    services that use Arrow

## Installation

Install the latest release of `arrow` from CRAN with

``` r
install.packages("arrow")
```

Conda users can install `arrow` from conda-forge with

``` shell
conda install -c conda-forge --strict-channel-priority r-arrow
```

Installing a released version of the `arrow` package requires no
additional system dependencies. For macOS and Windows, CRAN hosts binary
packages that contain the Arrow C++ library. On Linux, source package
installation will also build necessary C++ dependencies. For a faster,
more complete installation, set the environment variable
`NOT_CRAN=true`. See `vignette("install", package = "arrow")` for
details.

## Installing a development version

Development versions of the package (binary and source) are built
nightly and hosted at <https://arrow-r-nightly.s3.amazonaws.com>. To
install from there:

``` r
install.packages("arrow", repos = "https://arrow-r-nightly.s3.amazonaws.com")
```

Conda users can install `arrow` nightly builds with

``` shell
conda install -c arrow-nightlies -c conda-forge --strict-channel-priority r-arrow
```

If you already have a version of `arrow` installed, you can switch to
the latest nightly development version with

``` r
arrow::install_arrow(nightly = TRUE)
```

These nightly package builds are not official Apache releases and are
not recommended for production use. They may be useful for testing bug
fixes and new features under active development.

## Usage

Among the many uses of the `arrow` package, two of the most accessible
uses are:

-   Reading and writing data files
-   Manipulating Arrow data with `dplyr` verbs

The sections below describe these two uses and illustrate them with
basic examples. The sections below mention two Arrow data structures:

-   `Table`: a tabular, column-oriented data structure capable of
    storing and processing large amounts of data more efficiently than
    R’s built-in `data.frame` and with SQL-like column data types that
    afford better interoperability with databases and data warehouse
    systems
-   `Dataset`: a data structure functionally similar to `Table` but with
    the capability to read larger-than-memory data partitioned across
    multiple files

### Reading and writing data files with Arrow

The `arrow` package provides functions for reading single data files in
several common formats. By default, calling any of these functions
returns an R `data.frame`. To return an Arrow `Table`, set argument
`as_data_frame = FALSE`.

-   `read_parquet()`: read a file in Parquet format (an efficient
    columnar data format)
-   `read_feather()`: read a file in Feather format (the Apache Arrow
    IPC format)
-   `read_delim_arrow()`: read a delimited text file (default delimiter
    is comma)
-   `read_csv_arrow()`: read a comma-separated values (CSV) file
-   `read_tsv_arrow()`: read a tab-separated values (TSV) file
-   `read_json_arrow()`: read a JSON data file

For writing data to single files, the `arrow` package provides the
functions `write_parquet()` and `write_feather()`. These can be used
with R `data.frame` and Arrow `Table` objects.

For example, let’s write the Star Wars characters data that’s included
in `dplyr` to a Parquet file, then read it back in. First load the
`arrow` and `dplyr` packages:

``` r
library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
```

Then write the `data.frame` named `starwars` to a Parquet file at
`file_path`:

``` r
file_path <- tempfile()
write_parquet(starwars, file_path)
```

Then read the Parquet file into an R `data.frame` named `sw`:

``` r
sw <- read_parquet(file_path)
```

For reading and writing larger files or multiple files with Arrow
`Dataset` objects, `arrow` provides the functions `open_dataset()` and
`write_dataset()`. For examples of these, see
`vignette("dataset", package = "arrow")`.

All these functions can read and write files in the local filesystem (by
passing unqualified paths or `file://` URIs) or files in Amazon S3 (by
passing S3 URIs beginning with `s3://`). For more details, see
`vignette("fs", package = "arrow")`

### Using dplyr with Arrow

The `arrow` package provides a `dplyr` backend enabling manipulation of
Arrow tabular data with `dplyr` verbs. To use it, first load both
packages `arrow` and `dplyr`. Then load data into an Arrow `Table` or
`Dataset` object. For example, read the Parquet file written in the
previous example into an Arrow `Table` named `sw`:

``` r
sw <- read_parquet(file_path, as_data_frame = FALSE)
```

Next, pipe on `dplyr` verbs:

``` r
result <- sw %>% 
  filter(homeworld == "Tatooine") %>% 
  rename(height_cm = height, mass_kg = mass) %>%
  mutate(height_in = height_cm / 2.54, mass_lbs = mass_kg * 2.2046) %>%
  arrange(desc(birth_year)) %>%
  select(name, height_in, mass_lbs)
```

The `arrow` package uses lazy evaluation to delay computation until the
result is required. `result` is an object with class `arrow_dplyr_query`
which represents the computations to be performed:

``` r
result
#> Table (query)
#> name: string
#> height_in: expr
#> mass_lbs: expr
#> 
#> * Filter: equal(homeworld, "Tatooine")
#> * Sorted by birth_year [desc]
#> See $.data for the source Arrow object
```

To execute these computations and materialize the result, call
`compute()` or `collect()`. `compute()` returns an Arrow `Table`,
suitable for passing to other `arrow` or `dplyr` functions:

``` r
result %>% compute()
#> Table
#> 10 rows x 3 columns
#> $name <string>
#> $height_in <double>
#> $mass_lbs <double>
```

`collect()` returns an R `data.frame`, suitable for viewing or passing
to other R functions for analysis or visualization:

``` r
result %>% collect()
#> # A tibble: 10 x 3
#>    name               height_in mass_lbs
#>    <chr>                  <dbl>    <dbl>
#>  1 C-3PO                   65.7    165. 
#>  2 Cliegg Lars             72.0     NA  
#>  3 Shmi Skywalker          64.2     NA  
#>  4 Owen Lars               70.1    265. 
#>  5 Beru Whitesun lars      65.0    165. 
#>  6 Darth Vader             79.5    300. 
#>  7 Anakin Skywalker        74.0    185. 
#>  8 Biggs Darklighter       72.0    185. 
#>  9 Luke Skywalker          67.7    170. 
#> 10 R5-D4                   38.2     70.5
```

The `arrow` package works with most `dplyr` verbs except those that
compute aggregates (such as `summarise()`, and `mutate()` after
`group_by()`). Inside `dplyr` verbs, Arrow offers limited support for
functions and operators, with broader support expected in upcoming
releases. For more information about available compute functions, see
`help("list_compute_functions")`.

For `dplyr` queries on `Table` objects, if the `arrow` package detects
an unimplemented function within a `dplyr` verb, it automatically calls
`collect()` to return the data as an R `data.frame` before processing
that `dplyr` verb. For queries on `Dataset` objects (which can be larger
than memory), it raises an error if the function is unimplemented.

### Other uses

Other uses of `arrow` are described in the following vignettes:

-   `vignette("python", package = "arrow")`: use `arrow` and
    `reticulate` to pass data between R and Python
-   `vignette("flight", package = "arrow")`: connect to Arrow Flight RPC
    servers to send and receive data
-   `vignette("flight", package = "arrow")`: access and manipulate Arrow
    objects through low-level bindings to the C++ library

## Getting help

If you encounter a bug, please file an issue with a minimal reproducible
example on the [Apache Jira issue
tracker](https://issues.apache.org/jira/projects/ARROW/issues). Create
an account or log in, then click **Create** to file an issue. Select the
project **Apache Arrow (ARROW)**, select the component **R**, and begin
the issue summary with **\[R\]** followed by a space. For more
information, see the **Report bugs and propose features** section of the
[Contributing to Apache
Arrow](https://arrow.apache.org/docs/developers/contributing.html) page
in the Arrow developer documentation.

We welcome questions, discussion, and contributions from users of the
`arrow` package. For information about mailing lists and other venues
for engaging with the Apache Arrow developer and user communities,
please see the [Apache Arrow
Community](https://arrow.apache.org/community/) page.

------------------------------------------------------------------------

All participation in the Apache Arrow project is governed by the Apache
Software Foundation’s [code of
conduct](https://www.apache.org/foundation/policies/conduct.html).
