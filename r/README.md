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

The `arrow` package exposes an interface to the Arrow C++ library to
access many of its features in R. This includes support for analyzing
large, multi-file datasets (`open_dataset()`), working with individual
Parquet (`read_parquet()`, `write_parquet()`) and Feather
(`read_feather()`, `write_feather()`) files, as well as a `dplyr`
backend and lower-level access to Arrow memory and messages.

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

If you have `arrow` installed and want to switch to the latest nightly development version, you can use the included `install_arrow()` utility function:

``` r
arrow::install_arrow(nightly = TRUE)
```

Conda users can install `arrow` nightly builds with

``` shell
conda install -c arrow-nightlies -c conda-forge --strict-channel-priority r-arrow
```

These nightly package builds are not official Apache releases and are
not recommended for production use. They may be useful for testing bug
fixes and new features under active development.

## Apache Arrow metadata and data objects

Arrow defines the following classes for representing metadata:

| Class      | Description                                      | How to create an instance        |
|------------|--------------------------------------------------|----------------------------------|
| `DataType` | attribute controlling how values are represented | functions in `help("data-type")` |
| `Field`    | a character string name and a `DataType`         | `field(name, type)`              |
| `Schema`   | list of `Field`s                                 | `schema(...)`                    |

Arrow defines the following classes for representing zero-dimensional
(scalar), one-dimensional (array/vector-like), and two-dimensional
(tabular/data frame-like) data:

| Dim | Class          | Description                               | How to create an instance                          |
|-----|----------------|-------------------------------------------|----------------------------------------------------|
| 0   | `Scalar`       | single value and its `DataType`           | `Scalar$create(value, type)`                       |
| 1   | `Array`        | vector of values and its `DataType`       | `Array$create(vector, type)`                       |
| 1   | `ChunkedArray` | list of `Array`s with the same `DataType` | `chunked_array(..., type)`                         |
| 2   | `RecordBatch`  | list of `Array`s with a `Schema`          | `record_batch(...)`                                |
| 2   | `Table`        | list of `ChunkedArray` with a `Schema`    | `Table$create(...)` or `arrow::read_*()` functions |
| 2   | `Dataset`      | list of `Table`s with the same `Schema`   | see `vignette("dataset", package = "arrow")`       |

These classes exist in the `arrow` R package and correspond to classes
of the same names in the Arrow C++ library. For convenience, the `arrow`
package also defines several synthetic classes that do not exist in the
C++ library, including:

-   `ArrowDatum`: inherited by `Scalar`, `Array`, and `ChunkedArray`
-   `ArrowTabular`: inherited by `RecordBatch` and `Table`
-   `ArrowObject`: inherited by all Arrow objects

These are all defined as `R6` classes. The `arrow` package provides a
variety of `R6` and S3 methods for interacting with instances of these
classes.

## Reading and writing data files with Arrow

The `arrow` package provides functions for reading data from several
common file formats. By default, calling any of these functions returns
an R data frame. To return an Arrow `Table`, set argument
`as_data_frame = FALSE`.

-   `read_delim_arrow()`: read a delimited text file (default delimiter
    is comma)
-   `read_csv_arrow()`: read a comma-separated values (CSV) file
-   `read_tsv_arrow()`: read a tab-separated values (TSV) file
-   `read_json_arrow()`: read a JSON data file
-   `read_feather()`: read a file in Feather format (the Apache Arrow
    IPC format)
-   `read_parquet()`: read a file in Parquet format (an efficient
    columnar data format)

For writing Arrow tabular data structures to files, the `arrow` package
provides the functions `write_feather()` and `write_parquet()`. These
functions also accept R data frames.

## Using dplyr with Arrow

The `arrow` package provides a `dplyr` backend, enabling manipulation of
Arrow tabular data with `dplyr` verbs. To begin, load both `arrow` and
`dplyr`:

``` r
library(arrow, warn.conflicts = FALSE)
library(dplyr, warn.conflicts = FALSE)
```

Then create an Arrow `Table` or `RecordBatch` using one of the object
creation or file loading functions listed above. For example, create a
`Table` named `sw` with the Star Wars characters data frame that’s
included in `dplyr`:

``` r
sw <- Table$create(starwars)
```

Or read the same data from a Parquet file, using `as_data_frame = FALSE`
to create a `Table` named `sw`:

``` r
write_parquet(starwars, data_file <- tempfile()) # write file to demonstrate reading it
sw <- read_parquet(data_file, as_data_frame = FALSE)
```

For larger or multi-file datasets, load the data into a `Dataset` as
described in `vignette("dataset", package = "arrow")`.

Next, pipe on `dplyr` verbs:

``` r
result <- sw %>% 
  filter(homeworld == "Tatooine") %>% 
  rename(height_cm = height, mass_kg = mass) %>%
  mutate(height_in = height_cm / 2.54, mass_lbs = mass_kg * 2.2046) %>%
  arrange(desc(birth_year)) %>%
  select(name, height_in, mass_lbs)
```

The `arrow` package uses lazy evaluation. `result` is an object with
class `arrow_dplyr_query` which represents the computations to be
performed.

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

To execute these computations and obtain the result, call `compute()` or
`collect()`. `compute()` returns an Arrow `Table`, suitable for passing
to other `arrow` or `dplyr` functions.

``` r
result %>% compute()
#> Table
#> 10 rows x 3 columns
#> $name <string>
#> $height_in <double>
#> $mass_lbs <double>
```

`collect()` returns an R data frame or tibble, suitable for viewing or
passing to other R functions for analysis or visualization:

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

Arrow supports most `dplyr` verbs except those that compute aggregates
(such as `summarise()`, and `mutate()` after `group_by()`). Inside
`dplyr` verbs, Arrow offers limited support for functions and operators,
with broader support expected in upcoming releases. For more information
about available compute functions, see `help("list_compute_functions")`.

For `dplyr` queries on `Table` and `RecordBatch` objects, if the `arrow`
package detects an unsupported function within a `dplyr` verb, it
automatically calls `collect()` to return the data as an R data frame
before processing that `dplyr` verb.

## Getting help

If you encounter a bug, please file an issue with a minimal reproducible
example on [the Apache Jira issue
tracker](https://issues.apache.org/jira/). Choose the project **Apache
Arrow (ARROW)**, select the component **R**, and begin the issue summary
with **\[R\]** followed by a space. For more information, see the
**Report bugs and propose features** section of the [Contributing to
Apache
Arrow](https://arrow.apache.org/docs/developers/contributing.html) page
in the Arrow developer documentation.

We welcome questions and discussion about the `arrow` package. For
information about mailing lists and other venues for engaging with the
Arrow developer and user communities, please see the [Apache Arrow
Community](https://arrow.apache.org/community/) page.

------------------------------------------------------------------------

All participation in the Apache Arrow project is governed by the Apache
Software Foundation’s [code of
conduct](https://www.apache.org/foundation/policies/conduct.html).
