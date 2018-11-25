
<!-- README.md is generated from README.Rmd. Please edit that file -->

# arrow

R integration with Apache Arrow.

## Installation

First install a release build of the C++ bindings to arrow.

``` shell
git clone https://github.com/apache/arrow.git
cd arrow/cpp && mkdir release && cd release

# It is important to statically link to boost libraries
cmake .. -DCMAKE_BUILD_TYPE=Release -DARROW_BOOST_USE_SHARED:BOOL=Off
make install
```

Then the R package:

``` r
devtools::install_github("apache/arrow/r")
```

## Example

``` r
library(arrow, warn.conflicts = FALSE)
library(tibble)
library(reticulate)

tf <- tempfile()

# write arrow::Table to file
(tib <- tibble(x = 1:10, y = rnorm(10)))
#> # A tibble: 10 x 2
#>        x       y
#>    <int>   <dbl>
#>  1     1 -0.255
#>  2     2 -0.162
#>  3     3 -0.614
#>  4     4 -0.322
#>  5     5  0.0693
#>  6     6 -0.920
#>  7     7 -1.08
#>  8     8  0.658
#>  9     9  0.821
#> 10    10  0.539
arrow::write_arrow(tib, tf)

# read it back with pyarrow
pa <- import("pyarrow")
as_tibble(pa$open_file(tf)$read_pandas())
#> # A tibble: 10 x 2
#>        x       y
#>    <int>   <dbl>
#>  1     1 -0.255
#>  2     2 -0.162
#>  3     3 -0.614
#>  4     4 -0.322
#>  5     5  0.0693
#>  6     6 -0.920
#>  7     7 -1.08
#>  8     8  0.658
#>  9     9  0.821
#> 10    10  0.539
```

## Development

### Code style

We use Google C++ style in our C++ code. Check for style errors with

```
./lint.sh
```

You can fix the style issues with

```
./lint.sh --fix
```