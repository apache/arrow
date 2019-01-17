
<!-- README.md is generated from README.Rmd. Please edit that file -->

# arrow

R integration with Apache Arrow.

## Installation

First install a release build of the C++ bindings to arrow.

``` shell
git clone https://github.com/apache/arrow.git
cd arrow/cpp && mkdir release && cd release

# It is important to statically link to boost libraries.
# For this, you may need to use a vendored version of the
# Boost libraries (e.g. on Ubuntu).
cmake .. -DARROW_PARQUET=ON -DCMAKE_BUILD_TYPE=Release -DARROW_BOOST_USE_SHARED:BOOL=Off -DARROW_BOOST_VENDORED=ON
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
#>  1     1  0.516 
#>  2     2 -0.324 
#>  3     3  0.110 
#>  4     4  0.511 
#>  5     5  0.423 
#>  6     6 -1.09  
#>  7     7 -0.862 
#>  8     8 -0.249 
#>  9     9 -0.0986
#> 10    10  0.340
# arrow::write_arrow(tib, tf)

# # read it back with pyarrow
# pa <- import("pyarrow")
# as_tibble(pa$open_file(tf)$read_pandas())
```
