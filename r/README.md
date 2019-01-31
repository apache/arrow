
<!-- README.md is generated from README.Rmd. Please edit that file -->

# arrow

R integration with Apache Arrow.

## Installation

First install a release build of the C++ bindings to arrow.

``` shell
git clone https://github.com/apache/arrow.git
cd arrow/cpp && mkdir release && cd release

# It is important to statically link to boost libraries
cmake .. -DARROW_PARQUET=ON -DCMAKE_BUILD_TYPE=Release -DARROW_BOOST_USE_SHARED:BOOL=Off
make install
```

Then the R package:

``` r
devtools::install_github("apache/arrow/r")
```

If libarrow was built with the old CXXABI then you need to pass
the ARROW_USE_OLD_CXXABI configuration variable.

``` r
devtools::install_github("apache/arrow/r", args=c("--configure-vars=ARROW_USE_OLD_CXXABI=1"))
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
#>  1     1  0.0855
#>  2     2 -1.68  
#>  3     3 -0.0294
#>  4     4 -0.124 
#>  5     5  0.0675
#>  6     6  1.64  
#>  7     7  1.54  
#>  8     8 -0.0209
#>  9     9 -0.982 
#> 10    10  0.349
# arrow::write_arrow(tib, tf)

# # read it back with pyarrow
# pa <- import("pyarrow")
# as_tibble(pa$open_file(tf)$read_pandas())
```
