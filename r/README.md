
<!-- README.md is generated from README.Rmd. Please edit that file -->

# arrow

[![conda-forge](https://img.shields.io/conda/vn/conda-forge/r-arrow.svg)](https://anaconda.org/conda-forge/r-arrow)

R integration with Apache Arrow.

## Installation

You first need to install the C++ library:

### macOS

On macOS, you may install using homebrew:

    brew install apache-arrow

### From source

First install a release build of the C++ bindings to arrow.

``` shell
git clone https://github.com/apache/arrow.git
cd arrow/cpp && mkdir release && cd release

# It is important to statically link to boost libraries
cmake .. -DARROW_PARQUET=ON -DCMAKE_BUILD_TYPE=Release -DARROW_BOOST_USE_SHARED:BOOL=Off
make install
```

## Then the R package

``` r
devtools::install_github("apache/arrow/r")
```

## Example

``` r
library(arrow)
#> 
#> Attaching package: 'arrow'
#> The following object is masked from 'package:utils':
#> 
#>     timestamp
#> The following objects are masked from 'package:base':
#> 
#>     array, table

(tib <- tibble::tibble(x = 1:10, y = rnorm(10)))
#> # A tibble: 10 x 2
#>        x       y
#>    <int>   <dbl>
#>  1     1  0.585 
#>  2     2  0.378 
#>  3     3  0.958 
#>  4     4 -0.321 
#>  5     5  0.702 
#>  6     6  0.188 
#>  7     7 -0.625 
#>  8     8 -2.34  
#>  9     9 -0.0325
#> 10    10 -1.22
tab <- table(tib)
tab$schema
#> arrow::Schema 
#> x: int32
#> y: double
tab
#> arrow::Table
as_tibble(tab)
#> # A tibble: 10 x 2
#>        x       y
#>    <int>   <dbl>
#>  1     1  0.585 
#>  2     2  0.378 
#>  3     3  0.958 
#>  4     4 -0.321 
#>  5     5  0.702 
#>  6     6  0.188 
#>  7     7 -0.625 
#>  8     8 -2.34  
#>  9     9 -0.0325
#> 10    10 -1.22
```
