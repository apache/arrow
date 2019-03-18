
<!-- README.md is generated from README.Rmd. Please edit that file -->
arrow
=====

[![conda-forge](https://img.shields.io/conda/vn/conda-forge/r-arrow.svg)](https://anaconda.org/conda-forge/r-arrow)

R integration with Apache Arrow.

Installation
------------

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

Then the R package
------------------

### From github (defaults to master branch)

``` r
library("devtools")
devtools::install_github("apache/arrow/r")
```

Example
-------

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
#>        x      y
#>    <int>  <dbl>
#>  1     1 -1.59 
#>  2     2  1.31 
#>  3     3 -0.513
#>  4     4  2.64 
#>  5     5  0.389
#>  6     6  0.660
#>  7     7  1.66 
#>  8     8 -0.120
#>  9     9  2.43 
#> 10    10  1.53
tab <- table(tib)
tab$schema
#> arrow::Schema 
#> x: int32
#> y: double
tab
#> arrow::Table
as_tibble(tab)
#> # A tibble: 10 x 2
#>        x      y
#>    <int>  <dbl>
#>  1     1 -1.59 
#>  2     2  1.31 
#>  3     3 -0.513
#>  4     4  2.64 
#>  5     5  0.389
#>  6     6  0.660
#>  7     7  1.66 
#>  8     8 -0.120
#>  9     9  2.43 
#> 10    10  1.53
```

Developing
----------

The arrow package is using devtools for package related manipulations. To install:

``` r
library.packages("devtools")
```

Within a local clone, one can test changes by running the test suite.

### Running the test suite

``` r
library("devtools")
devtools::install_dev_deps()
devtools::test()
```

### Full package validation

``` shell
R CMD build --keep-empty-dirs .
R CMD check arrow_*.tar.gz --as-cran --no-manual
```
