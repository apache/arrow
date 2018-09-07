
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

(not yet)
