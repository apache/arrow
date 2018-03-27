
<!-- README.md is generated from README.Rmd. Please edit that file -->

# rrrow

rrrow is an R front end to Apache Arrow.

## Installation

I’ve only tested this locally for now, here is how I install it,
borrowed from [rarrow](https://github.com/jimhester/rarrow).

First you need the Arrow C++ library installed

    git clone https://github.com/apache/arrow.git
    cd arrow/cpp && mkdir release && cd release
    
    # It is important to statically link to boost libraries
    cmake .. -DCMAKE_BUILD_TYPE=Release -DARROW_BOOST_USE_SHARED:BOOL=Off
    make install

Then you can install the development version of the package:

``` r
# devtools::install_github("apache/arrow/r")
devtools::install_github("romainfrancois/arrow/r")
```

## Example

factory functions:

``` r
library(rrrow)
#> 
#> Attaching package: 'rrrow'
#> The following object is masked from 'package:utils':
#> 
#>     timestamp

# metadata factories
int32()
#> DataType(int32)
struct( x = int32(), y = int64() )
#> StructType(struct<x: int32, y: int64>)
```

ArrayBuilder

``` r
# make a builder for an array of type int32. 
(b <- ArrayBuilder( int32() ) )
#> <pointer: 0x7fe5d26b8e10>
#> attr(,"class")
#> [1] "arrow::NumericBuilder<arrow::Int32Type>"
#> [2] "arrow::ArrayBuilder"
ArrayBuilder__num_children(b)
#> [1] 0
```

This is just kicking the tires with the C++ class system for now.
Eventully, we probably will have something like `b$num_children()` or
perhaps the `ArrayBuilder` class won’t even be public.

At the moment the internal code is hand written, we are considering
moving to [RcppR6](https://github.com/richfitz/RcppR6) as a development
time dependency to generate the bindings.
