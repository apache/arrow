
<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# arrow

arrow is an R front end to Apache Arrow.

## Installation

This is work in progress, and will have to be adjusted for various
platforms.

First you need the Arrow C++ library installed

    git clone https://github.com/apache/arrow.git
    cd arrow/cpp && mkdir release && cd release
    
    # It is important to statically link to boost libraries
    cmake .. -DCMAKE_BUILD_TYPE=Release -DARROW_BOOST_USE_SHARED:BOOL=Off
    make install

Then you can install the development version of the package:

``` r
devtools::install_github("apache/arrow/r")
```

## Example

factory functions:

``` r
library(arrow)
#> 
#> Attaching package: 'arrow'
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
#> <pointer: 0x7fda1a4bd4c0>
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
