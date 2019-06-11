
<!-- README.md is generated from README.Rmd. Please edit that file -->

# arrow

[![conda-forge](https://img.shields.io/conda/vn/conda-forge/r-arrow.svg)](https://anaconda.org/conda-forge/r-arrow)

[Apache Arrow](https://arrow.apache.org/) is a cross-language
development platform for in-memory data. It specifies a standardized
language-independent columnar memory format for flat and hierarchical
data, organized for efficient analytic operations on modern hardware. It
also provides computational libraries and zero-copy streaming messaging
and interprocess communication.

The `arrow` package exposes an interface to the Arrow C++ library.

## Installation

Installing `arrow` is a two-step process: first install the Arrow C++
library, then the R package.

### Release version

The current release of Apache Arrow is 0.13.

On macOS, you may install the C++ library using
[Homebrew](https://brew.sh/):

``` shell
brew install apache-arrow
```

On Linux, see the [Arrow project installation
page](https://arrow.apache.org/install/).

Then, you can install the release version of the package from GitHub
using the [`remotes`](https://remotes.r-lib.org/) package. From within
an R session,

``` r
# install.packages("remotes") # Or install "devtools", which includes remotes
remotes::install_github("apache/arrow/r", ref="76e1bc5dfb9d08e31eddd5cbcc0b1bab934da2c7")
```

or if you prefer to stay at the command line,

``` shell
R -e 'remotes::install_github("apache/arrow/r", ref="76e1bc5dfb9d08e31eddd5cbcc0b1bab934da2c7")'
```

### Development version

First, clone the repository and install a release build of the C++
library.

``` shell
git clone https://github.com/apache/arrow.git
mkdir arrow/cpp/build && cd arrow/cpp/build
cmake .. -DARROW_PARQUET=ON -DARROW_BOOST_USE_SHARED:BOOL=Off -DARROW_INSTALL_NAME_RPATH=OFF
make install
```

Then, you can install the R package and its dependencies from the git
checkout:

``` shell
cd ../../r
R -e 'install.packages("remotes"); remotes::install_deps()'
R CMD INSTALL .
```

If the package fails to install/load with an error like this:

    ** testing if installed package can be loaded from temporary location
    Error: package or namespace load failed for 'arrow' in dyn.load(file, DLLpath = DLLpath, ...):
    unable to load shared object '/Users/you/R/00LOCK-r/00new/arrow/libs/arrow.so':
    dlopen(/Users/you/R/00LOCK-r/00new/arrow/libs/arrow.so, 6): Library not loaded: @rpath/libarrow.14.dylib

try setting the environment variable `LD_LIBRARY_PATH` (or
`DYLD_LIBRARY_PATH` on macOS) to wherever Arrow C++ was put in `make
install`, e.g. `export LD_LIBRARY_PATH=/usr/local/lib`, and retry
installing the R package.

For any other build/configuration challenges, see the [C++ developer
guide](https://arrow.apache.org/docs/developers/cpp.html#building).

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

tab <- arrow::table(x = 1:10, y = rnorm(10))
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
#>  1     1  0.524
#>  2     2 -0.606
#>  3     3 -0.655
#>  4     4  1.37 
#>  5     5  1.53 
#>  6     6  1.96 
#>  7     7  1.80 
#>  8     8  1.27 
#>  9     9  0.698
#> 10    10 -0.661
```

## Developing

It is recommended to use the `devtools` package to help with some
package-related manipulations. You’ll need a few more R packages, which
you can install using

``` r
install.packages("devtools")
devtools::install_dev_deps()
```

If you change C++ code, you need to set the `ARROW_R_DEV` environment
variable to `TRUE`, e.g.  in your `~/.Renviron` so that the
`data-raw/codegen.R` file is used for code generation.

### Useful functions

``` r
devtools::load_all() # Load the dev package
devtools::test(filter="^regexp$") # Run the test suite, optionally filtering file names
devtools::document() # Update roxygen documentation
rmarkdown::render("README.Rmd") # To rebuild README.md
pkgdown::build_site() # To preview the documentation website
devtools::check() # All package checks; see also below
```

### Full package validation

``` shell
R CMD build --keep-empty-dirs .
R CMD check arrow_*.tar.gz --as-cran --no-manual
```
