
<!-- README.md is generated from README.Rmd. Please edit that file -->

# arrow

[![cran](https://www.r-pkg.org/badges/version-last-release/arrow)](https://cran.r-project.org/package=arrow)
[![conda-forge](https://img.shields.io/conda/vn/conda-forge/r-arrow.svg)](https://anaconda.org/conda-forge/r-arrow)

[Apache Arrow](https://arrow.apache.org/) is a cross-language
development platform for in-memory data. It specifies a standardized
language-independent columnar memory format for flat and hierarchical
data, organized for efficient analytic operations on modern hardware. It
also provides computational libraries and zero-copy streaming messaging
and interprocess communication.

The `arrow` package exposes an interface to the Arrow C++ library to
access many of its features in R. This includes support for working with
Parquet (`read_parquet()`, `write_parquet()`) and Feather
(`read_feather()`, `write_feather()`) files, as well as lower-level
access to Arrow memory and messages.

## Installation

Install the latest release of `arrow` from CRAN with

``` r
install.packages("arrow")
```

On macOS and Windows, installing a binary package from CRAN will handle
Arrow’s C++ dependencies for you. On Linux, you’ll need to first install
the C++ library. See the [Arrow project installation
page](https://arrow.apache.org/install/) to find pre-compiled binary
packages for some common Linux distributions, including Debian, Ubuntu,
and CentOS. You’ll need to install `libparquet-dev` on Debian and
Ubuntu, or `parquet-devel` on CentOS. This will also automatically
install the Arrow C++ library as a dependency.

If you install the `arrow` package from source and the C++ library is
not found, the R package functions will notify you that Arrow is not
available. Call

``` r
arrow::install_arrow()
```

for version- and platform-specific guidance on installing the Arrow C++
library.

When installing from source, if the R and C++ library versions do not
match, installation may fail. If you’ve previously installed the
libraries and want to upgrade the R package, you’ll need to update the
Arrow C++ library first.

## Example

``` r
library(arrow)
set.seed(24)

tab <- arrow::table(x = 1:10, y = rnorm(10))
tab$schema
#> arrow::Schema
#> x: int32
#> y: double
tab
#> arrow::Table
as.data.frame(tab)
#>     x            y
#> 1   1 -0.545880758
#> 2   2  0.536585304
#> 3   3  0.419623149
#> 4   4 -0.583627199
#> 5   5  0.847460017
#> 6   6  0.266021979
#> 7   7  0.444585270
#> 8   8 -0.466495124
#> 9   9 -0.848370044
#> 10 10  0.002311942
```

## Installing a development version

To use the development version of the R package, you’ll need to install
it from source, which requires the additional C++ library setup. On
macOS, you may install the C++ library using
[Homebrew](https://brew.sh/):

``` shell
# For the released version:
brew install apache-arrow
# Or for a development version, you can try:
brew install apache-arrow --HEAD
```

On Windows, you can download a .zip file with the arrow dependencies
from the [rwinlib](https://github.com/rwinlib/arrow/releases) project,
and then set the `RWINLIB_LOCAL` environment variable to point to that
zip file before installing the `arrow` R package. That project contains
released versions of the C++ library; for a development version, Windows
users may be able to find a binary by going to the [Apache Arrow
project’s
Appveyor](https://ci.appveyor.com/project/ApacheSoftwareFoundation/arrow),
selecting an R job from a recent build, and downloading the
`build\arrow-*.zip` file from the “Artifacts” tab.

Linux users can get a released version of the library from our PPAs, as
described above. If you need a development version of the C++ library,
you will likely need to build it from source. See “Development” below.

Once you have the C++ library, you can install the R package from GitHub
using the [`remotes`](https://remotes.r-lib.org/) package. From within
an R session,

``` r
# install.packages("remotes") # Or install "devtools", which includes remotes
remotes::install_github("apache/arrow/r")
```

or if you prefer to stay at the command line,

``` shell
R -e 'remotes::install_github("apache/arrow/r")'
```

You can specify a particular commit, branch, or
[release](https://github.com/apache/arrow/releases) to install by
including a `ref` argument to `install_github()`. This is particularly
useful to match the R package version to the C++ library version you’ve
installed.

## Developing

If you need to alter both the Arrow C++ library and the R package code,
or if you can’t get a binary version of the latest C++ library
elsewhere, you’ll need to build it from source too.

First, install the C++ library. See the [C++ developer
guide](https://arrow.apache.org/docs/developers/cpp.html) for details.

Note that after any change to the C++ library, you must reinstall it and
run `make clean` or `git clean -fdx .` to remove any cached object code
in the `r/src/` directory before reinstalling the R package. This is
only necessary if you make changes to the C++ library source; you do not
need to manually purge object files if you are only editing R or Rcpp
code inside `r/`.

Once you’ve built the C++ library, you can install the R package and its
dependencies, along with additional dev dependencies, from the git
checkout:

``` shell
cd ../../r
R -e 'install.packages("devtools"); devtools::install_dev_deps()'
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

### Editing Rcpp code

The `arrow` package uses some customized tools on top of `Rcpp` to
prepare its C++ code in `src/`. If you change C++ code in the R package,
you will need to set the `ARROW_R_DEV` environment variable to `TRUE`
(optionally, add it to your`~/.Renviron` file to persist across
sessions) so that the `data-raw/codegen.R` file is used for code
generation.

The codegen.R script has these dependencies:

``` r
remotes::install_github("romainfrancois/decor")
install.packages(c("dplyr", "purrr", "glue"))
```

### Useful functions

Within an R session, these can help with package development:

``` r
devtools::load_all() # Load the dev package
devtools::test(filter="^regexp$") # Run the test suite, optionally filtering file names
devtools::document() # Update roxygen documentation
rmarkdown::render("README.Rmd") # To rebuild README.md
pkgdown::build_site(run_dont_run=TRUE) # To preview the documentation website
devtools::check() # All package checks; see also below
```

Any of those can be run from the command line by wrapping them in `R -e
'$COMMAND'`. There’s also a `Makefile` to help with some common tasks
from the command line (`make test`, `make doc`, `make clean`, etc.)

### Full package validation

``` shell
R CMD build --keep-empty-dirs .
R CMD check arrow_*.tar.gz --as-cran --no-manual
```
