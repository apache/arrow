# arrow

[![cran](https://www.r-pkg.org/badges/version-last-release/arrow)](https://cran.r-project.org/package=arrow)
[![CI](https://github.com/apache/arrow/workflows/R/badge.svg?event=push)](https://github.com/apache/arrow/actions?query=workflow%3AR+branch%3Amaster+event%3Apush)
[![conda-forge](https://img.shields.io/conda/vn/conda-forge/r-arrow.svg)](https://anaconda.org/conda-forge/r-arrow)

[Apache Arrow](https://arrow.apache.org/) is a cross-language
development platform for in-memory data. It specifies a standardized
language-independent columnar memory format for flat and hierarchical
data, organized for efficient analytic operations on modern hardware. It
also provides computational libraries and zero-copy streaming messaging
and interprocess communication.

The `arrow` package exposes an interface to the Arrow C++ library to
access many of its features in R. This includes support for analyzing
large, multi-file datasets (`open_dataset()`), working with individual
Parquet (`read_parquet()`, `write_parquet()`) and Feather
(`read_feather()`, `write_feather()`) files, as well as lower-level
access to Arrow memory and messages.

## Installation

Install the latest release of `arrow` from CRAN with

```r
install.packages("arrow")
```

Conda users can install `arrow` from conda-forge with

```
conda install -c conda-forge --strict-channel-priority r-arrow
```

Installing a released version of the `arrow` package requires no
additional system dependencies. For macOS and Windows, CRAN hosts binary
packages that contain the Arrow C++ library. On Linux, source package
installation will also build necessary C++ dependencies. For a faster,
more complete installation, set the environment variable `NOT_CRAN=true`.
See `vignette("install", package = "arrow")` for details.

If you install the `arrow` package from source and the C++ library is
not found, the R package functions will notify you that Arrow is not
available. Call

```r
arrow::install_arrow()
```

to retry installation with dependencies.

Note that `install_arrow()` is available as a standalone script, so you can
access it for convenience without first installing the package:

```r
source("https://raw.githubusercontent.com/apache/arrow/master/r/R/install-arrow.R")
install_arrow()
```

## Installing a development version

Development versions of the package (binary and source) are built daily and hosted at
<https://dl.bintray.com/ursalabs/arrow-r/>. To install from there:

``` r
install.packages("arrow", repos = "https://dl.bintray.com/ursalabs/arrow-r")
```

Or

```r
install_arrow(nightly = TRUE)
```

These daily package builds are not official Apache releases and are not
recommended for production use. They may be useful for testing bug fixes
and new features under active development.

## Developing

Windows and macOS users who wish to contribute to the R package and
don’t need to alter the Arrow C++ library may be able to obtain a
recent version of the library without building from source. On macOS,
you may install the C++ library using [Homebrew](https://brew.sh/):

``` shell
# For the released version:
brew install apache-arrow
# Or for a development version, you can try:
brew install apache-arrow --HEAD
```

On Windows, you can download a .zip file with the arrow dependencies from the
[nightly bintray repository](https://dl.bintray.com/ursalabs/arrow-r/libarrow/bin/windows-35/),
and then set the `RWINLIB_LOCAL` environment variable to point to that
zip file before installing the `arrow` R package. Version numbers in that
repository correspond to dates, and you will likely want the most recent.

If you need to alter both the Arrow C++ library and the R package code,
or if you can’t get a binary version of the latest C++ library
elsewhere, you’ll need to build it from source too.

First, install the C++ library. See the [developer
guide](https://arrow.apache.org/docs/developers/cpp/building.html) for details.

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
R -e 'install.packages(c("devtools", "roxygen2", "pkgdown", "covr")); devtools::install_dev_deps()'
R CMD INSTALL .
```

If you need to set any compilation flags while building the Rcpp
extensions, you can use the `ARROW_R_CXXFLAGS` environment variable. For
example, if you are using `perf` to profile the R extensions, you may
need to set

``` shell
export ARROW_R_CXXFLAGS=-fno-omit-frame-pointer
```

If the package fails to install/load with an error like this:

    ** testing if installed package can be loaded from temporary location
    Error: package or namespace load failed for 'arrow' in dyn.load(file, DLLpath = DLLpath, ...):
    unable to load shared object '/Users/you/R/00LOCK-r/00new/arrow/libs/arrow.so':
    dlopen(/Users/you/R/00LOCK-r/00new/arrow/libs/arrow.so, 6): Library not loaded: @rpath/libarrow.14.dylib

try setting the environment variable `R_LD_LIBRARY_PATH` to wherever
Arrow C++ was put in `make install`, e.g. `export
R_LD_LIBRARY_PATH=/usr/local/lib`, and retry installing the R package.

When installing from source, if the R and C++ library versions do not
match, installation may fail. If you’ve previously installed the
libraries and want to upgrade the R package, you’ll need to update the
Arrow C++ library first.

For any other build/configuration challenges, see the [C++ developer
guide](https://arrow.apache.org/docs/developers/cpp/building.html) and
`vignette("install", package = "arrow")`.

### Editing Rcpp code

The `arrow` package uses some customized tools on top of `Rcpp` to
prepare its C++ code in `src/`. If you change C++ code in the R package,
you will need to set the `ARROW_R_DEV` environment variable to `TRUE`
(optionally, add it to your`~/.Renviron` file to persist across
sessions) so that the `data-raw/codegen.R` file is used for code
generation.

We use Google C++ style in our C++ code. Check for style errors with

    ./lint.sh

Fix any style issues before committing with

    ./lint.sh --fix

The lint script requires Python 3 and `clang-format-8`. If the command
isn’t found, you can explicitly provide the path to it like
`CLANG_FORMAT=$(which clang-format-8) ./lint.sh`. On macOS, you can get
this by installing LLVM via Homebrew and running the script as
`CLANG_FORMAT=$(brew --prefix llvm@8)/bin/clang-format ./lint.sh`

### Useful functions

Within an R session, these can help with package development:

``` r
devtools::load_all() # Load the dev package
devtools::test(filter="^regexp$") # Run the test suite, optionally filtering file names
devtools::document() # Update roxygen documentation
pkgdown::build_site() # To preview the documentation website
devtools::check() # All package checks; see also below
covr::package_coverage() # See test coverage statistics
```

Any of those can be run from the command line by wrapping them in `R -e
'$COMMAND'`. There’s also a `Makefile` to help with some common tasks
from the command line (`make test`, `make doc`, `make clean`, etc.)

### Full package validation

``` shell
R CMD build .
R CMD check arrow_*.tar.gz --as-cran
```
