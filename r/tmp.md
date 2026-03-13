# Temporary development notes

> TODO: Remove this before we open a PR to upstream arrow library.

## Using codegen.R

1. Install repo dependencies in `arrow/r`: `install.packages("remotes")`, then `remotes::install_deps(dependencies = TRUE)`

2. Rscript `data-raw/codegen.R`

The second step auto-generates stubs in `arrowExports.R` and `arrowExports.cpp` based on which C++ functions have `// [[arrow::export]]` comments above them.

**Note**: at the moment we need to run `export ARROW_R_WITH_AZUREFS=true` before `R CMD INSTALL .` to export the environment variable that "forces" the Azure build flag.

## Using Azurite

`docker run -p 10000:10000 -p 10001:10001 -p 10002:10002 mcr.microsoft.com/azure-storage/azurite` (see README.md in https://github.com/Azure/Azurite)

## Build troubleshooting continued

```bash
export ARROW_HOME=/workspaces/arrow/dist

cmake \
  -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
  -DCMAKE_INSTALL_LIBDIR=lib \
  -DARROW_COMPUTE=ON \
  -DARROW_CSV=ON \
  -DARROW_DATASET=ON \
  -DARROW_EXTRA_ERROR_CONTEXT=ON \
  -DARROW_FILESYSTEM=ON \
  -DARROW_INSTALL_NAME_RPATH=OFF \
  -DARROW_JEMALLOC=ON \
  -DARROW_JSON=ON \
  -DARROW_PARQUET=ON \
  -DARROW_WITH_SNAPPY=ON \
  -DARROW_WITH_ZLIB=ON \
  -DARROW_AZURE=ON \
  ..

cmake --build . --target install -j8


R -e 'install.packages("remotes"); remotes::install_deps(dependencies = TRUE)'

R CMD INSTALL --no-multiarch .

# ----------------------


# Try building from source via R with the relevant env vars set for feature flags.

# Core Build Settings
export LIBARROW_MINIMAL=false
export FORCE_BUNDLED_BUILD=true
export BOOST_SOURCE=BUNDLED

# Feature Toggles
export ARROW_COMPUTE=ON
export ARROW_CSV=ON
export ARROW_DATASET=ON
export ARROW_EXTRA_ERROR_CONTEXT=ON
export ARROW_FILESYSTEM=ON
export ARROW_JEMALLOC=ON
export ARROW_JSON=ON
export ARROW_PARQUET=ON
export ARROW_AZURE=ON

# Visibility into build
export ARROW_R_DEV=TRUE

# Use multiple available cores
export MAKEFLAGS="-j8"

# Compression Codecs
export ARROW_WITH_SNAPPY=ON
export ARROW_WITH_ZLIB=ON

# Library Linkage
export ARROW_BUILD_STATIC=ON
export ARROW_BUILD_SHARED=OFF

# For R-specific behavior (replaces CMAKE_INSTALL_LIBDIR=lib)
export LIBARROW_BINARY=false

export EXTRA_CMAKE_FLAGS="-DARROW_INSTALL_NAME_RPATH=OFF -DARROW_AZURE=ON -DCMAKE_SHARED_LINKER_FLAGS=-lxml2"

export PKG_CONFIG_PATH="/usr/lib/x86_64-linux-gnu/pkgconfig"
export LDFLAGS=$(pkg-config --libs libxml-2.0)
export PKG_LIBS=$(pkg-config --libs libxml-2.0)

# export LIBARROW_EXTERNAL_LIBDIR=/workspaces/arrow/r/libarrow

R CMD INSTALL . --preclean

```