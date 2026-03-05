# Temporary development notes

> TODO: Remove this before we open a PR to upstream arrow library.

## Using codegen.R

1. Install repo dependencies in `arrow/r`: `install.packages("remotes")`, then `remotes::install_deps(dependencies = TRUE)`

2. Rscript `data-raw/codegen.R`

The second step auto-generates stubs in `arrowExports.R` and `arrowExports.cpp` based on which C++ functions have `// [[arrow::export]]` comments above them.
