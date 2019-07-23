## Test environments
* Fedora Linux, R-devel, clang, gfortran
* Debian Linux, R-devel, GCC ASAN/UBSAN
* Ubuntu Linux 16.04 LTS, R-release, GCC
* win-builder (R-devel and R-release)
* macOS (10.11, 10.14), R-release

## R CMD check results

There were no ERRORs, WARNINGs, or NOTEs.

## Feedback from initial submission

Version 0.14.0 was submitted to CRAN on 18 July 2019. The CRAN team requested two revisions, which have been addressed in this submission.

1. Source files contain a comment header, required in all source files in Apache Software Foundation projects (see https://www.apache.org/legal/src-headers.html), which mentions a NOTICE file that is not included in the package.

This submission includes a NOTICE.txt file in the inst/ directory.

2. Rd files for main exported functions should have executable (not in \dontrun{}) examples.

This submission includes examples for the user-facing functions (read_parquet, write_parquet, read_feather, write_feather, et al.)
