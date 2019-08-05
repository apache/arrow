## Test environments
* Debian Linux, R-devel, GCC ASAN/UBSAN
* Ubuntu Linux 16.04 LTS, R-release, GCC
* win-builder (R-devel and R-release)
* macOS (10.11, 10.14), R-release
* Oracle Solaris 10, x86, 32-bit, R-patched 

## R CMD check results

There were no ERRORs or WARNINGs. On some platforms, there is a NOTE about the installed package size, as well as the "New submission" NOTE.

## Feedback from previous submission

Version 0.14.1 was submitted to CRAN on 24 July 2019. The CRAN team requested two revisions:

1. Put quotes around 'Arrow C++' in the package Description.

2. Remove usage of utils::installed.packages()

Both have been addressed in this resubmission.

## Feedback from initial submission

Version 0.14.0 was submitted to CRAN on 18 July 2019. The CRAN team requested two revisions, which have been addressed in this re-submission.

1. Source files contain a comment header, required in all source files in Apache Software Foundation projects (see https://www.apache.org/legal/src-headers.html), which mentions a NOTICE file. But the NOTICE file was not included in the package.

This submission includes a NOTICE.txt file in the inst/ directory.

2. Rd files for main exported functions should have executable (not in \dontrun{}) examples.

This submission includes examples for the user-facing functions (read_parquet, write_parquet, read_feather, write_feather, et al.)
