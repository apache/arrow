## Test environments

* Debian Linux, GCC, R-devel/R-patched/R-release
* Fedora Linux, GCC/clang, R-devel
* Ubuntu Linux 16.04 LTS, R-release, GCC
* win-builder (R-devel and R-release)
* macOS 10.14, R-oldrel

## R CMD check results

There were no ERRORs or WARNINGs. On some platforms, there is a NOTE about the installed package size.

## Reverse dependency test failures

Reverse dependency checks failed for dataversionr - we have both opened an [issue on the repo](https://github.com/riazarbi/dataversionr/issues/1) and emailed the maintainer, but no response.

Reverse dependency checks failed for pins - we notified them and they updated their tests.
