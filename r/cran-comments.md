## Test environments
* local OS X install, R 3.5.3
* win-builder (devel and release)

## R CMD check results

0 errors | 0 warnings | 1 note

* This is a new release.

## Platform support

This package supports Windows and macOS but not Linux.

The Arrow project is cross-language development platform
for in-memory data, it spans several languages and
their code base is quite large (about 150K lines of C
sources and more than 600K lines across all languages).

In the future, the Apache Arrow project will release
binaries in the official Fedora and Debian repos;
we're working on hard on this, but due to the size,
this is likely to be implemented until next year.

In the meantime, R users can install the Linux binaries
from custom repos or build Arrow from source when using
Linux.
