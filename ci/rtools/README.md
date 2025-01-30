# rtools40 patches for AWS SDK and related libs

The patches in this directory are solely for the purpose of building Arrow C++
under [Rtools40](https://cran.r-project.org/bin/windows/Rtools/rtools40.html)
and not used elsewhere. Once we've dropped support for Rtools40, we can consider
removing these patches.

The larger reason these patches are needed is that Rtools provides their own
packages and their versions of the AWS libraries weren't compatible with CMake
3.25. Our solution was to bundle the AWS libs instead and these patches were
required to get them building under the Rtools40 environment.

The patches were added while upgrading the minimum required CMake version to
3.25 in [GH-44950](https://github.com/apache/arrow/issues/44950). Please see the
associated PR, [GH-44989](https://github.com/apache/arrow/pull/44989), for more
context.
