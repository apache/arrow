# boost_install

This repository contains the implementation of the
`boost-install` rule. This rule, when called from
a library `Jamfile`, creates an `install` target
that installs the library and its CMake configuration
file. It also invokes the `install` targets of its
dependencies.
