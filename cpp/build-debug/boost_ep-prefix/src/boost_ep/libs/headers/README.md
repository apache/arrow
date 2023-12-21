# headers

This is a "fake" library that installs the Boost headers on
`b2 libs/headers/build//install`.

It also creates a CMake target `Boost::headers`, on which the
non-header library targets such as `Boost::filesystem` depend.
