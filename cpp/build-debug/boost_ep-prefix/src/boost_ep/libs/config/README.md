Boost Config Library
============================

This library provides configuration support for the Boost C++ libraries.

The full documentation is available on [boost.org](http://www.boost.org/doc/libs/release/libs/config/index.html).

|                  |  Master  |   Develop   |
|------------------|----------|-------------|
| Drone            |  [![Build Status](https://drone.cpp.al/api/badges/boostorg/config/status.svg?ref=refs/heads/master)](https://drone.cpp.al/boostorg/config) | [![Build Status](https://drone.cpp.al/api/badges/boostorg/config/status.svg)](https://drone.cpp.al/boostorg/config) |
| Travis           | [![Build Status](https://travis-ci.org/boostorg/config.svg?branch=master)](https://travis-ci.org/boostorg/config)  |  [![Build Status](https://travis-ci.org/boostorg/config.png)](https://travis-ci.org/boostorg/config) |
| Appveyor         | [![Build status](https://ci.appveyor.com/api/projects/status/wo2n2mhoy8vegmuo/branch/master?svg=true)](https://ci.appveyor.com/project/jzmaddock/config/branch/master) | [![Build status](https://ci.appveyor.com/api/projects/status/wo2n2mhoy8vegmuo/branch/develop?svg=true)](https://ci.appveyor.com/project/jzmaddock/config/branch/develop) |

## Support, bugs and feature requests ##

Bugs and feature requests can be reported through the [Gitub issue tracker](https://github.com/boostorg/config/issues)
(see [open issues](https://github.com/boostorg/config/issues) and
[closed issues](https://github.com/boostorg/config/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aclosed)).

You can submit your changes through a [pull request](https://github.com/boostorg/config/pulls).

There is no mailing-list specific to Boost Config, although you can use the general-purpose Boost [mailing-list](http://lists.boost.org/mailman/listinfo.cgi/boost-users) using the tag [config].


## Development ##

Clone the whole boost project, which includes the individual Boost projects as submodules ([see boost+git doc](https://github.com/boostorg/boost/wiki/Getting-Started)): 

    git clone https://github.com/boostorg/boost
    cd boost
    git submodule update --init

The Boost Config Library is located in `libs/config/`. 

### Running tests ###
First, make sure you are in `libs/config/test`. 
You can either run all the tests listed in `Jamfile.v2` or run a single test:

    ../../../b2                        <- run all tests
    ../../../b2 config_info            <- single test

### For developers ###
Please check the [Guidelines for Boost Authors](http://www.boost.org/doc/libs/release/libs/config/doc/html/boost_config/guidelines_for_boost_authors.html). from the full documentation.
