/*
Copyright 2020 Glen Joseph Fernandes
(glenjofe@gmail.com)

Distributed under Boost Software License, Version 1.0.
(http://www.boost.org/LICENSE_1_0.txt)
*/
#include <boost/config.hpp>

struct base {
    virtual void first() = 0;
    virtual void second() { }
};

struct derived
    : base {
    void first() BOOST_OVERRIDE { }
    void second() BOOST_OVERRIDE { }
};
