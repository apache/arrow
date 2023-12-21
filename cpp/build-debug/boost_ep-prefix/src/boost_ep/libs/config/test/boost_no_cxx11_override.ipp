/*
Copyright 2020 Glen Joseph Fernandes
(glenjofe@gmail.com)

Distributed under Boost Software License, Version 1.0.
(See accompanying file LICENSE_1_0.txt or copy at
http://www.boost.org/LICENSE_1_0.txt)
*/

// MACRO: BOOST_NO_CXX11_OVERRIDE
// TITLE: C++11 SFINAE for expressions
// DESCRIPTION: C++11 SFINAE for expressions not supported.

namespace boost_no_cxx11_override {

struct base {
    virtual void first() = 0;
    virtual void second() { }
};

struct derived
    : base {
    void first() override { }
    void second() override { }
};

int test()
{
    return 0;
}

} /* boost_no_cxx11_override */
