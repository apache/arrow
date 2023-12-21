/*
Copyright 2017 Glen Joseph Fernandes
(glenjofe@gmail.com)

Distributed under Boost Software License, Version 1.0.
(See accompanying file LICENSE_1_0.txt or copy at
http://www.boost.org/LICENSE_1_0.txt)
*/

// MACRO: BOOST_NO_CXX17_STRUCTURED_BINDINGS
// TITLE: C++17 structured bindings
// DESCRIPTION: C++17 structured bindings are not supported.

#include <tuple>

namespace boost_no_cxx17_structured_bindings {

struct P {
    int x;
    int y;
};

int test()
{
    auto [c, d] = std::make_tuple(1, 2);
    if (c != 1 || d != 2) {
        return 1;
    }
    auto [a, b] = P{1, 2};
    if (a != 1 || b != 2) {
        return 1;
    }
    return 0;
}

} /* boost_no_cxx17_structured_bindings */
