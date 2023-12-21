/*
Copyright 2017 Glen Joseph Fernandes
(glenjofe@gmail.com)

Distributed under Boost Software License, Version 1.0.
(See accompanying file LICENSE_1_0.txt or copy at
http://www.boost.org/LICENSE_1_0.txt)
*/

// MACRO: BOOST_NO_CXX17_INLINE_VARIABLES
// TITLE: C++17 inline variables
// DESCRIPTION: C++17 inline variables are not supported.

namespace boost_no_cxx17_inline_variables {

inline const int Value = 1;

struct Type {
   static inline const int value = 1;
};

int test()
{
    return Type::value - Value;
}

} /* boost_no_cxx17_inline_variables */
