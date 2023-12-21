/*
Copyright 2017 Glen Joseph Fernandes
(glenjofe@gmail.com)

Distributed under Boost Software License, Version 1.0.
(See accompanying file LICENSE_1_0.txt or copy at
http://www.boost.org/LICENSE_1_0.txt)
*/

// MACRO: BOOST_NO_CXX17_FOLD_EXPRESSIONS
// TITLE: C++17 fold expressions
// DESCRIPTION: C++17 fold expressions are not supported.

namespace boost_no_cxx17_fold_expressions {

template<class... Args>
auto sum(Args&&... args)
{
    return (args + ... + 0);
}

int test()
{
    return sum(1, -1, 1, 1, -1, -1);
}

} /* boost_no_cxx17_fold_expressions */
