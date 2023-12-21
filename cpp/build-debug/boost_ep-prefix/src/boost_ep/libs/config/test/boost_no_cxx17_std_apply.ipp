//  (C) Copyright Oliver Kowalke 2016. 
//  Use, modification and distribution are subject to the 
//  Boost Software License, Version 1.0. (See accompanying file 
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for most recent version.

//  MACRO:         BOOST_NO_CXX17_STD_APPLY
//  TITLE:         apply
//  DESCRIPTION:   The compiler supports the std::apply() function.

#include <functional>
#include <tuple>

namespace boost_no_cxx17_std_apply {

int foo( int i, int j) {
    return i + j;
}

int test() {
    int i = 1, j = 2;
    std::apply( foo, std::make_tuple( i, j) );
    return 0;
}

}

