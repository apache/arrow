//  (C) Copyright Oliver Kowalke 2016. 
//  Use, modification and distribution are subject to the 
//  Boost Software License, Version 1.0. (See accompanying file 
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for most recent version.

//  MACRO:         BOOST_NO_CXX17_STD_INVOKE
//  TITLE:         invoke
//  DESCRIPTION:   The compiler supports the std::invoke() function.

#include <functional>

namespace boost_no_cxx17_std_invoke {

int foo( int i, int j) {
    return i + j;
}

int test() {
    int i = 1, j = 2;
    typename std::invoke_result<int(&)(int,int), int, int>::type t = std::invoke( foo, i, j);
    return 0;
}

}

