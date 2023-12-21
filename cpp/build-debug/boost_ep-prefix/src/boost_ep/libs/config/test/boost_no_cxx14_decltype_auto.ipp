
//  (C) Copyright Kohei Takahashi 2014

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX14_DECLTYPE_AUTO
//  TITLE:         C++14 decltype(auto) unavailable
//  DESCRIPTION:   The compiler does not support C++14 decltype(auto)

namespace boost_no_cxx14_decltype_auto
{

void quiet_warning(int){}

const int &foo(const int &x)
{
    return x;
}

int test()
{
    int j;
    decltype(auto) x = foo(j);
    quiet_warning(x);
    return 0;
}

}

