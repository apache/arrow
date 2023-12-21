
//  (C) Copyright Kohei Takahashi 2014

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX14_BINARY_LITERALS
//  TITLE:         C++14 binary literals unavailable
//  DESCRIPTION:   The compiler does not support C++14 binary literals

namespace boost_no_cxx14_binary_literals
{

int test()
{
    return ((int)0b01000010 == (int)0x42) ? 0 : 1;
}

}

