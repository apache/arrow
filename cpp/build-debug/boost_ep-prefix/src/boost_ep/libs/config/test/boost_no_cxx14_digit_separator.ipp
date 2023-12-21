
//  (C) Copyright Kohei Takahashi 2014

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX14_DIGIT_SEPARATORS
//  TITLE:         C++14 digit separator unavailable
//  DESCRIPTION:   The compiler does not support C++14 digit separator

namespace boost_no_cxx14_digit_separators
{

int test()
{
    return 0'0;
}

}

