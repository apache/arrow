
//  (C) Copyright Kohei Takahashi 2014

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX14_INITIALIZED_LAMBDA_CAPTURES
//  TITLE:         C++14 initialized lambda capture unavailable
//  DESCRIPTION:   The compiler does not support C++14 initialized lambda capture

namespace boost_no_cxx14_initialized_lambda_captures
{

int test()
{
    return [ret = 0] { return ret; } ();
}

}

