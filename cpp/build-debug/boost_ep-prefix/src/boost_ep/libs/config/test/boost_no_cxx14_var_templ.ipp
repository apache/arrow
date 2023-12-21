
//  (C) Copyright Kohei Takahashi 2014

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX14_VARIABLE_TEMPLATES
//  TITLE:         C++14 variable templates unavailable
//  DESCRIPTION:   The compiler does not support C++14 variable templates

namespace boost_no_cxx14_variable_templates
{

template <class T>
T zero = static_cast<T>(0);

int test()
{
    return zero<int>;
}

}

