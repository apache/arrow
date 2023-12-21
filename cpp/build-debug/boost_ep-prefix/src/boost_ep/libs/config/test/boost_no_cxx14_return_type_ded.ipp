
//  (C) Copyright Kohei Takahashi 2014

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX14_RETURN_TYPE_DEDUCTION
//  TITLE:         C++14 return type deduction unavailable
//  DESCRIPTION:   The compiler does not support C++14 return type deduction

namespace boost_no_cxx14_return_type_deduction
{

template<typename T>
auto deduced_abs(T x)
{
   return x > 0 ? x : -x;
}

int test()
{
    return deduced_abs(2) - deduced_abs(-2);
}

}

