//  (C) Copyright John Maddock 2017.
//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for most recent version.

//  MACRO:         BOOST_NO_CXX98_FUNCTION_BASE
//  TITLE:         std::unary_function and std::binary_function
//  DESCRIPTION:   The std lib has unary_function and binary_function.

#include <functional>

namespace boost_no_cxx98_function_base{

struct A : public std::unary_function<int, int>{};
struct B : public std::binary_function<int, int, int>{};

int test()
{
   A a;
   B b;
   (void)a;
   (void)b;
   return static_cast<B::result_type>(0);
}

}
