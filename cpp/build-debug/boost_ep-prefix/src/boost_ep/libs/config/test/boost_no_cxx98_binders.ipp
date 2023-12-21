//  (C) Copyright John Maddock 2017.
//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for most recent version.

//  MACRO:         BOOST_NO_CXX98_BINDERS
//  TITLE:         std::bind1st, prt_fun and mem_fun
//  DESCRIPTION:   The std lib has C++98 binders and adaptors.

#include <functional>

namespace boost_no_cxx98_binders{

int f2(int a, int b) { return a + b; }

struct A
{
   int f1(int a) { return a; }
};


int test()
{
   A a;
   return std::bind1st(std::ptr_fun(f2), 0)(0) + std::bind1st(std::mem_fun(&A::f1), &a)(0);
}

}
