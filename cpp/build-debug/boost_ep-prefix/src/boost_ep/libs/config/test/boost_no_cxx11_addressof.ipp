//  (C) Copyright John Maddock 2012
//  (C) Copyright Peter Dimov 2014

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX11_ADDRESSOF
//  TITLE:         C++11 <memory> doesn't have a working std::addressof
//  DESCRIPTION:   The compiler does not support the function std::addressof added to <memory>

#include <memory>

namespace boost_no_cxx11_addressof {

void x3()
{
}

int test()
{
   int x1, x2[3];
   return std::addressof(x1) != &x1 || std::addressof(x2) != &x2 || std::addressof(x3) != &x3;
}

}
