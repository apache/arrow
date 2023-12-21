//  (C) Copyright John Maddock 2021

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX20_HDR_LATCH
//  TITLE:         C++20 <latch> header is either not present or too broken to be used
//  DESCRIPTION:   The compiler does not support the C++20 header <latch>

#include <latch>

namespace boost_no_cxx20_hdr_latch {

   using std::latch;

   int test()
   {
      return 0;
   }

}
