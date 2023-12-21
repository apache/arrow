//  (C) Copyright John Maddock 2021

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX20_HDR_BIT
//  TITLE:         C++20 <bit> header is either not present or too broken to be used
//  DESCRIPTION:   The compiler does not support the C++20 header <bit>

#include <bit>

namespace boost_no_cxx20_hdr_bit {

   using std::bit_cast;
   using std::has_single_bit;
   using std::bit_ceil;
   using std::bit_floor;
   using std::bit_width;
   using std::rotl;
   using std::rotr;
   using std::countl_zero;
   using std::countl_one;
   using std::countr_zero;
   using std::countr_one;
   using std::popcount;

   int test()
   {
      return 0;
   }

}
