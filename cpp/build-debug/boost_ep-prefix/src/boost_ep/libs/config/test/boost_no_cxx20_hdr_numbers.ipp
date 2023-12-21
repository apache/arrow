//  (C) Copyright John Maddock 2021

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX20_HDR_NUMBERS
//  TITLE:         C++20 <numbers> header is either not present or too broken to be used
//  DESCRIPTION:   The compiler does not support the C++20 header <numbers>

#include <numbers>

namespace boost_no_cxx20_hdr_numbers {

   using std::numbers::e_v;
   using std::numbers::log2e_v;
   using std::numbers::log10e_v;
   using std::numbers::pi_v;
   using std::numbers::inv_pi_v;
   using std::numbers::inv_sqrtpi_v;
   using std::numbers::ln2_v;
   using std::numbers::ln10_v;
   using std::numbers::sqrt2_v;
   using std::numbers::sqrt3_v;
   using std::numbers::inv_sqrt3_v;
   using std::numbers::egamma_v;
   using std::numbers::phi_v;

   int test()
   {
      return 0;
   }

}
