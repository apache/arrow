//  (C) Copyright John Maddock 2021

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX20_HDR_STOP_TOKEN
//  TITLE:         C++20 <stop_token> header is either not present or too broken to be used
//  DESCRIPTION:   The compiler does not support the C++20 header <stop_token>

#include <stop_token>

namespace boost_no_cxx20_hdr_stop_token {

   using std::stop_token;
   using std::stop_source;
   using std::stop_callback;
   using std::nostopstate_t;
   using std::nostopstate;

   int test()
   {
      return 0;
   }

}
