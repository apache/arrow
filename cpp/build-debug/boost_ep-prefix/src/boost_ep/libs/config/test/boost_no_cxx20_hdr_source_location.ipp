//  (C) Copyright John Maddock 2021

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX20_HDR_SOURCE_LOCATION
//  TITLE:         C++20 <source_location> header is either not present or too broken to be used
//  DESCRIPTION:   The compiler does not support the C++20 header <source_location>

#include <source_location>

namespace boost_no_cxx20_hdr_source_location {

   using std::source_location;

   int test()
   {
      return 0;
   }

}
