//  (C) Copyright John Maddock 2021

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX20_HDR_SYNCSTREAM
//  TITLE:         C++20 <syncstream> header is either not present or too broken to be used
//  DESCRIPTION:   The compiler does not support the C++20 header <syncstream>

#include <syncstream>

namespace boost_no_cxx20_hdr_syncstream {

   using std::basic_syncbuf;
   using std::basic_osyncstream;
   using std::syncbuf;
   using std::wsyncbuf;
   using std::osyncstream;
   using std::wosyncstream;

   int test()
   {
      return 0;
   }

}
