//  (C) Copyright John Maddock 2021

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX20_HDR_SEMAPHORE
//  TITLE:         C++20 <semaphore> header is either not present or too broken to be used
//  DESCRIPTION:   The compiler does not support the C++20 header <semaphore>

#include <semaphore>

namespace boost_no_cxx20_hdr_semaphore {

   using std::counting_semaphore;
   using std::binary_semaphore;

   int test()
   {
      return 0;
   }

}
