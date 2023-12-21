//  (C) Copyright John Maddock 2021

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX20_HDR_COROUTINE
//  TITLE:         C++20 <coroutine> header is either not present or too broken to be used
//  DESCRIPTION:   The compiler does not support the C++20 header <coroutine>

#include <coroutine>

namespace boost_no_cxx20_hdr_coroutine {

   using std::coroutine_traits;
   using std::coroutine_handle;
   using std::noop_coroutine_promise;
   using std::noop_coroutine_handle;
   using std::noop_coroutine;
   using std::suspend_never;
   using std::suspend_always;

   int test()
   {
      return 0;
   }

}
