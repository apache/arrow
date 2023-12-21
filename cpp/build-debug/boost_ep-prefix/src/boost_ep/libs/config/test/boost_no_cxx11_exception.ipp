//  (C) Copyright Beman Dawes 2009

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX11_HDR_EXCEPTION
//  TITLE:         C++11 header <exception> not compatible
//  DESCRIPTION:   The standard library does not provide a C++11 compatible version of <exception>.

#include <exception>

namespace boost_no_cxx11_hdr_exception {

   int test()
   {
#ifdef BOOST_NO_EXCEPTIONS
      using std::exception_ptr;
      using std::current_exception;
      using std::rethrow_exception;
      return 0;
#else
      std::exception_ptr ep;
      try
      {
         throw 42;
      }
      catch (...)
      {
         ep = std::current_exception();
      }
      try
      {
         std::rethrow_exception(ep);
      }
      catch (int i)
      {
         // return zero on success
         return i == 42 ? 0 : 1;
      }
      return 1;
#endif
   }

}
