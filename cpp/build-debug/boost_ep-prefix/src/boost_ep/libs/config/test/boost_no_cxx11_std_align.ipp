//  (C) Copyright John Maddock 2012
//  (C) Copyright Peter Dimov 2014

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX11_STD_ALIGN
//  TITLE:         C++11 <memory> doesn't have a working std::align
//  DESCRIPTION:   The compiler does not support the function std::align added to <memory>

#include <memory>

namespace boost_no_cxx11_std_align {

int test()
{
   char buffer[ 32 ];

   void * ptr = buffer + 1;
   std::size_t space = sizeof( buffer ) - 1;

   void * p2 = std::align( 4, 2, ptr, space );

   if( p2 == 0 ) return 1;
   if( p2 != ptr ) return 1;
   if( (size_t)p2 % 4 ) return 1;

   return 0;
}

}
