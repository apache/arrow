//  (C) Copyright John Maddock 2012. 
//  Use, modification and distribution are subject to the 
//  Boost Software License, Version 1.0. (See accompanying file 
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for most recent version.

//  MACRO:         BOOST_HAS_FLOAT128
//  TITLE:         __float128
//  DESCRIPTION:   The platform supports __float128.

#include <cstdlib>


namespace boost_has_float128{

int test()
{
#ifdef __GNUC__
   __extension__ __float128 big_float = 0.0Q;
#else
   __float128 big_float = 0.0Q;
#endif
   (void)&big_float;
   
   __float128 i(2.00), j(1.00), k;
   k = i / j;
   
   return 0;
}

}

