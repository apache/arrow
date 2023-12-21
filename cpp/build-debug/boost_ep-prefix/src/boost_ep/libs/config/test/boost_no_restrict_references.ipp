//  (C) Copyright Beman Dawes 2009

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_RESTRICT_REFERENCES
//  TITLE:         We cannot apply BOOST_RESTRICT to a reference type.
//  DESCRIPTION:   We cannot apply BOOST_RESTRICT to a reference type

#include <boost/config.hpp>

namespace boost_no_restrict_references {

#ifdef _MSC_VER
#pragma warning(error:4227)
#endif


void sum2(int (& BOOST_RESTRICT a)[4], int (& BOOST_RESTRICT b)[4], int (&c)[4], int (&d)[4]) {  
   int i;  
   for (i = 0; i < 4; i++) {  
      a[i] = b[i] + c[i];  
      c[i] = b[i] + d[i];  
    }  
}  

int test()
{
  int a[4] = { 1, 2, 3, 4 };
  int b[4] = { 3, 4, 5, 6 };
  int c[4] = { 0, 1, 3, 5 };
  int d[4] = { 2, 4, 6, 8 };

  sum2(a, b, c, d);

  return 0;
}

#ifdef _MSC_VER
#pragma warning(default:4227)
#endif


}
