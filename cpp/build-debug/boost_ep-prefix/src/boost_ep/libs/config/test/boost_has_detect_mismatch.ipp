//  (C) Copyright John Maddock 2018. 
//  Use, modification and distribution are subject to the 
//  Boost Software License, Version 1.0. (See accompanying file 
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for most recent version.

//  MACRO:         BOOST_HAS_PRAGMA_DETECT_MISMATCH
//  TITLE:         detect_mismatch pragma
//  DESCRIPTION:   The compiler supports #pragma detect_mismatch

#include <cstdlib>


namespace boost_has_pragma_detect_mismatch {

# pragma detect_mismatch("Boost_Config", "1")

int test()
{
   return 0;
}

}

