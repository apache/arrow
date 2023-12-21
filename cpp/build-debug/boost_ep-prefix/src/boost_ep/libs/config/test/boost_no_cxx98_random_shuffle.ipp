//  (C) Copyright John Maddock 2017.
//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for most recent version.

//  MACRO:         BOOST_NO_CXX98_RANDOM_SHUFFLE
//  TITLE:         std::random_shuffle
//  DESCRIPTION:   The std lib has random_shuffle.

#include <algorithm>

namespace boost_no_cxx98_random_shuffle{

int test()
{
   int my_array[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
   std::random_shuffle(&my_array[0], &my_array[9] );
   return 0;
}

}
