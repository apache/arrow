//  (C) Copyright John Maddock 2012. 
//  (C) Copyright Dynatrace 2017. 
//  Use, modification and distribution are subject to the 
//  Boost Software License, Version 1.0. (See accompanying file 
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for most recent version.

//  MACRO:         BOOST_HAS_INT128
//  TITLE:         __int128
//  DESCRIPTION:   The platform supports __int128.

#include <cstdlib>
#include <stdio.h>
#include <limits.h>

namespace boost_has_int128{

#ifdef __GNUC__
__extension__ typedef __int128 my_int128_t;
__extension__ typedef unsigned __int128 my_uint128_t;
#else
typedef __int128 my_int128_t;
typedef unsigned __int128 my_uint128_t;
#endif

my_uint128_t volatile g_ui128 = 0;
unsigned long volatile g_ul = 0;

int test()
{
   my_int128_t si128 = 0;
   (void)&si128;

   // Some compilers have seriously broken __int128 implementations, so we need to do a little more than simply check if we can declare variables with __int128...
   // #1: check __int128 size
   if (sizeof(my_uint128_t) < (128 / CHAR_BIT))
   {
      fputs("Type too small.", stderr);
      return 1;
   }

   // #2: check result of computation with __int128
   my_uint128_t p1 = 1;
   my_uint128_t p2 = 1;
   unsigned int i = 0;
   for (; i < 180; i++)
   {
      g_ui128 = p1 + p2;
      if (g_ui128 < p1)
      {
         fputs("Unexpected overflow.", stderr);
         return 1;
      }
      p2 = p1;
      p1 = g_ui128;
   }

   g_ul = static_cast<unsigned long>((g_ui128 >> 92) & 0xFFFFFFFFUL);
   g_ul -= 1216382273UL;
   if (g_ul != 0)
   {
      fputs("Incorrect computation result.", stderr);
      return 1;
   }
   
   my_uint128_t ii(2), jj(1), kk;
   kk = ii / jj;

   return 0;
}

}





