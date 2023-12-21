//  (C) Copyright John Maddock 2001. 
//  Use, modification and distribution are subject to the 
//  Boost Software License, Version 1.0. (See accompanying file 
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for most recent version.

//  MACRO:         BOOST_NO_CTYPE_FUNCTIONS
//  TITLE:         functions in <ctype.h>
//  DESCRIPTION:   The Platform does not provide functions for the character-
//                 classifying operations in <ctype.h>. Some platforms provide
//                 macros and don't provide functions. Under C++ it's an error
//                 to provide the macros at all, but that's a separate issue.

#include <cctype>

namespace boost_no_ctype_functions {

int test()
{
   using std::isalpha;
   using std::isalnum;
   using std::iscntrl;
   using std::isdigit;
   using std::isgraph;
   using std::islower;
   using std::isprint;
   using std::ispunct;
   using std::isspace;
   using std::isupper;
   using std::isxdigit;

   int r = 0;
   char c = 'a';

   r |= (isalpha)(c);
   r |= (isalnum)(c);
   r |= (iscntrl)(c);
   r |= (isdigit)(c);
   r |= (isgraph)(c);
   r |= (islower)(c);
   r |= (isprint)(c);
   r |= (ispunct)(c);
   r |= (isspace)(c);
   r |= (isupper)(c);
   r |= (isxdigit)(c);

   return r == 0 ? 1 : 0;
}

}

