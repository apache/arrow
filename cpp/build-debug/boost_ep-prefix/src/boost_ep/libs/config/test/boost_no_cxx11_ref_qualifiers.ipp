//  (C) Copyright Andrzej Krzemienski 2014

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX11_REF_QUALIFIERS
//  TITLE:         C++11 ref-qualifiers on member functions.
//  DESCRIPTION:   The compiler does not support the C++11 ref-qualifiers on member functions as described in N2439.

namespace boost_no_cxx11_ref_qualifiers {

struct G
{
   char get() & { return 'l'; }
   char get() && { return 'r'; }
   char get() const& { return 'c'; }
};

int test()
{
   G m;
   const G c = G();
   
   if (m.get() != 'l') return 1;
   if (c.get() != 'c') return 1;
   if (G().get() != 'r') return 1;
   return 0;
}

}
