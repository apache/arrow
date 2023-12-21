//  Copyright John Maddock 2014.
//  Use, modification and distribution are subject to the 
//  Boost Software License, Version 1.0. (See accompanying file 
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for the most recent version.//
//  Revision $Id$
//
// We include this as it contains some workarounds we may need (like a declaration for ::gets to get clang building):
#include <boost/config.hpp>

#  include "../test/boost_has_int128.ipp"
#  include "../test/boost_no_constexpr.ipp"
#  include "../test/boost_no_cxx11_user_lit.ipp"

int main( int, char *[] )
{
   return boost_has_int128::test() || boost_no_cxx11_constexpr::test() || boost_no_cxx11_user_defined_literals::test();
}

