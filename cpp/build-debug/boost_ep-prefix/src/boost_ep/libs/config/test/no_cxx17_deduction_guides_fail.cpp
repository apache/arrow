//  This file was automatically generated on Sun Jun  5 16:50:17 2022
//  by libs/config/tools/generate.cpp
//  Copyright John Maddock 2002-21.
//  Use, modification and distribution are subject to the 
//  Boost Software License, Version 1.0. (See accompanying file 
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for the most recent version.//
//  Revision $Id$
//


// Test file for macro BOOST_NO_CXX17_DEDUCTION_GUIDES
// This file should not compile, if it does then
// BOOST_NO_CXX17_DEDUCTION_GUIDES should not be defined.
// See file boost_no_cxx17_deduction_guides.ipp for details

// Must not have BOOST_ASSERT_CONFIG set; it defeats
// the objective of this file:
#ifdef BOOST_ASSERT_CONFIG
#  undef BOOST_ASSERT_CONFIG
#endif

#include <boost/config.hpp>
#include "test.hpp"

#ifdef BOOST_NO_CXX17_DEDUCTION_GUIDES
#include "boost_no_cxx17_deduction_guides.ipp"
#else
#error "this file should not compile"
#endif

int main( int, char *[] )
{
   return boost_no_cxx17_deduction_guides::test();
}

