/*
 * Copyright 2022 Andrey Semashev
 *
 * Distributed under Boost Software License, Version 1.0.
 * (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 */

 // MACRO: BOOST_NO_CXX17_DEDUCTION_GUIDES
 // TITLE: C++17 class template argument deduction guides
 // DESCRIPTION: C++17 class template argument deduction guides are not supported.

namespace boost_no_cxx17_deduction_guides {

   template< typename T >
   struct foo
   {
      T m_val;

      template< typename U >
      foo(U const& x) : m_val(x) {}
   };

   template< typename T >
   foo(T const&)->foo< T >;


   int test()
   {
      foo x1(10);
      return x1.m_val - 10;
   }

} // boost_no_cxx17_deduction_guides
