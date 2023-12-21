//  (C) Copyright Andrzej Krzemienski 2018

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX11_DEFAULTED_MOVES 
//  TITLE:         C++0x defaulting of move constructor/assignment unavailable
//  DESCRIPTION:   The compiler does not support C++0x defaulting of move constructor/assignment

namespace boost_no_cxx11_defaulted_moves {

  struct foo {
    foo(foo&&) = default;
    foo& operator=(foo&&) = default;
  };

  int test()
  {
    return 0;
  }

}
