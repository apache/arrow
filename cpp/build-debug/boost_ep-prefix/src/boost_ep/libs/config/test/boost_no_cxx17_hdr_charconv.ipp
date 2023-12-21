//  (C) Copyright John Maddock 2020

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX17_HDR_CHARCONV
//  TITLE:         C++17 header <charconv> unavailable
//  DESCRIPTION:   The standard library does not supply C++17 header <charconv>

#include <charconv>

namespace boost_no_cxx17_hdr_charconv {

int test()
{
  using std::chars_format;
  using std::to_chars_result;
  using std::to_chars;
  using std::from_chars_result;
  using std::from_chars;
  return 0;
}

}
