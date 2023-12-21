//  (C) Copyright John Maddock 2020

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX17_HDR_ANY
//  TITLE:         C++17 header <any> unavailable
//  DESCRIPTION:   The standard library does not supply C++17 header <any>

#include <any>

namespace boost_no_cxx17_hdr_any {

int test()
{
  using std::bad_any_cast;
  using std::any;
  using std::make_any;
  using std::any_cast;
  return 0;
}

}
