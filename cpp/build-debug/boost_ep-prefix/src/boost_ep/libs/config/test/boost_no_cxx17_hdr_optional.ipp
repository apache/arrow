//  (C) Copyright John Maddock 2018

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX17_HDR_OPTIONAL
//  TITLE:         C++17 header <optional> unavailable
//  DESCRIPTION:   The standard library does not supply C++17 header <optional>

#include <optional>

namespace boost_no_cxx17_hdr_optional {

int test()
{
  using std::optional;
  return 0;
}

}
