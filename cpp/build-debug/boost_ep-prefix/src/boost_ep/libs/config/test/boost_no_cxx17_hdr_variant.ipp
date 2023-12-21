//  (C) Copyright John Maddock 2019

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX17_HDR_VARIANT
//  TITLE:         C++17 header <variant> unavailable
//  DESCRIPTION:   The standard library does not supply C++17 header <variant>

#include <variant>

namespace boost_no_cxx17_hdr_variant {

int test()
{
  using std::variant;
  using std::visit;
  using std::holds_alternative;
  using std::get;
  using std::get_if;
  using std::monostate;
  using std::bad_variant_access;
  using std::variant_size;
  using std::variant_alternative;
  using std::variant_npos;
  return 0;
}

}
