//  (C) Copyright John Maddock 2018

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX17_HDR_STRING_VIEW
//  TITLE:         C++17 header <string_view> unavailable
//  DESCRIPTION:   The standard library does not supply C++17 header <string_view>

#include <string_view>

namespace boost_no_cxx17_hdr_string_view {

int test()
{
  using std::string_view;
  using std::wstring_view;
  using std::u16string_view;
  using std::u32string_view;
  return 0;
}

}
