//  (C) Copyright Beman Dawes 2009
//  Copyright (c) Microsoft Corporation
//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX11_HDR_REGEX
//  TITLE:         C++0x header <regex> unavailable
//  DESCRIPTION:   The standard library does not supply C++0x header <regex>

#include <regex>

namespace boost_no_cxx11_hdr_regex {

int test()
{
  using std::regex;
  using std::wregex;

  regex e("\\d+");
  wregex we(L"\\d+");
  std::string s("123456");
  std::wstring ws(L"123456");
  return regex_match(s, e) && regex_match(ws, we) ? 0 : 1;
}

}
