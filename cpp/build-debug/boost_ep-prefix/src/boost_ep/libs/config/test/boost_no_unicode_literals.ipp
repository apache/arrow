//  (C) Copyright Beman Dawes 2008

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX11_UNICODE_LITERALS
//  TITLE:         C++0x unicode literals unavailable
//  DESCRIPTION:   The compiler does not support C++0x Unicode literals (N2442)

namespace boost_no_cxx11_unicode_literals {

template <class CharT>
void quiet_warning(const CharT*){}

int test()
{
#if defined(__cpp_char8_type) || defined(__cpp_char8_t)
   // The change to char8_t in C++20 is a breaking change to the std:
   const char8_t* c8 = u8"";
#else
  const char* c8 = u8"";
#endif
  const char16_t* c16 = u"";
  const char32_t* c32 = U"";
  quiet_warning(c8);
  quiet_warning(c16);
  quiet_warning(c32);
  return 0;
}

}
