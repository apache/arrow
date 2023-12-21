//  Copyright (C) 2007 Douglas Gregor
//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for most recent version.

//  MACRO:         BOOST_NO_CXX11_FIXED_LENGTH_VARIADIC_TEMPLATE_EXPANSION_PACKS
//  TITLE:         C++0x variadic templates unavailable
//  DESCRIPTION:   The compiler does not support C++0x variadic templates

namespace boost_no_cxx11_fixed_length_variadic_template_expansion_packs {

template<char one, char two, char... Others> struct char_tuple {};

template<char... Args> struct super_class : public char_tuple<Args...> {};

int test()
{
   super_class<'a', 'b', 'c', 'd'> sc;
   (void)sc;
   return 0;
}

}

