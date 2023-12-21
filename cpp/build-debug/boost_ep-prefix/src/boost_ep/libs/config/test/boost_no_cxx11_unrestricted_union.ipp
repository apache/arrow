//  (C) Copyright Edward Diener 2019

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX11_UNRESTRICTED_UNION
//  TITLE:         C++11 unrestricted union
//  DESCRIPTION:   The compiler does not support the C++11 unrestricted union

#include <new>

namespace boost_no_cxx11_unrestricted_union {

struct HoldsShort
    {
    short i;
    HoldsShort();
    };

HoldsShort::HoldsShort() : i(1)
    {
    }
    
union with_static_data
    {
    int a;
    long b;
    HoldsShort o;
    with_static_data();
    static int sd;
    };
    
with_static_data::with_static_data() :
    a(0)
    {
    }
    
int with_static_data::sd = 0;

int test()
{
  with_static_data wsd;
  wsd.a = 24;
  wsd.b = 48L;
  new(&wsd.o) HoldsShort;
  wsd.o.i = 2;
  with_static_data::sd = 1;
  bool b = (wsd.o.i == 2 && with_static_data::sd == 1);
  return b ? 0 : 1;
}

}
