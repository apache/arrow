//  (C) Copyright Agustin Berge 2014

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX11_FINAL
//  TITLE:         C++11 final class-virt-specifier
//  DESCRIPTION:   The compiler does not support the C++ class-virt-specifier final

namespace boost_no_cxx11_final {

struct X final {};

struct abstract
{
   virtual int f() = 0;
};

struct derived : public abstract
{
   virtual int f() final
   {
      return 0;
   }
};

int check(abstract* pa)
{
   return pa->f();
}

int test()
{
    derived d;
    return check(&d);
}

}
