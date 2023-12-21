//  (C) Copyright Andrey Semashev 2014

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX11_NON_PUBLIC_DEFAULTED_FUNCTIONS
//  TITLE:         C++11 non-public defaulted functions unavailable
//  DESCRIPTION:   The compiler does not support C++11 defaulted functions in access control sections other than public

#if defined(__GNUC__) && !defined(__GXX_EXPERIMENTAL_CXX0X__) && !defined(BOOST_INTEL_STDCXX0X)
#  error Non-public defaulted functions are not supported in non-C++11 mode
#endif

namespace boost_no_cxx11_non_public_defaulted_functions {

struct foo
{
protected:
    foo() = default;
    foo& operator= (foo const&) = default;
};

struct bar
{
private:
    bar() = default;
    bar& operator= (bar const&) = default;
};

int test()
{
    return 0;
}

}
