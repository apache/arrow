/*
Copyright 2017 Glen Joseph Fernandes
(glenjofe@gmail.com)

Distributed under Boost Software License, Version 1.0.
(See accompanying file LICENSE_1_0.txt or copy at
http://www.boost.org/LICENSE_1_0.txt)
*/

// MACRO: BOOST_NO_CXX11_POINTER_TRAITS
// TITLE: C++11 <memory> lacks a correct std::pointer_traits
// DESCRIPTION: The standard library lacks a working std::pointer_traits.

#include <memory>

namespace boost_no_cxx11_pointer_traits {

template<class T>
struct pointer {
    template<class U>
    using rebind = pointer<U>;
};

template<class T>
struct result { };

template<>
struct result<pointer<bool> > {
    static const int value = 0;
};

int test()
{
    return result<std::pointer_traits<pointer<int> >::rebind<bool> >::value;
}

} /* boost_no_cxx11_pointer_traits */
