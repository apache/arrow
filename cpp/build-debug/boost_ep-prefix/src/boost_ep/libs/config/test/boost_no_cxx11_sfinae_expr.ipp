/*
Copyright 2017 Glen Joseph Fernandes
(glenjofe@gmail.com)

Distributed under Boost Software License, Version 1.0.
(See accompanying file LICENSE_1_0.txt or copy at
http://www.boost.org/LICENSE_1_0.txt)
*/

// MACRO: BOOST_NO_CXX11_SFINAE_EXPR
// TITLE: C++11 SFINAE for expressions
// DESCRIPTION: C++11 SFINAE for expressions not supported.

namespace boost_no_cxx11_sfinae_expr {

template<class>
struct ignore {
    typedef void type;
};

template<class T>
T& object();

template<class T, class E = void>
struct trait {
    static const int value = 0;
};

template<class T>
struct trait<T, typename ignore<decltype(&object<T>())>::type> { };

template<class T>
struct result {
    static const int value = T::value;
};

class type {
    void operator&() const { }
};

int test()
{
    return result<trait<type> >::value;
}

} /* boost_no_cxx11_sfinae_expr */
