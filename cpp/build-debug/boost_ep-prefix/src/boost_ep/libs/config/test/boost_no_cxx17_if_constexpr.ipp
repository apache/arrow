/*
Copyright 2018 T. Zachary Laine
(whatwasthataddress@gmail.com)

Distributed under Boost Software License, Version 1.0.
(See accompanying file LICENSE_1_0.txt or copy at
http://www.boost.org/LICENSE_1_0.txt)
*/

// MACRO: BOOST_NO_CXX17_IF_CONSTEXPR
// TITLE: C++17 if constexpr
// DESCRIPTION: C++17 if constexpr are not supported.

namespace boost_no_cxx17_if_constexpr {

template <typename T, typename U>
struct same
{
    static constexpr bool value = false;
};

template <typename T>
struct same<T, T>
{
    static constexpr bool value = true;
};

int test()
{
    if constexpr (true) {
        if constexpr (1 != 0) {
            if constexpr (same<int, double>::value) {
                static_assert(!same<int, double>::value, "");
                return 1;
            } else if constexpr (false) {
                return 1;
            } else {
                return 0;
            }
        }
    }
    return 1;
}

}
