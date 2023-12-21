
//  (C) Copyright Kohei Takahashi 2014,2016

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX14_CONSTEXPR
//  TITLE:         C++14 relaxed constexpr unavailable
//  DESCRIPTION:   The compiler does not support C++14 relaxed constexpr

namespace boost_no_cxx14_constexpr
{

namespace detail
{
    template <class> struct void_ { typedef void type; };

    struct non_tmpl
    {
        constexpr int foo() const { return 1; }
        constexpr int foo()       { return 0; }
    };

    template <typename T>
    struct tmpl : non_tmpl { };
}

// Test relaxed constexpr with dependent type; for more details, see comment of
// BOOST_CXX14_CONSTEXPR definition in boost/config/compiler/clang.hpp .
template <class T>
constexpr typename detail::void_<T>::type decrement(T &value)
{
    --value;
}

constexpr int non_cv_member(detail::non_tmpl x)
{
    return x.foo();
}

template <typename T>
constexpr int non_cv_member(detail::tmpl<T> x)
{
    return x.foo();
}

constexpr int zero()
{
    int ret = 1;
    decrement(ret);
    return ret;
}

template <int v> struct compile_time_value
{
    static constexpr int value = v;
};

int test()
{
    return compile_time_value<
        zero()
      + non_cv_member(detail::non_tmpl())
      + non_cv_member(detail::tmpl<int>())
    >::value;
}

}

