//  Copyright 2017 Peter Dimov.
//
//  Distributed under the Boost Software License, Version 1.0.
//
//  See accompanying file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt

#include <boost/config/helper_macros.hpp>
#include <boost/core/lightweight_test.hpp>

int main()
{
#define X pumpkin

    BOOST_TEST_CSTR_EQ( BOOST_STRINGIZE(X), "pumpkin" );
    BOOST_TEST_CSTR_EQ( BOOST_STRINGIZE(__LINE__), "16" );

#define Y 2

    int BOOST_JOIN(X, Y) = 0;
    (void)pumpkin2;

    int BOOST_JOIN(X, __LINE__) = 0;
    (void)pumpkin23;

    BOOST_TEST_CSTR_EQ( BOOST_STRINGIZE(BOOST_JOIN(X, Y)), "pumpkin2" );
    BOOST_TEST_CSTR_EQ( BOOST_STRINGIZE(BOOST_JOIN(X, __LINE__)), "pumpkin27" );

    return boost::report_errors();
}
