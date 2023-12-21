
// Copyright 2018 Peter Dimov.
//
// Distributed under the Boost Software License, Version 1.0.
//
// See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt

#define BOOST_TEST_MODULE main
#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE( t1 )
{
    int i = 0;
    BOOST_TEST( i == 0 );
}
