
// Copyright 2017-2020 Peter Dimov.
//
// Distributed under the Boost Software License, Version 1.0.
//
// See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt

#include <boost/core/lightweight_test.hpp>

unsigned get_random_number();

int main()
{
    BOOST_TEST_NE( get_random_number(), get_random_number() );
    return boost::report_errors();
}
