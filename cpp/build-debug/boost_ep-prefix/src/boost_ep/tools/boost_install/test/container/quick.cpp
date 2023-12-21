
// Copyright 2018 Peter Dimov.
//
// Distributed under the Boost Software License, Version 1.0.
//
// See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt

#include <boost/container/pmr/vector.hpp>
#include <boost/core/lightweight_test.hpp>

int main()
{
    boost::container::pmr::vector_of<int>::type v;

    v.push_back( 1 );
    v.push_back( 2 );

    BOOST_TEST_EQ( v.size(), 2 );

    BOOST_TEST_EQ( v[0], 1 );
    BOOST_TEST_EQ( v[1], 2 );

    return boost::report_errors();
}
