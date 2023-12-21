
// Copyright 2017, 2018 Peter Dimov.
//
// Distributed under the Boost Software License, Version 1.0.
//
// See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt

#include <boost/thread.hpp>
#include <boost/core/lightweight_test.hpp>

void f( int & x, int y )
{
    x = y;
}

int main()
{
    int x = 0;

    boost::thread th( f, boost::ref( x ), 12 );
    th.join();

    BOOST_TEST_EQ( x, 12 );

    return boost::report_errors();
}
