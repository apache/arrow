
// Copyright 2018 Peter Dimov.
//
// Distributed under the Boost Software License, Version 1.0.
//
// See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt

#include <boost/atomic.hpp>
#include <boost/core/lightweight_test.hpp>

struct X
{
    int u_;
    int v_;
    int w_;

    X(): u_( 0 ), v_( 0 ), w_( 0 )
    {
    }

    X( int u, int v, int w ): u_( u ), v_( v ), w_( w )
    {
    }
};

int main()
{
    boost::atomic<X> a;

    a.store( X( 1, 2, 3 ) );
    X x1 = a.exchange( X( 4, 5, 6 ) );

    BOOST_TEST_EQ( x1.u_, 1 );
    BOOST_TEST_EQ( x1.v_, 2 );
    BOOST_TEST_EQ( x1.w_, 3 );

    X x2 = a.load();

    BOOST_TEST_EQ( x2.u_, 4 );
    BOOST_TEST_EQ( x2.v_, 5 );
    BOOST_TEST_EQ( x2.w_, 6 );

    return boost::report_errors();
}
