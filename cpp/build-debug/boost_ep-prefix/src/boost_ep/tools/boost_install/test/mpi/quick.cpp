
// Copyright 2019 Peter Dimov.
//
// Distributed under the Boost Software License, Version 1.0.
//
// See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt

#include <boost/mpi/environment.hpp>
#include <boost/core/lightweight_test.hpp>

int main()
{
    std::pair<int, int> ver = boost::mpi::environment::version();

    BOOST_TEST_EQ( ver.first, MPI_VERSION );
    BOOST_TEST_EQ( ver.second, MPI_SUBVERSION );

    return boost::report_errors();
}
