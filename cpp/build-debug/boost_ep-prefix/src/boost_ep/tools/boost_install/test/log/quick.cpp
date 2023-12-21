
// Copyright 2018 Peter Dimov.
//
// Distributed under the Boost Software License, Version 1.0.
//
// See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt

#include <boost/log/trivial.hpp>

int main()
{
    BOOST_LOG_TRIVIAL(info) << "An informational message";
    BOOST_LOG_TRIVIAL(warning) << "A warning message";
}
