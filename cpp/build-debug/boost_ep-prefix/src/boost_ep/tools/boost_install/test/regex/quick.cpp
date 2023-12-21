
// Copyright 2018 Peter Dimov.
//
// Distributed under the Boost Software License, Version 1.0.
//
// See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt

#include <boost/regex.hpp>
#include <boost/core/lightweight_test.hpp>

int main()
{
    boost::regex rx( "(\\d+)-+(\\d+)-+(\\d+)" );

    std::string s = boost::regex_replace( std::string( "+1--2--3+" ), rx, "$1$2$3" );

    BOOST_TEST_EQ( s, "+123+" );

    return boost::report_errors();
}
