
// Copyright 2017 Peter Dimov.
//
// Distributed under the Boost Software License, Version 1.0.
//
// See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt

// See library home page at http://www.boost.org/libs/program_options

#include <boost/program_options.hpp>
#include <boost/core/lightweight_test.hpp>

namespace po = boost::program_options;

int main( int argc, char const* argv[] )
{
    po::options_description desc( "Allowed options" );

    desc.add_options()
        ( "path,p", po::value<std::string>(), "set initial path" )
    ;

    po::variables_map vm;

    try
    {
        po::store( po::parse_command_line( argc, argv, desc ), vm );
        po::notify( vm );
    }
    catch( std::exception const & x )
    {
        std::cerr << "Error: " << x.what() << std::endl;
        return 1;
    }

    std::string p;

    if( vm.count( "path" ) )
    {
        p = vm[ "path" ].as<std::string>();
    }

    std::string expected( "initial" );

    BOOST_TEST_EQ( p, expected );

    return boost::report_errors();
}
