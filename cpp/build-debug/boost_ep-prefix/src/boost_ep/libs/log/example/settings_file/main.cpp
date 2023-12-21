/*
 *          Copyright Andrey Semashev 2007 - 2015.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   main.cpp
 * \author Andrey Semashev
 * \date   26.04.2008
 *
 * \brief  An example of initializing the library from a settings file.
 *         See the library tutorial for expanded comments on this code.
 *         It may also be worthwhile reading the Wiki requirements page:
 *         http://www.crystalclearsoftware.com/cgi-bin/boost_wiki/wiki.pl?Boost.Logging
 */

// #define BOOST_ALL_DYN_LINK 1

#include <exception>
#include <iostream>
#include <fstream>

#include <boost/log/trivial.hpp>
#include <boost/log/common.hpp>
#include <boost/log/attributes.hpp>
#include <boost/log/utility/setup/from_stream.hpp>

namespace logging = boost::log;
namespace attrs = boost::log::attributes;

void try_logging()
{
    BOOST_LOG_TRIVIAL(trace) << "This is a trace severity record";
    BOOST_LOG_TRIVIAL(debug) << "This is a debug severity record";
    BOOST_LOG_TRIVIAL(info) << "This is an info severity record";
    BOOST_LOG_TRIVIAL(warning) << "This is a warning severity record";
    BOOST_LOG_TRIVIAL(error) << "This is an error severity record";
    BOOST_LOG_TRIVIAL(fatal) << "This is a fatal severity record";
}

int main(int argc, char* argv[])
{
    try
    {
        // Open the file
        std::ifstream settings("settings.txt");
        if (!settings.is_open())
        {
            std::cout << "Could not open settings.txt file" << std::endl;
            return 1;
        }

        // Read the settings and initialize logging library
        logging::init_from_stream(settings);

        // Add some attributes
        logging::core::get()->add_global_attribute("TimeStamp", attrs::local_clock());

        // Try logging
        try_logging();

        // Now enable tagging and try again
        BOOST_LOG_SCOPED_THREAD_TAG("Tag", "TAGGED");
        try_logging();

        return 0;
    }
    catch (std::exception& e)
    {
        std::cout << "FAILURE: " << e.what() << std::endl;
        return 1;
    }
}
