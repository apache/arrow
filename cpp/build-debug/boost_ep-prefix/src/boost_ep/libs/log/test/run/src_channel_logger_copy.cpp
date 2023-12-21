/*
 *             Copyright Andrey Semashev 2021.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   src_channel_logger_copy.cpp
 * \author Andrey Semashev
 * \date   10.02.2021
 *
 * \brief  This header contains tests for the channel logger copy constructor and assignment.
 */

#define BOOST_TEST_MODULE src_channel_logger_copy

#include <string>
#include <boost/test/unit_test.hpp>
#include <boost/log/sources/channel_logger.hpp>

namespace src = boost::log::sources;

// Test that copy constructor decouples the channel attribute
BOOST_AUTO_TEST_CASE(copy_constructor)
{
    src::channel_logger< std::string > lg1("channel1");
    src::channel_logger< std::string > lg2 = lg1;
    BOOST_CHECK_EQUAL(lg1.channel(), lg2.channel());

    lg2.channel("channel2");
    BOOST_CHECK_NE(lg1.channel(), lg2.channel());
}

// Test that copy assignment decouples the channel attribute
BOOST_AUTO_TEST_CASE(copy_assignment)
{
    src::channel_logger< std::string > lg1("channel1");
    src::channel_logger< std::string > lg2("channel2");
    BOOST_CHECK_NE(lg1.channel(), lg2.channel());

    lg2 = lg1;
    BOOST_CHECK_EQUAL(lg1.channel(), lg2.channel());

    lg2.channel("channel3");
    BOOST_CHECK_NE(lg1.channel(), lg2.channel());
}
