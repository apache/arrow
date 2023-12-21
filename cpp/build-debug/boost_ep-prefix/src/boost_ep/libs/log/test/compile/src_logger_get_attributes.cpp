/*
 *          Copyright Andrey Semashev 2007 - 2015.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   src_logger_get_attributes.cpp
 * \author Andrey Semashev
 * \date   01.03.2014
 *
 * \brief  This header contains a test for logger \c get_attributes method.
 */

#include <boost/log/sources/logger.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/channel_logger.hpp>
#include <boost/log/sources/severity_channel_logger.hpp>

template< typename LoggerT >
void test()
{
    LoggerT lg;

    // Test that get_attributes(), which is a const method, can acquire the internal mutex in the threading model.
    lg.get_attributes();
}

int main(int, char*[])
{
    test< boost::log::sources::logger >();
    test< boost::log::sources::severity_logger< > >();
    test< boost::log::sources::channel_logger< > >();
    test< boost::log::sources::severity_channel_logger< > >();

#if !defined(BOOST_LOG_NO_THREADS)
    test< boost::log::sources::logger_mt >();
    test< boost::log::sources::severity_logger_mt< > >();
    test< boost::log::sources::channel_logger_mt< > >();
    test< boost::log::sources::severity_channel_logger_mt< > >();
#endif

    return 0;
}
