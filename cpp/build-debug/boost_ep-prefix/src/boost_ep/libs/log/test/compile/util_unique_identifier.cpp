/*
 *          Copyright Andrey Semashev 2007 - 2015.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   util_unique_identifier.cpp
 * \author Andrey Semashev
 * \date   24.01.2009
 *
 * \brief  This header contains tests for the unique identifier name generator.
 */

#include <boost/log/utility/unique_identifier_name.hpp>

// Some hints to avoid warnings about unused variables in this test
#if defined(__GNUC__)
#define BOOST_LOG_AUX_UNUSED_ATTR __attribute__((unused))
#else
#define BOOST_LOG_AUX_UNUSED_ATTR
#endif

int main(int, char*[])
{
    // Names with the same prefixes may coexist in different lines
    BOOST_LOG_AUX_UNUSED_ATTR int BOOST_LOG_UNIQUE_IDENTIFIER_NAME(var) = 0;
    BOOST_LOG_AUX_UNUSED_ATTR int BOOST_LOG_UNIQUE_IDENTIFIER_NAME(var) = 0;

    // Names with different prefixes may coexist on the same line
    BOOST_LOG_AUX_UNUSED_ATTR int BOOST_LOG_UNIQUE_IDENTIFIER_NAME(var1) = 0; BOOST_LOG_AUX_UNUSED_ATTR int BOOST_LOG_UNIQUE_IDENTIFIER_NAME(var2) = 0;

    return 0;
}
