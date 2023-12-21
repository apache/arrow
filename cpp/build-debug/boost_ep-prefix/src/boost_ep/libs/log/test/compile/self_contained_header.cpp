/*
 *          Copyright Andrey Semashev 2007 - 2015.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   self_contained_header.cpp
 * \author Andrey Semashev
 * \date   15.03.2014
 *
 * \brief  This file contains a test boilerplate for checking that every public header is self-contained and does not have any missing #includes.
 */

#define BOOST_LOG_TEST_INCLUDE_HEADER() <boost/log/BOOST_LOG_TEST_HEADER>

#include BOOST_LOG_TEST_INCLUDE_HEADER()

int main(int, char*[])
{
    return 0;
}
