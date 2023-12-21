/*
 *          Copyright Andrey Semashev 2007 - 2015.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   attr_attribute_set_ticket11106.cpp
 * \author Andrey Semashev
 * \date   15.03.2015
 *
 * \brief  This header contains a test for the fix for https://svn.boost.org/trac/boost/ticket/11106.
 */

#define BOOST_TEST_MODULE attr_attribute_set_ticket11106

#include <string>
#include <sstream>
#include <utility>
#include <boost/test/unit_test.hpp>
#include <boost/log/attributes/constant.hpp>
#include <boost/log/attributes/attribute_set.hpp>

// The test checks that insertion does not invalidate existing elements in the container
BOOST_AUTO_TEST_CASE(ticket11106)
{
    boost::log::attribute_set set;

    unsigned int i = 0;
    while (i < 100)
    {
        std::ostringstream strm;
        strm << "Attr" << i;
        boost::log::attribute_name name = strm.str();

        std::pair< boost::log::attribute_set::iterator, bool > res = set.insert(name, boost::log::attributes::make_constant(5));
        ++i;

        BOOST_CHECK(res.second); // check that insertion succeeded
        BOOST_CHECK(set.find(res.first->first) != set.end()); // check that lookup works

        // Now check that all previously inserted elements are still findable
        unsigned int j = 0;
        for (boost::log::attribute_set::const_iterator it = set.begin(), end = set.end(); it != end; ++it, ++j)
        {
            boost::log::attribute_name key = it->first;
            BOOST_CHECK(set.find(key) != set.end());
        }
        BOOST_CHECK_EQUAL(j, i);
    }
}
