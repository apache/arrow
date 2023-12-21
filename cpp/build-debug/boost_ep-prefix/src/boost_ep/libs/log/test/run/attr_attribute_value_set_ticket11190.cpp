/*
 *          Copyright Andrey Semashev 2007 - 2015.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   attr_attribute_value_set_ticket11190.cpp
 * \author Andrey Semashev
 * \date   25.04.2015
 *
 * \brief  This header contains a test for the fix for https://svn.boost.org/trac/boost/ticket/11190.
 */

#define BOOST_TEST_MODULE attr_attribute_value_set_ticket11190

#include <string>
#include <sstream>
#include <utility>
#include <boost/test/unit_test.hpp>
#include <boost/log/attributes/constant.hpp>
#include <boost/log/attributes/attribute_set.hpp>
#include <boost/log/attributes/attribute_value_set.hpp>

// The test checks that insertion does not invalidate existing elements in the container
BOOST_AUTO_TEST_CASE(ticket11190)
{
    boost::log::attribute_set set, dummy;

    unsigned int i = 0;
    while (i < 100)
    {
        std::ostringstream strm;
        strm << "Attr" << i;
        boost::log::attribute_name name = strm.str();

        std::pair< boost::log::attribute_set::iterator, bool > res = set.insert(name, boost::log::attributes::make_constant(5));
        ++i;

        BOOST_CHECK(res.second); // check that insertion succeeded
        // check that lookup works
        boost::log::attribute_set::iterator find_result = set.find(name);
        BOOST_CHECK(find_result != set.end());
        BOOST_CHECK(find_result == res.first);
    }

    boost::log::attribute_value_set vset(set, dummy, dummy);
    BOOST_CHECK_EQUAL(vset.size(), set.size());

    // Check that all inserted elements are findable
    unsigned int j = 0;
    for (boost::log::attribute_value_set::const_iterator it = vset.begin(), end = vset.end(); it != end; ++it, ++j)
    {
        boost::log::attribute_name key = it->first;
        BOOST_CHECK(vset.find(key) != vset.end());
    }

    // Check that vset.size() is valid
    BOOST_CHECK_EQUAL(j, i);
}
