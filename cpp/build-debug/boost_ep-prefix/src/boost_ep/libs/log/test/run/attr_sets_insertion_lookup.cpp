/*
 *          Copyright Andrey Semashev 2007 - 2015.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   attr_sets_insertion_lookup.cpp
 * \author Andrey Semashev
 * \date   21.06.2014
 *
 * \brief  This header contains tests for the attribute and attribute value sets. This test performs special checks
 *         for insert() and find() methods that depend on the attribute name ids and the order in which
 *         insert() operations are invoked, see https://sourceforge.net/p/boost-log/discussion/710022/thread/e883db9a/.
 */

#define BOOST_TEST_MODULE attr_sets_insertion_lookup

#include <string>
#include <sstream>
#include <boost/config.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/log/attributes/constant.hpp>
#include <boost/log/attributes/attribute_set.hpp>
#include <boost/log/attributes/attribute_value_set.hpp>

namespace logging = boost::log;
namespace attrs = logging::attributes;

namespace {

template< typename SetT, typename ValueT >
void test_insertion_lookup(SetT& values, ValueT const& value)
{
    // Initialize attribute names. Each name will gain a consecutive id.
    logging::attribute_name names[20];
    for (unsigned int i = 0; i < sizeof(names) / sizeof(*names); ++i)
    {
        std::ostringstream strm;
        strm << "Attr" << i;
        names[i] = logging::attribute_name(strm.str());
    }

    // Insert attribute values in this exact order so that different cases in the hash table implementation are tested.
    values.insert(names[17], value);
    values.insert(names[1], value);
    values.insert(names[8], value);
    values.insert(names[9], value);
    values.insert(names[10], value);
    values.insert(names[16], value);
    values.insert(names[0], value);
    values.insert(names[11], value);
    values.insert(names[12], value);
    values.insert(names[13], value);
    values.insert(names[14], value);
    values.insert(names[15], value);
    values.insert(names[18], value);
    values.insert(names[19], value);
    values.insert(names[4], value);
    values.insert(names[5], value);
    values.insert(names[7], value);
    values.insert(names[6], value);
    values.insert(names[2], value);
    values.insert(names[3], value);

    // Check that all values are accessible through iteration and find()
    for (unsigned int i = 0; i < sizeof(names) / sizeof(*names); ++i)
    {
        BOOST_CHECK_MESSAGE(values.find(names[i]) != values.end(), "Attribute " << names[i] << " (id: " << names[i].id() << ") not found by find()");

        bool found_by_iteration = false;
        for (typename SetT::const_iterator it = values.begin(), end = values.end(); it != end; ++it)
        {
            if (it->first == names[i])
            {
                found_by_iteration = true;
                break;
            }
        }

        BOOST_CHECK_MESSAGE(found_by_iteration, "Attribute " << names[i] << " (id: " << names[i].id() << ") not found by iteration");
    }
}

} // namespace

BOOST_AUTO_TEST_CASE(attributes)
{
    logging::attribute_set values;
    attrs::constant< int > attr(10);

    test_insertion_lookup(values, attr);
}

BOOST_AUTO_TEST_CASE(attribute_values)
{
    logging::attribute_value_set values;
    attrs::constant< int > attr(10);

    test_insertion_lookup(values, attr.get_value());
}
