/*
 *          Copyright Andrey Semashev 2007 - 2015.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   filt_matches_xpressive.cpp
 * \author Andrey Semashev
 * \date   30.03.2014
 *
 * \brief  This header contains tests for the \c matches filter with Boost.Xpressive backend.
 */

#include <boost/config.hpp>

// gcc 10 and 11 prior to 11.2 are known to ICE on Boost.Xpressive code in C++03 mode, see:
// https://gcc.gnu.org/bugzilla/show_bug.cgi?id=102293
#if !defined(BOOST_GCC) || defined(BOOST_GCC_CXX11) || BOOST_GCC < 100000 || BOOST_GCC >= 110200

#define BOOST_TEST_MODULE filt_matches_xpressive

#include <string>
#include <boost/xpressive/xpressive_dynamic.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/log/attributes/constant.hpp>
#include <boost/log/attributes/attribute_set.hpp>
#include <boost/log/attributes/attribute_value_set.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/support/xpressive.hpp>
#include <boost/log/utility/string_literal.hpp>
#include "char_definitions.hpp"

namespace logging = boost::log;
namespace attrs = logging::attributes;
namespace expr = logging::expressions;

// The test checks that string matching works
BOOST_AUTO_TEST_CASE(matching_check)
{
    typedef logging::attribute_set attr_set;
    typedef logging::attribute_value_set attr_values;
    typedef logging::filter filter;
    typedef test_data< char > data;
    typedef boost::xpressive::basic_regex< const char* > regex_type;

    attrs::constant< std::string > attr1("127.0.0.1");
    attrs::constant< logging::string_literal > attr2(logging::str_literal("BIG brown FoX"));
    attrs::constant< std::string > attr3("Hello, world!");

    attr_set set1, set2, set3;
    set1[data::attr1()] = attr1;
    set1[data::attr2()] = attr2;
    set1[data::attr3()] = attr3;

    attr_values values1(set1, set2, set3);
    values1.freeze();

    filter f = expr::matches< std::string >(data::attr1(), regex_type::compile("\\d+\\.\\d+\\.\\d+\\.\\d+"));
    BOOST_CHECK(f(values1));

    f = expr::matches< std::string >(data::attr1(), regex_type::compile("[a-z]*"));
    BOOST_CHECK(!f(values1));

    f = expr::matches< logging::string_literal >(data::attr2(), regex_type::compile("[A-Z]* [a-z]* [A-Za-z]*"));
    BOOST_CHECK(f(values1));

    f = expr::matches< std::string >(data::attr3(), regex_type::compile("Hello, world!"));
    BOOST_CHECK(f(values1));

    // Attribute value not present
    f = expr::matches< std::string >(data::attr4(), regex_type::compile(".*"));
    BOOST_CHECK(!f(values1));

    // Attribute value type does not match
    f = expr::matches< std::string >(data::attr2(), regex_type::compile("[A-Z]* [a-z]* [A-Za-z]*"));
    BOOST_CHECK(!f(values1));
}

// The test checks that the filter composition works
BOOST_AUTO_TEST_CASE(composition_check)
{
    typedef logging::attribute_set attr_set;
    typedef logging::attribute_value_set attr_values;
    typedef logging::filter filter;
    typedef test_data< char > data;
    typedef boost::xpressive::basic_regex< const char* > regex_type;

    attrs::constant< std::string > attr1("127.0.0.1");
    attrs::constant< logging::string_literal > attr2(logging::str_literal("BIG brown FoX"));
    attrs::constant< std::string > attr3("Hello, world!");

    attr_set set1, set2, set3;
    attr_values values1(set1, set2, set3);
    values1.freeze();
    set1[data::attr2()] = attr2;
    attr_values values2(set1, set2, set3);
    values2.freeze();
    set1[data::attr3()] = attr3;
    set1[data::attr1()] = attr1;
    attr_values values3(set1, set2, set3);
    values3.freeze();

    filter f = expr::matches< std::string >(data::attr1(), regex_type::compile("\\d+\\.\\d+\\.\\d+\\.\\d+")) || expr::matches< logging::string_literal >(data::attr2(), regex_type::compile("[A-Z]* [a-z]* [A-Za-z]*"));
    BOOST_CHECK(!f(values1));
    BOOST_CHECK(f(values2));
    BOOST_CHECK(f(values3));

    f = expr::matches< std::string >(data::attr1(), regex_type::compile("\\d+\\.\\d+\\.\\d+\\.\\d+")) && expr::matches< logging::string_literal >(data::attr2(), regex_type::compile("[A-Z]* [a-z]* [A-Za-z]*"));
    BOOST_CHECK(!f(values1));
    BOOST_CHECK(!f(values2));
    BOOST_CHECK(f(values3));
}

#else // !defined(BOOST_GCC) || defined(BOOST_GCC_CXX11) || BOOST_GCC < 100000 || BOOST_GCC >= 110200

int main()
{
    return 0;
}

#endif // !defined(BOOST_GCC) || defined(BOOST_GCC_CXX11) || BOOST_GCC < 100000 || BOOST_GCC >= 110200
