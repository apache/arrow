/*
 *          Copyright Andrey Semashev 2007 - 2015.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   filt_matches_spirit_qi.cpp
 * \author Andrey Semashev
 * \date   30.03.2014
 *
 * \brief  This header contains tests for the \c matches filter with Boost.Spirit.Qi backend.
 */

#define BOOST_TEST_MODULE filt_matches_spirit_qi

#include <string>
#include <boost/spirit/include/qi_core.hpp>
#include <boost/spirit/include/qi_rule.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/log/attributes/constant.hpp>
#include <boost/log/attributes/attribute_set.hpp>
#include <boost/log/attributes/attribute_value_set.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/support/spirit_qi.hpp>
#include <boost/log/utility/string_literal.hpp>
#include "char_definitions.hpp"

namespace logging = boost::log;
namespace attrs = logging::attributes;
namespace expr = logging::expressions;

namespace spirit = boost::spirit::qi;

// The test checks that string matching works
BOOST_AUTO_TEST_CASE(matching_check)
{
    typedef logging::attribute_set attr_set;
    typedef logging::attribute_value_set attr_values;
    typedef logging::filter filter;
    typedef test_data< char > data;

    attrs::constant< std::string > attr1("127.0.0.1");
    attrs::constant< logging::string_literal > attr2(logging::str_literal("BIG brown FoX"));
    attrs::constant< std::string > attr3("Hello, world!");

    attr_set set1, set2, set3;
    set1[data::attr1()] = attr1;
    set1[data::attr2()] = attr2;
    set1[data::attr3()] = attr3;

    attr_values values1(set1, set2, set3);
    values1.freeze();

    filter f = expr::matches< std::string >(data::attr1(), spirit::uint_ >> '.' >> spirit::uint_ >> '.' >> spirit::uint_ >> '.' >> spirit::uint_);
    BOOST_CHECK(f(values1));

    // Make sure rules also work
    f = expr::matches< std::string >(data::attr1(), spirit::rule< std::string::const_iterator, void() >(spirit::uint_ >> '.' >> spirit::uint_ >> '.' >> spirit::uint_ >> '.' >> spirit::uint_));
    BOOST_CHECK(f(values1));

    f = expr::matches< std::string >(data::attr1(), *spirit::lower);
    BOOST_CHECK(!f(values1));

    f = expr::matches< logging::string_literal >(data::attr2(), *spirit::upper >> spirit::space >> *spirit::lower >> spirit::space >> *spirit::alpha);
    BOOST_CHECK(f(values1));

    f = expr::matches< std::string >(data::attr3(), spirit::lit("Hello, world!"));
    BOOST_CHECK(f(values1));

    // Attribute value not present
    f = expr::matches< std::string >(data::attr4(), *spirit::char_);
    BOOST_CHECK(!f(values1));

    // Attribute value type does not match
    f = expr::matches< std::string >(data::attr2(), *spirit::upper >> spirit::space >> *spirit::lower >> spirit::space >> *spirit::alpha);
    BOOST_CHECK(!f(values1));
}

// The test checks that the filter composition works
BOOST_AUTO_TEST_CASE(composition_check)
{
    typedef logging::attribute_set attr_set;
    typedef logging::attribute_value_set attr_values;
    typedef logging::filter filter;
    typedef test_data< char > data;

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

    filter f = expr::matches< std::string >(data::attr1(), +spirit::digit >> '.' >> +spirit::digit >> '.' >> +spirit::digit >> '.' >> +spirit::digit)
            || expr::matches< logging::string_literal >(data::attr2(), *spirit::upper >> spirit::space >> *spirit::lower >> spirit::space >> *spirit::alpha);
    BOOST_CHECK(!f(values1));
    BOOST_CHECK(f(values2));
    BOOST_CHECK(f(values3));

    f = expr::matches< std::string >(data::attr1(), +spirit::digit >> '.' >> +spirit::digit >> '.' >> +spirit::digit >> '.' >> +spirit::digit)
            && expr::matches< logging::string_literal >(data::attr2(), *spirit::upper >> spirit::space >> *spirit::lower >> spirit::space >> *spirit::alpha);
    BOOST_CHECK(!f(values1));
    BOOST_CHECK(!f(values2));
    BOOST_CHECK(f(values3));
}
