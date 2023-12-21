/*
 *          Copyright Andrey Semashev 2007 - 2015.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   filt_matches_spirit_classic.cpp
 * \author Andrey Semashev
 * \date   30.03.2014
 *
 * \brief  This header contains tests for the \c matches filter with Boost.Spirit.Classic backend.
 */

#define BOOST_TEST_MODULE filt_matches_spirit_classic

#include <boost/log/detail/config.hpp>

#if !defined(BOOST_LOG_NO_THREADS)
#define BOOST_SPIRIT_THREADSAFE 1
#endif

#include <string>
#include <boost/spirit/include/classic_core.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/log/attributes/constant.hpp>
#include <boost/log/attributes/attribute_set.hpp>
#include <boost/log/attributes/attribute_value_set.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/support/spirit_classic.hpp>
#include <boost/log/utility/string_literal.hpp>
#include "char_definitions.hpp"

namespace logging = boost::log;
namespace attrs = logging::attributes;
namespace expr = logging::expressions;

namespace spirit = boost::spirit::classic;

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

    filter f = expr::matches< std::string >(data::attr1(), spirit::uint_p >> '.' >> spirit::uint_p >> '.' >> spirit::uint_p >> '.' >> spirit::uint_p);
    BOOST_CHECK(f(values1));

    f = expr::matches< std::string >(data::attr1(), *spirit::lower_p);
    BOOST_CHECK(!f(values1));

    f = expr::matches< logging::string_literal >(data::attr2(), *spirit::upper_p >> spirit::space_p >> *spirit::lower_p >> spirit::space_p >> *spirit::alpha_p);
    BOOST_CHECK(f(values1));

    f = expr::matches< std::string >(data::attr3(), spirit::str_p("Hello, world!"));
    BOOST_CHECK(f(values1));

    // Attribute value not present
    f = expr::matches< std::string >(data::attr4(), *spirit::anychar_p);
    BOOST_CHECK(!f(values1));

    // Attribute value type does not match
    f = expr::matches< std::string >(data::attr2(), *spirit::upper_p >> spirit::space_p >> *spirit::lower_p >> spirit::space_p >> *spirit::alpha_p);
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

    filter f = expr::matches< std::string >(data::attr1(), +spirit::digit_p >> '.' >> +spirit::digit_p >> '.' >> +spirit::digit_p >> '.' >> +spirit::digit_p)
            || expr::matches< logging::string_literal >(data::attr2(), *spirit::upper_p >> spirit::space_p >> *spirit::lower_p >> spirit::space_p >> *spirit::alpha_p);
    BOOST_CHECK(!f(values1));
    BOOST_CHECK(f(values2));
    BOOST_CHECK(f(values3));

    f = expr::matches< std::string >(data::attr1(), +spirit::digit_p >> '.' >> +spirit::digit_p >> '.' >> +spirit::digit_p >> '.' >> +spirit::digit_p)
            && expr::matches< logging::string_literal >(data::attr2(), *spirit::upper_p >> spirit::space_p >> *spirit::lower_p >> spirit::space_p >>*spirit::alpha_p);
    BOOST_CHECK(!f(values1));
    BOOST_CHECK(!f(values2));
    BOOST_CHECK(f(values3));
}
