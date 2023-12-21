/*
 *             Copyright Andrey Semashev 2019.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          https://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   form_auto_newline.cpp
 * \author Andrey Semashev
 * \date   23.06.2019
 *
 * \brief  This header contains tests for the auto_newline formatter.
 */

#define BOOST_TEST_MODULE form_auto_newline

#include <string>
#include <boost/test/unit_test.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/utility/formatting_ostream.hpp>
#include "char_definitions.hpp"
#include "make_record.hpp"

namespace logging = boost::log;
namespace expr = logging::expressions;

// Test appending a newline to a non-empty string
BOOST_AUTO_TEST_CASE_TEMPLATE(append_to_non_empty_string, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;
    typedef logging::basic_formatting_ostream< char_type > formatting_ostream_type;
    typedef typename formatting_ostream_type::string_type string_type;
    typedef logging::record_view record_view;
    typedef logging::basic_formatter< char_type > formatter;

    record_view rec = make_record_view();
    string_type str_fmt;
    formatting_ostream_type strm_fmt(str_fmt);

    formatter f = expr::stream << "Hello" << expr::auto_newline;
    f(rec, strm_fmt);

    ostream_type strm_correct;
    strm_correct << "Hello\n";

    BOOST_CHECK(equal_strings(strm_fmt.str(), strm_correct.str()));
}

// Test appending a newline to an empty string
BOOST_AUTO_TEST_CASE_TEMPLATE(append_to_empty_string, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;
    typedef logging::basic_formatting_ostream< char_type > formatting_ostream_type;
    typedef typename formatting_ostream_type::string_type string_type;
    typedef logging::record_view record_view;
    typedef logging::basic_formatter< char_type > formatter;

    record_view rec = make_record_view();
    string_type str_fmt;
    formatting_ostream_type strm_fmt(str_fmt);

    formatter f = expr::stream << expr::auto_newline;
    f(rec, strm_fmt);

    ostream_type strm_correct;
    strm_correct << "\n";

    BOOST_CHECK(equal_strings(strm_fmt.str(), strm_correct.str()));
}

// Test not appending a newline to a non-empty string which already ends with a newline
BOOST_AUTO_TEST_CASE_TEMPLATE(not_append_if_ends_with_a_newline, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;
    typedef logging::basic_formatting_ostream< char_type > formatting_ostream_type;
    typedef typename formatting_ostream_type::string_type string_type;
    typedef logging::record_view record_view;
    typedef logging::basic_formatter< char_type > formatter;

    record_view rec = make_record_view();
    string_type str_fmt;
    formatting_ostream_type strm_fmt(str_fmt);

    formatter f = expr::stream << "Hello\n" << expr::auto_newline;
    f(rec, strm_fmt);

    ostream_type strm_correct;
    strm_correct << "Hello\n";

    BOOST_CHECK(equal_strings(strm_fmt.str(), strm_correct.str()));
}
