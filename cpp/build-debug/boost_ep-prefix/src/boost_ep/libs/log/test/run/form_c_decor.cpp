/*
 *          Copyright Andrey Semashev 2007 - 2015.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   form_c_decor.cpp
 * \author Andrey Semashev
 * \date   26.09.2015
 *
 * \brief  This header contains tests for the \c c_decor and \c c_ascii_decor formatter.
 */

#define BOOST_TEST_MODULE form_c_decor

#include <string>
#include <boost/test/unit_test.hpp>
#include <boost/log/attributes/constant.hpp>
#include <boost/log/attributes/attribute_set.hpp>
#include <boost/log/utility/type_dispatch/standard_types.hpp>
#include <boost/log/utility/formatting_ostream.hpp>
#include <boost/log/expressions/attr.hpp>
#include <boost/log/expressions/formatter.hpp>
#include <boost/log/expressions/formatters/c_decorator.hpp>
#include <boost/log/expressions/formatters/stream.hpp>
#include <boost/log/core/record.hpp>
#include "char_definitions.hpp"
#include "make_record.hpp"

namespace logging = boost::log;
namespace attrs = logging::attributes;
namespace expr = logging::expressions;

namespace {

template< typename >
struct test_strings;

#ifdef BOOST_LOG_USE_CHAR
template< >
struct test_strings< char > : public test_data< char >
{
    static const char* chars() { return "\a\b\f\n\r\t\v !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"; }
    static const char* escaped_chars() { return "\\a\\b\\f\\n\\r\\t\\v !\\\"#$%&\\'()*+,-./0123456789:;<=>\\?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"; }

    static const char* special_chars() { return "\a\b\f\n\r\t\v !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\x7f\x80\x81\x82"; }
    static const char* escaped_special_chars() { return "\\a\\b\\f\\n\\r\\t\\v !\\\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\\x7f\\x80\\x81\\x82"; }
};
#endif

#ifdef BOOST_LOG_USE_WCHAR_T
template< >
struct test_strings< wchar_t > : public test_data< wchar_t >
{
    static const wchar_t* chars() { return L"\a\b\f\n\r\t\v !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"; }
    static const wchar_t* escaped_chars() { return L"\\a\\b\\f\\n\\r\\t\\v !\\\"#$%&\\'()*+,-./0123456789:;<=>\\?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"; }

    static const wchar_t* special_chars() { return L"\a\b\f\n\r\t\v !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\x7f\x80\x81\x82"; }
    static const wchar_t* escaped_special_chars()
    {
        if (sizeof(wchar_t) == 1)
            return L"\\a\\b\\f\\n\\r\\t\\v !\\\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\\x7f\\x80\\x81\\x82";
        if (sizeof(wchar_t) == 2)
            return L"\\a\\b\\f\\n\\r\\t\\v !\\\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\\x007f\\x0080\\x0081\\x0082";
        if (sizeof(wchar_t) == 4)
            return L"\\a\\b\\f\\n\\r\\t\\v !\\\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\\x0000007f\\x00000080\\x00000081\\x00000082";

        return L"\\a\\b\\f\\n\\r\\t\\v !\\\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\\x000000000000007f\\x0000000000000080\\x0000000000000081\\x0000000000000082";
    }
};
#endif

} // namespace

BOOST_AUTO_TEST_CASE_TEMPLATE(c_decorator_formatting, CharT, char_types)
{
    typedef logging::record_view record_view;
    typedef logging::attribute_set attr_set;
    typedef std::basic_string< CharT > string;
    typedef logging::basic_formatting_ostream< CharT > osstream;
    typedef logging::basic_formatter< CharT > formatter;
    typedef test_strings< CharT > data;

    attrs::constant< string > attr1(data::chars());

    attr_set set1;
    set1[data::attr1()] = attr1;

    record_view rec = make_record_view(set1);

    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        formatter f = expr::stream << expr::make_c_decor< CharT >()[ expr::stream << expr::attr< string >(data::attr1()) ];
        f(rec, strm1);
        strm2 << data::escaped_chars();
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }
}

BOOST_AUTO_TEST_CASE_TEMPLATE(c_ascii_decorator_formatting, CharT, char_types)
{
    typedef logging::record_view record_view;
    typedef logging::attribute_set attr_set;
    typedef std::basic_string< CharT > string;
    typedef logging::basic_formatting_ostream< CharT > osstream;
    typedef logging::basic_formatter< CharT > formatter;
    typedef test_strings< CharT > data;

    attrs::constant< string > attr1(data::chars());

    attr_set set1;
    set1[data::attr1()] = attr1;

    record_view rec = make_record_view(set1);

    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        formatter f = expr::stream << expr::make_c_ascii_decor< CharT >()[ expr::stream << expr::attr< string >(data::attr1()) ];
        f(rec, strm1);
        strm2 << data::escaped_chars();
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }
}
