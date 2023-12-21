/*
 *             Copyright Andrey Semashev 2016.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   form_max_size_decor.cpp
 * \author Andrey Semashev
 * \date   09.08.2016
 *
 * \brief  This header contains tests for the \c max_size_decor formatter.
 */

#define BOOST_TEST_MODULE form_max_size_decor

#include <string>
#include <locale>
#include <boost/test/unit_test.hpp>
#include <boost/log/attributes/constant.hpp>
#include <boost/log/attributes/attribute_set.hpp>
#include <boost/log/utility/type_dispatch/standard_types.hpp>
#include <boost/log/utility/formatting_ostream.hpp>
#include <boost/log/expressions/attr.hpp>
#include <boost/log/expressions/formatter.hpp>
#include <boost/log/expressions/formatters/max_size_decorator.hpp>
#include <boost/log/expressions/formatters/stream.hpp>
#include <boost/log/core/record.hpp>
#include <boost/phoenix/operator.hpp>
#include "char_definitions.hpp"
#include "make_record.hpp"

#define BOOST_UTF8_DECL
#define BOOST_UTF8_BEGIN_NAMESPACE namespace {
#define BOOST_UTF8_END_NAMESPACE }

#include <boost/detail/utf8_codecvt_facet.hpp>
#include <boost/detail/utf8_codecvt_facet.ipp>

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
    static const char* printable_chars() { return " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"; }
    static const char* overflow_marker() { return ">>>"; }
};
#endif

#ifdef BOOST_LOG_USE_WCHAR_T
template< >
struct test_strings< wchar_t > : public test_data< wchar_t >
{
    static const wchar_t* printable_chars() { return L" !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"; }
    static const wchar_t* overflow_marker() { return L">>>"; }
};
#endif

} // namespace

BOOST_AUTO_TEST_CASE_TEMPLATE(decorator_formatting, CharT, char_types)
{
    typedef logging::record_view record_view;
    typedef logging::attribute_set attr_set;
    typedef std::basic_string< CharT > string;
    typedef logging::basic_formatting_ostream< CharT > osstream;
    typedef logging::basic_formatter< CharT > formatter;
    typedef test_strings< CharT > data;

    attrs::constant< string > attr1(data::printable_chars());

    attr_set set1;
    set1[data::attr1()] = attr1;

    record_view rec = make_record_view(set1);

    // Test output truncation
    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        formatter f = expr::stream << expr::max_size_decor< CharT >(10)[ expr::stream << expr::attr< string >(data::attr1()) << data::some_test_string() << 1234 << data::abc() ];
        f(rec, strm1);
        strm2 << string(data::printable_chars(), 10);
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }

    // Test output truncation with a marker
    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        formatter f = expr::stream << expr::max_size_decor(10, data::overflow_marker())[ expr::stream << expr::attr< string >(data::attr1()) << data::some_test_string() << 1234 << data::abc() ];
        f(rec, strm1);
        strm2 << string(data::printable_chars(), 7) << data::overflow_marker();
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }

    // Test nested decorators, when the outer decorator enforces the limit that includes the inner decorator
    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        formatter f = expr::stream << expr::max_size_decor(35, data::overflow_marker())[
            expr::stream << data::abcdefg0123456789() << expr::max_size_decor(10, data::overflow_marker())[ expr::stream << expr::attr< string >(data::attr1()) ] << data::abcdefg0123456789()
        ];
        f(rec, strm1);
        strm2 << data::abcdefg0123456789() << string(data::printable_chars(), 7) << data::overflow_marker() << string(data::abcdefg0123456789(), 5) << data::overflow_marker();
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }

    // Test nested decorators, when the outer decorator enforces the limit that also limits the inner decorator
    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        formatter f = expr::stream << expr::max_size_decor(25, data::overflow_marker())[
            expr::stream << data::abcdefg0123456789() << expr::max_size_decor(10, data::overflow_marker())[ expr::stream << expr::attr< string >(data::attr1()) ] << data::abcdefg0123456789()
        ];
        f(rec, strm1);
        strm2 << data::abcdefg0123456789() << string(data::printable_chars(), 5) << data::overflow_marker();
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }
}

namespace {

const char narrow_chars[] = "\xd0\x9f\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82, \xd0\xbc\xd0\xb8\xd1\x80!";

} // namespace

BOOST_AUTO_TEST_CASE(character_boundary_maintenance)
{
    typedef logging::record_view record_view;
    typedef logging::attribute_set attr_set;
    typedef std::basic_string< char > string;
    typedef logging::basic_formatting_ostream< char > osstream;
    typedef logging::basic_formatter< char > formatter;
    typedef test_strings< char > data;

    std::locale loc(std::locale::classic(), new utf8_codecvt_facet());

    attrs::constant< string > attr1(narrow_chars);

    attr_set set1;
    set1[data::attr1()] = attr1;

    record_view rec = make_record_view(set1);

    // Test that output truncation does not happen in the middle of a multibyte character
    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        strm1.imbue(loc);
        strm2.imbue(loc);
        formatter f = expr::stream << expr::max_size_decor< char >(7)[ expr::stream << expr::attr< string >(data::attr1()) ];
        f(rec, strm1);
        strm2 << string(narrow_chars, 6);
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }

    // Test output truncation with a marker, when the marker length would have caused truncation in the middle of a multibyte character
    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        strm1.imbue(loc);
        strm2.imbue(loc);
        formatter f = expr::stream << expr::max_size_decor(6, data::overflow_marker())[ expr::stream << expr::attr< string >(data::attr1()) ];
        f(rec, strm1);
        strm2 << string(narrow_chars, 2) << data::overflow_marker();
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }
}
