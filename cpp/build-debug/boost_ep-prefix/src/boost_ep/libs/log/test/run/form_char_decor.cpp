/*
 *          Copyright Andrey Semashev 2007 - 2015.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   form_char_decor.cpp
 * \author Andrey Semashev
 * \date   26.09.2015
 *
 * \brief  This header contains tests for the \c char_decor formatter.
 */

#define BOOST_TEST_MODULE form_char_decor

#include <string>
#include <vector>
#include <utility>
#include <boost/test/unit_test.hpp>
#include <boost/log/attributes/constant.hpp>
#include <boost/log/attributes/attribute_set.hpp>
#include <boost/log/utility/type_dispatch/standard_types.hpp>
#include <boost/log/utility/formatting_ostream.hpp>
#include <boost/log/expressions/attr.hpp>
#include <boost/log/expressions/formatter.hpp>
#include <boost/log/expressions/formatters/char_decorator.hpp>
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
    static const char* printable_chars() { return " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"; }
    static const char* escaped_chars() { return " !\"#$%&'()*+,-./0123456789:;<=>?@aBBBDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`123defghijklmnopqrstuvwxyz{|}~"; }

    static std::vector< std::string > patterns()
    {
        std::vector< std::string > v;
        v.push_back("A");
        v.push_back("B");
        v.push_back("C");
        v.push_back("abc");
        return v;
    }

    static std::vector< std::string > replacements()
    {
        std::vector< std::string > v;
        v.push_back("a");
        v.push_back("BBB");
        v.push_back("");
        v.push_back("123");
        return v;
    }

    static std::vector< std::pair< std::string, std::string > > decorations()
    {
        typedef std::pair< std::string, std::string > element;
        std::vector< element > v;
        v.push_back(element("A", "a"));
        v.push_back(element("B", "BBB"));
        v.push_back(element("C", ""));
        v.push_back(element("abc", "123"));
        return v;
    }
};
#endif

#ifdef BOOST_LOG_USE_WCHAR_T
template< >
struct test_strings< wchar_t > : public test_data< wchar_t >
{
    static const wchar_t* printable_chars() { return L" !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"; }
    static const wchar_t* escaped_chars() { return L" !\"#$%&'()*+,-./0123456789:;<=>?@aBBBDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`123defghijklmnopqrstuvwxyz{|}~"; }

    static std::vector< std::wstring > patterns()
    {
        std::vector< std::wstring > v;
        v.push_back(L"A");
        v.push_back(L"B");
        v.push_back(L"C");
        v.push_back(L"abc");
        return v;
    }

    static std::vector< std::wstring > replacements()
    {
        std::vector< std::wstring > v;
        v.push_back(L"a");
        v.push_back(L"BBB");
        v.push_back(L"");
        v.push_back(L"123");
        return v;
    }

    static std::vector< std::pair< std::wstring, std::wstring > > decorations()
    {
        typedef std::pair< std::wstring, std::wstring > element;
        std::vector< element > v;
        v.push_back(element(L"A", L"a"));
        v.push_back(element(L"B", L"BBB"));
        v.push_back(element(L"C", L""));
        v.push_back(element(L"abc", L"123"));
        return v;
    }
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

    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        formatter f = expr::stream << expr::char_decor(data::patterns(), data::replacements())[ expr::stream << expr::attr< string >(data::attr1()) ];
        f(rec, strm1);
        strm2 << data::escaped_chars();
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }
}

BOOST_AUTO_TEST_CASE_TEMPLATE(zip_decorator_formatting, CharT, char_types)
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

    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        formatter f = expr::stream << expr::char_decor(data::decorations())[ expr::stream << expr::attr< string >(data::attr1()) ];
        f(rec, strm1);
        strm2 << data::escaped_chars();
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }
}
