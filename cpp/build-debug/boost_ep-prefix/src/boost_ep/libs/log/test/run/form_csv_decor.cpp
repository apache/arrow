/*
 *          Copyright Andrey Semashev 2007 - 2015.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   form_csv_decor.cpp
 * \author Andrey Semashev
 * \date   26.09.2015
 *
 * \brief  This header contains tests for the \c csv_decor formatter.
 */

#define BOOST_TEST_MODULE form_csv_decor

#include <string>
#include <boost/test/unit_test.hpp>
#include <boost/log/attributes/constant.hpp>
#include <boost/log/attributes/attribute_set.hpp>
#include <boost/log/utility/type_dispatch/standard_types.hpp>
#include <boost/log/utility/formatting_ostream.hpp>
#include <boost/log/expressions/attr.hpp>
#include <boost/log/expressions/formatter.hpp>
#include <boost/log/expressions/formatters/csv_decorator.hpp>
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
    static const char* escaped_chars() { return " !\"\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"; }
};
#endif

#ifdef BOOST_LOG_USE_WCHAR_T
template< >
struct test_strings< wchar_t > : public test_data< wchar_t >
{
    static const wchar_t* printable_chars() { return L" !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"; }
    static const wchar_t* escaped_chars() { return L" !\"\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"; }
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
        formatter f = expr::stream << expr::make_csv_decor< CharT >()[ expr::stream << expr::attr< string >(data::attr1()) ];
        f(rec, strm1);
        strm2 << data::escaped_chars();
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }
}
