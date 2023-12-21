/*
 *          Copyright Andrey Semashev 2007 - 2015.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   form_to_log_manip.cpp
 * \author Andrey Semashev
 * \date   05.07.2015
 *
 * \brief  This header contains tests for support for the \c to_log_manip customization point.
 */

#define BOOST_TEST_MODULE form_to_log_manip

#include <string>
#include <ostream>
#include <algorithm>
#include <boost/test/unit_test.hpp>
#include <boost/log/attributes/constant.hpp>
#include <boost/log/attributes/attribute_set.hpp>
#include <boost/log/utility/formatting_ostream.hpp>
#include <boost/log/utility/manipulators/to_log.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/core/record.hpp>
#include "char_definitions.hpp"
#include "make_record.hpp"

namespace logging = boost::log;
namespace attrs = logging::attributes;
namespace expr = logging::expressions;

namespace {

struct my_class
{
    int m_Data;

    explicit my_class(int data) : m_Data(data) {}
};

} // namespace

BOOST_LOG_ATTRIBUTE_KEYWORD(a_my_class, "MyClass", my_class)
BOOST_LOG_ATTRIBUTE_KEYWORD(a_string, "String", std::string)

namespace {

template< typename CharT, typename TraitsT, typename AllocatorT >
inline logging::basic_formatting_ostream< CharT, TraitsT, AllocatorT >&
operator<< (logging::basic_formatting_ostream< CharT, TraitsT, AllocatorT >& strm, logging::to_log_manip< my_class, tag::a_my_class > const& obj)
{
    strm << "a_my_class: [data: " << obj.get().m_Data << "]";
    return strm;
}

} // namespace

namespace std {

template< typename CharT, typename TraitsT, typename AllocatorT >
inline logging::basic_formatting_ostream< CharT, TraitsT, AllocatorT >&
operator<< (logging::basic_formatting_ostream< CharT, TraitsT, AllocatorT >& strm, logging::to_log_manip< std::string, tag::a_string > const& obj)
{
    strm << "a_string: [" << obj.get() << "]";
    return strm;
}

} // namespace std

BOOST_AUTO_TEST_CASE_TEMPLATE(operator_overrides, CharT, char_types)
{
    typedef logging::record_view record_view;
    typedef logging::attribute_set attr_set;
    typedef std::basic_string< CharT > string;
    typedef logging::basic_formatting_ostream< CharT > osstream;
    typedef logging::basic_formatter< CharT > formatter;

    attrs::constant< my_class > attr1(my_class(77));
    attrs::constant< std::string > attr2("Hello");

    attr_set set1;
    set1[a_my_class.get_name()] = attr1;
    set1[a_string.get_name()] = attr2;

    record_view rec = make_record_view(set1);

    // Check that out custom operators are called
    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        formatter f = expr::stream << a_my_class << ", " << a_string;
        f(rec, strm1);
        strm2 << "a_my_class: [data: " << 77 << "], a_string: [" << "Hello" << "]";
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }
}
