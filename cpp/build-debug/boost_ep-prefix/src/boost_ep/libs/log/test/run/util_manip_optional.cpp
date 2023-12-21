/*
 *             Copyright Andrey Semashev 2020.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   util_manip_optional.cpp
 * \author Andrey Semashev
 * \date   12.05.2020
 *
 * \brief  This header contains tests for the optional manipulator.
 */

#define BOOST_TEST_MODULE util_manip_optional

#include <string>
#include <sstream>
#include <boost/config.hpp>
#include <boost/optional.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/log/utility/manipulators/optional.hpp>
#include "char_definitions.hpp"

namespace logging = boost::log;

struct my_int_wrapper
{
    int n;

    my_int_wrapper(int x = 0) BOOST_NOEXCEPT : n(x) {}
};

template< typename CharT, typename TraitsT >
inline std::basic_ostream< CharT, TraitsT >& operator<< (std::basic_ostream< CharT, TraitsT >& strm, my_int_wrapper miw)
{
    if (BOOST_LIKELY(strm.good()))
        strm << '{' << miw.n << '}';

    return strm;
}

BOOST_AUTO_TEST_CASE_TEMPLATE(opt_none_no_marker, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;
    typedef std::basic_string< char_type > string_type;

    boost::optional< int > data;

    ostream_type strm_dump;
    strm_dump << logging::optional_manip(data);

    BOOST_CHECK(equal_strings(strm_dump.str(), string_type()));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(opt_none_with_marker_char, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;

    boost::optional< int > data;

    ostream_type strm_dump;
    strm_dump << logging::optional_manip(data, '-');

    ostream_type strm_correct;
    strm_correct << "-";

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(opt_none_with_marker_string, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;

    boost::optional< int > data;

    ostream_type strm_dump;
    strm_dump << logging::optional_manip(data, "[none]");

    ostream_type strm_correct;
    strm_correct << "[none]";

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(opt_none_with_marker_std_string, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;
    typedef std::basic_string< char_type > string_type;

    boost::optional< int > data;

    string_type marker;
    marker.push_back('[');
    marker.push_back('n');
    marker.push_back('o');
    marker.push_back('n');
    marker.push_back('e');
    marker.push_back(']');

    ostream_type strm_dump;
    strm_dump << logging::optional_manip(data, marker);

    ostream_type strm_correct;
    strm_correct << "[none]";

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(ptr_none_with_marker_string, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;

    const int* data = 0;

    ostream_type strm_dump;
    strm_dump << logging::optional_manip(data, "[none]");

    ostream_type strm_correct;
    strm_correct << "[none]";

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(opt_int_with_marker_string, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;

    boost::optional< int > data;
    data = 7;

    ostream_type strm_dump;
    strm_dump << logging::optional_manip(data, "[none]");

    ostream_type strm_correct;
    strm_correct << "7";

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(ptr_int_with_marker_string, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;

    int n = 7;
    int* data = &n;

    ostream_type strm_dump;
    strm_dump << logging::optional_manip(data, "[none]");

    ostream_type strm_correct;
    strm_correct << "7";

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(ptr_miw_with_marker_string, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;

    my_int_wrapper n(7);
    const my_int_wrapper* data = &n;

    ostream_type strm_dump;
    strm_dump << logging::optional_manip(data, "[none]");

    ostream_type strm_correct;
    strm_correct << "{7}";

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
}
