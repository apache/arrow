/*
 *             Copyright Andrey Semashev 2020.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   util_manip_tuple.cpp
 * \author Andrey Semashev
 * \date   11.05.2020
 *
 * \brief  This header contains tests for the tuple manipulator.
 */

#define BOOST_TEST_MODULE util_manip_tuple

#include <string>
#include <sstream>
#include <utility>
#include <boost/config.hpp>
#include <boost/tuple/tuple.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/fusion/include/std_pair.hpp>
#include <boost/fusion/include/boost_tuple.hpp>
#include <boost/log/utility/manipulators/tuple.hpp>
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

BOOST_AUTO_TEST_CASE_TEMPLATE(pair_no_separator, CharT, char_types)
{
    typedef CharT char_type;
    typedef test_data< char_type > test_data_type;
    typedef std::basic_ostringstream< char_type > ostream_type;
    typedef std::basic_string< char_type > string_type;

    std::pair< int, string_type > data(1, test_data_type::abc());

    ostream_type strm_dump;
    strm_dump << logging::tuple_manip(data);

    ostream_type strm_correct;
    strm_correct << "1abc";

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(tuple_char_separator, CharT, char_types)
{
    typedef CharT char_type;
    typedef test_data< char_type > test_data_type;
    typedef std::basic_ostringstream< char_type > ostream_type;
    typedef std::basic_string< char_type > string_type;

    boost::tuple< int, my_int_wrapper, string_type > data(1, my_int_wrapper(5), string_type(test_data_type::abc()));

    ostream_type strm_dump;
    strm_dump << logging::tuple_manip(data, ' ');

    ostream_type strm_correct;
    strm_correct << "1 {5} abc";

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(tuple_string_separator, CharT, char_types)
{
    typedef CharT char_type;
    typedef test_data< char_type > test_data_type;
    typedef std::basic_ostringstream< char_type > ostream_type;
    typedef std::basic_string< char_type > string_type;

    boost::tuple< int, my_int_wrapper, string_type > data(1, my_int_wrapper(5), string_type(test_data_type::abc()));

    ostream_type strm_dump;
    strm_dump << logging::tuple_manip(data, ", ");

    ostream_type strm_correct;
    strm_correct << "1, {5}, abc";

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(tuple_std_string_separator, CharT, char_types)
{
    typedef CharT char_type;
    typedef test_data< char_type > test_data_type;
    typedef std::basic_ostringstream< char_type > ostream_type;
    typedef std::basic_string< char_type > string_type;

    boost::tuple< int, my_int_wrapper, string_type > data(1, my_int_wrapper(5), string_type(test_data_type::abc()));

    string_type separator;
    separator.push_back(',');
    separator.push_back(' ');

    ostream_type strm_dump;
    strm_dump << logging::tuple_manip(data, separator);

    ostream_type strm_correct;
    strm_correct << "1, {5}, abc";

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(tie_string_separator, CharT, char_types)
{
    typedef CharT char_type;
    typedef test_data< char_type > test_data_type;
    typedef std::basic_ostringstream< char_type > ostream_type;
    typedef std::basic_string< char_type > string_type;

    int x = 1;
    my_int_wrapper y(5);
    string_type z(test_data_type::abc());

    ostream_type strm_dump;
    strm_dump << logging::tuple_manip(boost::tie(x, y, z), ", ");

    ostream_type strm_correct;
    strm_correct << "1, {5}, abc";

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
}
