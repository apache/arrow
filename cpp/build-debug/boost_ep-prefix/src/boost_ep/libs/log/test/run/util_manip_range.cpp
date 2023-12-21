/*
 *             Copyright Andrey Semashev 2020.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   util_manip_range.cpp
 * \author Andrey Semashev
 * \date   11.05.2020
 *
 * \brief  This header contains tests for the range manipulator.
 */

#define BOOST_TEST_MODULE util_manip_range

#include <list>
#include <vector>
#include <string>
#include <sstream>
#include <boost/config.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/log/utility/manipulators/range.hpp>
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

BOOST_AUTO_TEST_CASE_TEMPLATE(int_no_separator, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;

    std::vector< int > data;
    data.push_back(1);
    data.push_back(2);
    data.push_back(3);

    ostream_type strm_dump;
    strm_dump << logging::range_manip(data);

    ostream_type strm_correct;
    strm_correct << "123";

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(int_char_separator, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;

    std::vector< int > data;
    data.push_back(1);
    data.push_back(2);
    data.push_back(3);

    ostream_type strm_dump;
    strm_dump << logging::range_manip(data, ' ');

    ostream_type strm_correct;
    strm_correct << "1 2 3";

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(int_string_separator, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;

    std::vector< int > data;
    data.push_back(1);
    data.push_back(2);
    data.push_back(3);

    ostream_type strm_dump;
    strm_dump << logging::range_manip(data, ", ");

    ostream_type strm_correct;
    strm_correct << "1, 2, 3";

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(int_std_string_separator, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;
    typedef std::basic_string< char_type > string_type;

    std::vector< int > data;
    data.push_back(1);
    data.push_back(2);
    data.push_back(3);

    string_type separator;
    separator.push_back(',');
    separator.push_back(' ');

    ostream_type strm_dump;
    strm_dump << logging::range_manip(data, separator);

    ostream_type strm_correct;
    strm_correct << "1, 2, 3";

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(miw_string_separator, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;

    std::list< my_int_wrapper > data;
    data.push_back(my_int_wrapper(1));
    data.push_back(my_int_wrapper(2));
    data.push_back(my_int_wrapper(3));

    ostream_type strm_dump;
    strm_dump << logging::range_manip(data, ", ");

    ostream_type strm_correct;
    strm_correct << "{1}, {2}, {3}";

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(array_int_string_separator, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;

    int data[3] = { 1, 2, 3 };

    ostream_type strm_dump;
    strm_dump << logging::range_manip(data, ", ");

    ostream_type strm_correct;
    strm_correct << "1, 2, 3";

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
}
