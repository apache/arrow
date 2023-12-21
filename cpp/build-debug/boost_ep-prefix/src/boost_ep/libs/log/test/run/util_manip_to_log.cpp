/*
 *          Copyright Andrey Semashev 2007 - 2015.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   util_manip_to_log.cpp
 * \author Andrey Semashev
 * \date   05.07.2015
 *
 * \brief  This header contains tests for the \c to_log stream manipulator.
 */

#define BOOST_TEST_MODULE util_manip_to_log

#include <string>
#include <sstream>
#include <algorithm>
#include <boost/test/unit_test.hpp>
#include <boost/log/utility/formatting_ostream.hpp>
#include <boost/log/utility/manipulators/to_log.hpp>
#include "char_definitions.hpp"

namespace logging = boost::log;

namespace tag {

struct a_my_class;

} // namespace tag

namespace {

struct my_class
{
    int m_Data;

    explicit my_class(int data) : m_Data(data) {}
};

template< typename CharT, typename TraitsT >
inline std::basic_ostream< CharT, TraitsT >&
operator<< (std::basic_ostream< CharT, TraitsT >& strm, my_class const& obj)
{
    strm << "my_class: [data: " << obj.m_Data << "]";
    return strm;
}

template< typename StreamT >
inline StreamT&
operator<< (StreamT& strm, logging::to_log_manip< my_class > const& obj)
{
    strm << "to_log(my_class: [data: " << obj.get().m_Data << "])";
    return strm;
}

template< typename StreamT >
inline StreamT&
operator<< (StreamT& strm, logging::to_log_manip< my_class, tag::a_my_class > const& obj)
{
    strm << "to_log<a_my_class>(my_class: [data: " << obj.get().m_Data << "])";
    return strm;
}

template< typename CharT, typename StreamT >
struct tests
{
    typedef CharT char_type;
    typedef StreamT stream_type;
    typedef std::basic_string< char_type > string;
    typedef std::basic_ostringstream< char_type > std_stream;

    //! The test verifies that the default behavior of the manipulator is equivalent to the standard operator<<
    static void default_operator()
    {
        string str;
        stream_type strm1(str);
        strm1 << logging::to_log(10);

        std_stream strm2;
        strm2 << 10;
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }

    //! The test verifies that operator overrides work
    static void operator_overrides()
    {
        {
            string str;
            stream_type strm1(str);
            strm1 << my_class(10);

            std_stream strm2;
            strm2 << "my_class: [data: " << 10 << "]";
            BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
        }
        {
            string str;
            stream_type strm1(str);
            strm1 << logging::to_log(my_class(10));

            std_stream strm2;
            strm2 << "to_log(my_class: [data: " << 10 << "])";
            BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
        }
        {
            string str;
            stream_type strm1(str);
            strm1 << logging::to_log< tag::a_my_class >(my_class(10));

            std_stream strm2;
            strm2 << "to_log<a_my_class>(my_class: [data: " << 10 << "])";
            BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
        }
    }
};

} // namespace


//! The test verifies that the default behavior of the manipulator is equivalent to the standard operator<<
BOOST_AUTO_TEST_CASE_TEMPLATE(default_operator, CharT, char_types)
{
    tests< CharT, std::basic_ostringstream< CharT > >::default_operator();
    tests< CharT, logging::basic_formatting_ostream< CharT > >::default_operator();
}

//! The test verifies that operator overrides work
BOOST_AUTO_TEST_CASE_TEMPLATE(operator_overrides, CharT, char_types)
{
    tests< CharT, std::basic_ostringstream< CharT > >::operator_overrides();
    tests< CharT, logging::basic_formatting_ostream< CharT > >::operator_overrides();
}
