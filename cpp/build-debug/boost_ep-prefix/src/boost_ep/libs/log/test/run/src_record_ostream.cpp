/*
 *          Copyright Andrey Semashev 2007 - 2015.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   src_record_ostream.cpp
 * \author Andrey Semashev
 * \date   23.08.2015
 *
 * \brief  This header contains tests for the log record formatting output stream.
 */

#define BOOST_TEST_MODULE src_record_ostream

#include <locale>
#include <string>
#include <iomanip>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <boost/config.hpp>
#if !defined(BOOST_NO_CXX17_HDR_STRING_VIEW)
#include <string_view>
#endif
#include <boost/test/unit_test.hpp>
#include <boost/utility/string_view.hpp>
#include <boost/log/core/record.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/expressions/message.hpp>
#include <boost/log/attributes/value_extraction.hpp>
#include "char_definitions.hpp"
#include "make_record.hpp"

namespace logging = boost::log;
namespace expr = boost::log::expressions;

namespace {

struct unreferencable_data
{
    unsigned int m : 2;
    unsigned int n : 6;

    enum my_enum
    {
        one = 1,
        two = 2
    };

    // The following static constants don't have definitions, so they can only be used in constant expressions.
    // Trying to bind a reference to these members will result in linking errors.
    static const int x = 7;
    static const my_enum y = one;

    unreferencable_data()
    {
        m = 1;
        n = 5;
    }
};

template< typename CharT >
struct test_impl
{
    typedef CharT char_type;
    typedef test_data< char_type > strings;
    typedef std::basic_string< char_type > string_type;
    typedef std::basic_ostringstream< char_type > ostream_type;
    typedef logging::basic_record_ostream< char_type > record_ostream_type;

    template< typename StringT >
    static void width_formatting()
    {
        // Check that widening works
        {
            logging::record rec = make_record();
            BOOST_REQUIRE(!!rec);
            record_ostream_type strm_fmt(rec);
            strm_fmt << strings::abc() << std::setw(8) << (StringT)strings::abcd() << strings::ABC();
            strm_fmt.flush();
            string_type rec_message = logging::extract_or_throw< string_type >(expr::message.get_name(), rec);

            ostream_type strm_correct;
            strm_correct << strings::abc() << std::setw(8) << (StringT)strings::abcd() << strings::ABC();

            BOOST_CHECK(equal_strings(rec_message, strm_correct.str()));
        }

        // Check that the string is not truncated
        {
            logging::record rec = make_record();
            BOOST_REQUIRE(!!rec);
            record_ostream_type strm_fmt(rec);
            strm_fmt << strings::abc() << std::setw(1) << (StringT)strings::abcd() << strings::ABC();
            strm_fmt.flush();
            string_type rec_message = logging::extract_or_throw< string_type >(expr::message.get_name(), rec);

            ostream_type strm_correct;
            strm_correct << strings::abc() << std::setw(1) << (StringT)strings::abcd() << strings::ABC();

            BOOST_CHECK(equal_strings(rec_message, strm_correct.str()));
        }
    }

    template< typename StringT >
    static void fill_formatting()
    {
        logging::record rec = make_record();
        BOOST_REQUIRE(!!rec);
        record_ostream_type strm_fmt(rec);
        strm_fmt << strings::abc() << std::setfill(static_cast< char_type >('x')) << std::setw(8) << (StringT)strings::abcd() << strings::ABC();
        strm_fmt.flush();
        string_type rec_message = logging::extract_or_throw< string_type >(expr::message.get_name(), rec);

        ostream_type strm_correct;
        strm_correct << strings::abc() << std::setfill(static_cast< char_type >('x')) << std::setw(8) << (StringT)strings::abcd() << strings::ABC();

        BOOST_CHECK(equal_strings(rec_message, strm_correct.str()));
    }

    template< typename StringT >
    static void alignment()
    {
        // Left alignment
        {
            logging::record rec = make_record();
            BOOST_REQUIRE(!!rec);
            record_ostream_type strm_fmt(rec);
            strm_fmt << strings::abc() << std::setw(8) << std::left << (StringT)strings::abcd() << strings::ABC();
            strm_fmt.flush();
            string_type rec_message = logging::extract_or_throw< string_type >(expr::message.get_name(), rec);

            ostream_type strm_correct;
            strm_correct << strings::abc() << std::setw(8) << std::left << (StringT)strings::abcd() << strings::ABC();

            BOOST_CHECK(equal_strings(rec_message, strm_correct.str()));
        }

        // Right alignment
        {
            logging::record rec = make_record();
            BOOST_REQUIRE(!!rec);
            record_ostream_type strm_fmt(rec);
            strm_fmt << strings::abc() << std::setw(8) << std::right << (StringT)strings::abcd() << strings::ABC();
            strm_fmt.flush();
            string_type rec_message = logging::extract_or_throw< string_type >(expr::message.get_name(), rec);

            ostream_type strm_correct;
            strm_correct << strings::abc() << std::setw(8) << std::right << (StringT)strings::abcd() << strings::ABC();

            BOOST_CHECK(equal_strings(rec_message, strm_correct.str()));
        }
    }

#if !defined(BOOST_NO_CXX11_RVALUE_REFERENCES)
    template< typename StringT >
    static void rvalue_stream()
    {
        logging::record rec = make_record();
        BOOST_REQUIRE(!!rec);
        record_ostream_type(rec) << strings::abc() << std::setw(8) << (StringT)strings::abcd() << strings::ABC() << std::flush;
        string_type rec_message = logging::extract_or_throw< string_type >(expr::message.get_name(), rec);

        ostream_type strm_correct;
        strm_correct << strings::abc() << std::setw(8) << (StringT)strings::abcd() << strings::ABC();

        BOOST_CHECK(equal_strings(rec_message, strm_correct.str()));
    }
#endif

    static void output_unreferencable_data()
    {
        unreferencable_data data;
        {
            logging::record rec = make_record();
            BOOST_REQUIRE(!!rec);
            record_ostream_type strm_fmt(rec);
            strm_fmt << data.m << static_cast< char_type >(' ') << data.n << static_cast< char_type >(' ') << unreferencable_data::x << static_cast< char_type >(' ') << unreferencable_data::y;
            strm_fmt.flush();
            string_type rec_message = logging::extract_or_throw< string_type >(expr::message.get_name(), rec);

            ostream_type strm_correct;
            strm_correct << static_cast< unsigned int >(data.m) << static_cast< char_type >(' ') << static_cast< unsigned int >(data.n) << static_cast< char_type >(' ') << static_cast< int >(unreferencable_data::x) << static_cast< char_type >(' ') << static_cast< int >(unreferencable_data::y);

            BOOST_CHECK(equal_strings(rec_message, strm_correct.str()));
        }
#if !defined(BOOST_NO_CXX11_RVALUE_REFERENCES)
        {
            logging::record rec = make_record();
            BOOST_REQUIRE(!!rec);
            record_ostream_type(rec) << data.m << static_cast< char_type >(' ') << data.n << static_cast< char_type >(' ') << unreferencable_data::x << static_cast< char_type >(' ') << unreferencable_data::y << std::flush;
            string_type rec_message = logging::extract_or_throw< string_type >(expr::message.get_name(), rec);

            ostream_type strm_correct;
            strm_correct << static_cast< unsigned int >(data.m) << static_cast< char_type >(' ') << static_cast< unsigned int >(data.n) << static_cast< char_type >(' ') << static_cast< int >(unreferencable_data::x) << static_cast< char_type >(' ') << static_cast< int >(unreferencable_data::y);

            BOOST_CHECK(equal_strings(rec_message, strm_correct.str()));
        }
#endif
    }

    static void formatting_params_restoring()
    {
        record_ostream_type strm_fmt;
        {
            logging::record rec = make_record();
            BOOST_REQUIRE(!!rec);
            strm_fmt.attach_record(rec);
            strm_fmt << std::setw(8) << std::setfill(static_cast< char_type >('x')) << std::hex << 15;
            strm_fmt.flush();
            string_type rec_message = logging::extract_or_throw< string_type >(expr::message.get_name(), rec);
            strm_fmt.detach_from_record();

            ostream_type strm_correct;
            strm_correct << std::setw(8) << std::setfill(static_cast< char_type >('x')) << std::hex << 15;

            BOOST_CHECK(equal_strings(rec_message, strm_correct.str()));
        }

        // Check that the formatting flags are reset for the next record
        {
            logging::record rec = make_record();
            BOOST_REQUIRE(!!rec);
            strm_fmt.attach_record(rec);
            strm_fmt << 15;
            strm_fmt.flush();
            string_type rec_message = logging::extract_or_throw< string_type >(expr::message.get_name(), rec);
            strm_fmt.detach_from_record();

            ostream_type strm_correct;
            strm_correct << 15;

            BOOST_CHECK(equal_strings(rec_message, strm_correct.str()));
        }
    }
};

} // namespace

// Test support for width formatting
BOOST_AUTO_TEST_CASE_TEMPLATE(width_formatting, CharT, char_types)
{
    typedef test_impl< CharT > test;
    test::BOOST_NESTED_TEMPLATE width_formatting< const CharT* >();
    test::BOOST_NESTED_TEMPLATE width_formatting< typename test::string_type >();
    test::BOOST_NESTED_TEMPLATE width_formatting< boost::basic_string_view< CharT > >();
#if !defined(BOOST_NO_CXX17_HDR_STRING_VIEW)
    test::BOOST_NESTED_TEMPLATE width_formatting< std::basic_string_view< CharT > >();
#endif
}

// Test support for filler character setup
BOOST_AUTO_TEST_CASE_TEMPLATE(fill_formatting, CharT, char_types)
{
    typedef test_impl< CharT > test;
    test::BOOST_NESTED_TEMPLATE fill_formatting< const CharT* >();
    test::BOOST_NESTED_TEMPLATE fill_formatting< typename test::string_type >();
    test::BOOST_NESTED_TEMPLATE fill_formatting< boost::basic_string_view< CharT > >();
#if !defined(BOOST_NO_CXX17_HDR_STRING_VIEW)
    test::BOOST_NESTED_TEMPLATE fill_formatting< std::basic_string_view< CharT > >();
#endif
}

// Test support for text alignment
BOOST_AUTO_TEST_CASE_TEMPLATE(alignment, CharT, char_types)
{
    typedef test_impl< CharT > test;
    test::BOOST_NESTED_TEMPLATE alignment< const CharT* >();
    test::BOOST_NESTED_TEMPLATE alignment< typename test::string_type >();
    test::BOOST_NESTED_TEMPLATE alignment< boost::basic_string_view< CharT > >();
#if !defined(BOOST_NO_CXX17_HDR_STRING_VIEW)
    test::BOOST_NESTED_TEMPLATE alignment< std::basic_string_view< CharT > >();
#endif
}

#if !defined(BOOST_NO_CXX11_RVALUE_REFERENCES)
// Test support for rvalue stream objects
BOOST_AUTO_TEST_CASE_TEMPLATE(rvalue_stream, CharT, char_types)
{
    typedef test_impl< CharT > test;
    test::BOOST_NESTED_TEMPLATE rvalue_stream< const CharT* >();
    test::BOOST_NESTED_TEMPLATE rvalue_stream< typename test::string_type >();
    test::BOOST_NESTED_TEMPLATE rvalue_stream< boost::basic_string_view< CharT > >();
#if !defined(BOOST_NO_CXX17_HDR_STRING_VIEW)
    test::BOOST_NESTED_TEMPLATE rvalue_stream< std::basic_string_view< CharT > >();
#endif
}
#endif

// Test output of data to which a reference cannot be bound
BOOST_AUTO_TEST_CASE_TEMPLATE(output_unreferencable_data, CharT, char_types)
{
    typedef test_impl< CharT > test;
    test::output_unreferencable_data();
}

// Test that formatting settings are reset for new log records
BOOST_AUTO_TEST_CASE_TEMPLATE(formatting_params_restoring, CharT, char_types)
{
    typedef test_impl< CharT > test;
    test::formatting_params_restoring();
}

namespace my_namespace {

class A {};
template< typename CharT, typename TraitsT >
inline std::basic_ostream< CharT, TraitsT >& operator<< (std::basic_ostream< CharT, TraitsT >& strm, A const&)
{
    strm << "A";
    return strm;
}

class B {};
template< typename CharT, typename TraitsT >
inline std::basic_ostream< CharT, TraitsT >& operator<< (std::basic_ostream< CharT, TraitsT >& strm, B&)
{
    strm << "B";
    return strm;
}

template< typename CharT, typename TraitsT >
inline std::basic_ostream< CharT, TraitsT >& operator<< (std::basic_ostream< CharT, TraitsT >& strm, B*)
{
    strm << "B*";
    return strm;
}

template< typename CharT, typename TraitsT >
inline std::basic_ostream< CharT, TraitsT >& operator<< (std::basic_ostream< CharT, TraitsT >& strm, const B*)
{
    strm << "const B*";
    return strm;
}

class C {};
template< typename CharT, typename TraitsT >
inline std::basic_ostream< CharT, TraitsT >& operator<< (std::basic_ostream< CharT, TraitsT >& strm, C const&)
{
    strm << "C";
    return strm;
}

enum E { eee };
template< typename CharT, typename TraitsT >
inline std::basic_ostream< CharT, TraitsT >& operator<< (std::basic_ostream< CharT, TraitsT >& strm, E)
{
    strm << "E";
    return strm;
}

} // namespace my_namespace

// Test operator forwarding
BOOST_AUTO_TEST_CASE_TEMPLATE(operator_forwarding, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_string< char_type > string_type;
    typedef std::basic_ostringstream< char_type > ostream_type;
    typedef logging::basic_record_ostream< char_type > record_ostream_type;

    logging::record rec = make_record();
    BOOST_REQUIRE(!!rec);
    record_ostream_type strm_fmt(rec);

    const my_namespace::A a = my_namespace::A(); // const lvalue
    my_namespace::B b; // lvalue
    strm_fmt << a << b << my_namespace::C(); // rvalue
    strm_fmt << my_namespace::eee;
    strm_fmt << &b << (my_namespace::B const*)&b;
    strm_fmt.flush();
    string_type rec_message = logging::extract_or_throw< string_type >(expr::message.get_name(), rec);

    ostream_type strm_correct;
    strm_correct << a << b << my_namespace::C() << my_namespace::eee << &b << (my_namespace::B const*)&b;

    BOOST_CHECK(equal_strings(rec_message, strm_correct.str()));
}

namespace my_namespace2 {

class A {};
template< typename CharT >
inline logging::basic_record_ostream< CharT >& operator<< (logging::basic_record_ostream< CharT >& strm, A const&)
{
    strm << "A";
    return strm;
}

class B {};
template< typename CharT >
inline logging::basic_record_ostream< CharT >& operator<< (logging::basic_record_ostream< CharT >& strm, B&)
{
    strm << "B";
    return strm;
}

class C {};
template< typename CharT >
inline logging::basic_record_ostream< CharT >& operator<< (logging::basic_record_ostream< CharT >& strm, C const&)
{
    strm << "C";
    return strm;
}

class D {};
template< typename CharT >
inline logging::basic_record_ostream< CharT >& operator<< (logging::basic_record_ostream< CharT >& strm,
#if !defined(BOOST_NO_CXX11_RVALUE_REFERENCES)
    D&&
#else
    D const&
#endif
    )
{
    strm << "D";
    return strm;
}

enum E { eee };
template< typename CharT >
inline logging::basic_record_ostream< CharT >& operator<< (logging::basic_record_ostream< CharT >& strm, E)
{
    strm << "E";
    return strm;
}

} // namespace my_namespace2

// Test operator overriding
BOOST_AUTO_TEST_CASE_TEMPLATE(operator_overriding, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_string< char_type > string_type;
    typedef std::basic_ostringstream< char_type > ostream_type;
    typedef logging::basic_record_ostream< char_type > record_ostream_type;

    logging::record rec = make_record();
    BOOST_REQUIRE(!!rec);
    record_ostream_type strm_fmt(rec);

    const my_namespace2::A a = my_namespace2::A(); // const lvalue
    my_namespace2::B b; // lvalue
    strm_fmt << a << b << my_namespace2::C() << my_namespace2::D(); // rvalue
    strm_fmt << my_namespace2::eee;
    strm_fmt.flush();
    string_type rec_message = logging::extract_or_throw< string_type >(expr::message.get_name(), rec);

    ostream_type strm_correct;
    strm_correct << "ABCDE";

    BOOST_CHECK(equal_strings(rec_message, strm_correct.str()));
}

// Test that operator<< returns a record_ostream
BOOST_AUTO_TEST_CASE_TEMPLATE(operator_return_type, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_string< char_type > string_type;
    typedef std::basic_ostringstream< char_type > ostream_type;
    typedef logging::basic_record_ostream< char_type > record_ostream_type;

    logging::record rec = make_record();
    BOOST_REQUIRE(!!rec);
    record_ostream_type strm_fmt(rec);

    // The test here verifies that the result of << "Hello" is a record_ostream, not std::ostream or logging::formatting_ostream.
    // The subsequent << A() will only compile if the stream is record_ostream.
    strm_fmt << "Hello " << my_namespace2::A();
    strm_fmt.flush();
    string_type rec_message = logging::extract_or_throw< string_type >(expr::message.get_name(), rec);

    ostream_type strm_correct;
    strm_correct << "Hello A";

    BOOST_CHECK(equal_strings(rec_message, strm_correct.str()));
}
