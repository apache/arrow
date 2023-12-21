/*
 *          Copyright Andrey Semashev 2007 - 2015.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   util_formatting_ostream.cpp
 * \author Andrey Semashev
 * \date   26.05.2013
 *
 * \brief  This header contains tests for the formatting output stream wrapper.
 */

#define BOOST_TEST_MODULE util_formatting_ostream

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
#include <boost/log/utility/formatting_ostream.hpp>
#include "char_definitions.hpp"

#if defined(BOOST_LOG_USE_CHAR) && defined(BOOST_LOG_USE_WCHAR_T)

#define BOOST_UTF8_DECL
#define BOOST_UTF8_BEGIN_NAMESPACE namespace {
#define BOOST_UTF8_END_NAMESPACE }

#include <boost/detail/utf8_codecvt_facet.hpp>
#include <boost/detail/utf8_codecvt_facet.ipp>

#endif // defined(BOOST_LOG_USE_CHAR) && defined(BOOST_LOG_USE_WCHAR_T)

namespace logging = boost::log;

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
    typedef logging::basic_formatting_ostream< char_type > formatting_ostream_type;

    template< typename StringT >
    static void width_formatting()
    {
        // Check that widening works
        {
            string_type str_fmt;
            formatting_ostream_type strm_fmt(str_fmt);
            strm_fmt << strings::abc() << std::setw(8) << (StringT)strings::abcd() << strings::ABC();
            strm_fmt.flush();

            ostream_type strm_correct;
            strm_correct << strings::abc() << std::setw(8) << (StringT)strings::abcd() << strings::ABC();

            BOOST_CHECK(equal_strings(strm_fmt.str(), strm_correct.str()));
        }

        // Check that the string is not truncated
        {
            string_type str_fmt;
            formatting_ostream_type strm_fmt(str_fmt);
            strm_fmt << strings::abc() << std::setw(1) << (StringT)strings::abcd() << strings::ABC();
            strm_fmt.flush();

            ostream_type strm_correct;
            strm_correct << strings::abc() << std::setw(1) << (StringT)strings::abcd() << strings::ABC();

            BOOST_CHECK(equal_strings(strm_fmt.str(), strm_correct.str()));
        }
    }

    template< typename StringT >
    static void fill_formatting()
    {
        string_type str_fmt;
        formatting_ostream_type strm_fmt(str_fmt);
        strm_fmt << strings::abc() << std::setfill(static_cast< char_type >('x')) << std::setw(8) << (StringT)strings::abcd() << strings::ABC();
        strm_fmt.flush();

        ostream_type strm_correct;
        strm_correct << strings::abc() << std::setfill(static_cast< char_type >('x')) << std::setw(8) << (StringT)strings::abcd() << strings::ABC();

        BOOST_CHECK(equal_strings(strm_fmt.str(), strm_correct.str()));
    }

    template< typename StringT >
    static void alignment()
    {
        // Left alignment
        {
            string_type str_fmt;
            formatting_ostream_type strm_fmt(str_fmt);
            strm_fmt << strings::abc() << std::setw(8) << std::left << (StringT)strings::abcd() << strings::ABC();
            strm_fmt.flush();

            ostream_type strm_correct;
            strm_correct << strings::abc() << std::setw(8) << std::left << (StringT)strings::abcd() << strings::ABC();

            BOOST_CHECK(equal_strings(strm_fmt.str(), strm_correct.str()));
        }

        // Right alignment
        {
            string_type str_fmt;
            formatting_ostream_type strm_fmt(str_fmt);
            strm_fmt << strings::abc() << std::setw(8) << std::right << (StringT)strings::abcd() << strings::ABC();
            strm_fmt.flush();

            ostream_type strm_correct;
            strm_correct << strings::abc() << std::setw(8) << std::right << (StringT)strings::abcd() << strings::ABC();

            BOOST_CHECK(equal_strings(strm_fmt.str(), strm_correct.str()));
        }
    }

#if !defined(BOOST_NO_CXX11_RVALUE_REFERENCES)
    template< typename StringT >
    static void rvalue_stream()
    {
        string_type str_fmt;
        formatting_ostream_type(str_fmt) << strings::abc() << std::setw(8) << (StringT)strings::abcd() << strings::ABC() << std::flush;

        ostream_type strm_correct;
        strm_correct << strings::abc() << std::setw(8) << (StringT)strings::abcd() << strings::ABC();

        BOOST_CHECK(equal_strings(str_fmt, strm_correct.str()));
    }
#endif

    static void output_unreferencable_data()
    {
        unreferencable_data data;
        {
            string_type str_fmt;
            formatting_ostream_type strm_fmt(str_fmt);
            strm_fmt << data.m << static_cast< char_type >(' ') << data.n << static_cast< char_type >(' ') << unreferencable_data::x << static_cast< char_type >(' ') << unreferencable_data::y;
            strm_fmt.flush();

            ostream_type strm_correct;
            strm_correct << static_cast< unsigned int >(data.m) << static_cast< char_type >(' ') << static_cast< unsigned int >(data.n) << static_cast< char_type >(' ') << static_cast< int >(unreferencable_data::x) << static_cast< char_type >(' ') << static_cast< int >(unreferencable_data::y);

            BOOST_CHECK(equal_strings(strm_fmt.str(), strm_correct.str()));
        }
#if !defined(BOOST_NO_CXX11_RVALUE_REFERENCES)
        {
            string_type str_fmt;
            formatting_ostream_type(str_fmt) << data.m << static_cast< char_type >(' ') << data.n << static_cast< char_type >(' ') << unreferencable_data::x << static_cast< char_type >(' ') << unreferencable_data::y << std::flush;

            ostream_type strm_correct;
            strm_correct << static_cast< unsigned int >(data.m) << static_cast< char_type >(' ') << static_cast< unsigned int >(data.n) << static_cast< char_type >(' ') << static_cast< int >(unreferencable_data::x) << static_cast< char_type >(' ') << static_cast< int >(unreferencable_data::y);

            BOOST_CHECK(equal_strings(str_fmt, strm_correct.str()));
        }
#endif
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
    typedef logging::basic_formatting_ostream< char_type > formatting_ostream_type;

    string_type str_fmt;
    formatting_ostream_type strm_fmt(str_fmt);

    const my_namespace::A a = my_namespace::A(); // const lvalue
    my_namespace::B b; // lvalue
    strm_fmt << a << b << my_namespace::C(); // rvalue
    strm_fmt << my_namespace::eee;
    strm_fmt << &b << (my_namespace::B const*)&b;
    strm_fmt.flush();

    ostream_type strm_correct;
    strm_correct << a << b << my_namespace::C() << my_namespace::eee << &b << (my_namespace::B const*)&b;

    BOOST_CHECK(equal_strings(strm_fmt.str(), strm_correct.str()));
}

namespace my_namespace2 {

class A {};
template< typename CharT, typename TraitsT, typename AllocatorT >
inline logging::basic_formatting_ostream< CharT, TraitsT, AllocatorT >& operator<< (logging::basic_formatting_ostream< CharT, TraitsT, AllocatorT >& strm, A const&)
{
    strm << "A";
    return strm;
}

class B {};
template< typename CharT, typename TraitsT, typename AllocatorT >
inline logging::basic_formatting_ostream< CharT, TraitsT, AllocatorT >& operator<< (logging::basic_formatting_ostream< CharT, TraitsT, AllocatorT >& strm, B&)
{
    strm << "B";
    return strm;
}

class C {};
template< typename CharT, typename TraitsT, typename AllocatorT >
inline logging::basic_formatting_ostream< CharT, TraitsT, AllocatorT >& operator<< (logging::basic_formatting_ostream< CharT, TraitsT, AllocatorT >& strm, C const&)
{
    strm << "C";
    return strm;
}

class D {};
template< typename CharT, typename TraitsT, typename AllocatorT >
inline logging::basic_formatting_ostream< CharT, TraitsT, AllocatorT >& operator<< (logging::basic_formatting_ostream< CharT, TraitsT, AllocatorT >& strm,
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
template< typename CharT, typename TraitsT, typename AllocatorT >
inline logging::basic_formatting_ostream< CharT, TraitsT, AllocatorT >& operator<< (logging::basic_formatting_ostream< CharT, TraitsT, AllocatorT >& strm, E)
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
    typedef logging::basic_formatting_ostream< char_type > formatting_ostream_type;

    string_type str_fmt;
    formatting_ostream_type strm_fmt(str_fmt);

    const my_namespace2::A a = my_namespace2::A(); // const lvalue
    my_namespace2::B b; // lvalue
    strm_fmt << a << b << my_namespace2::C() << my_namespace2::D(); // rvalue
    strm_fmt << my_namespace2::eee;
    strm_fmt.flush();

    ostream_type strm_correct;
    strm_correct << "ABCDE";

    BOOST_CHECK(equal_strings(strm_fmt.str(), strm_correct.str()));
}

#if defined(BOOST_LOG_USE_CHAR) && defined(BOOST_LOG_USE_WCHAR_T)

namespace {

const char narrow_chars[] = "\xd0\x9f\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82, \xd0\xbc\xd0\xb8\xd1\x80!";
const wchar_t wide_chars[] = { 0x041f, 0x0440, 0x0438, 0x0432, 0x0435, 0x0442, L',', L' ', 0x043c, 0x0438, 0x0440, L'!', 0 };

template< typename StringT >
void test_narrowing_code_conversion()
{
    std::locale loc(std::locale::classic(), new utf8_codecvt_facet());

    // Test rvalues
    {
        std::string str_fmt;
        logging::formatting_ostream strm_fmt(str_fmt);
        strm_fmt.imbue(loc);
        strm_fmt << (StringT)wide_chars;
        strm_fmt.flush();

        BOOST_CHECK(equal_strings(str_fmt, std::string(narrow_chars)));
    }
    // Test lvalues
    {
        std::string str_fmt;
        logging::formatting_ostream strm_fmt(str_fmt);
        strm_fmt.imbue(loc);
        StringT wstr = StringT(wide_chars);
        strm_fmt << wstr;
        strm_fmt.flush();

        BOOST_CHECK(equal_strings(str_fmt, std::string(narrow_chars)));
    }
    // Test const lvalues
    {
        std::string str_fmt;
        logging::formatting_ostream strm_fmt(str_fmt);
        strm_fmt.imbue(loc);
        const StringT wstr = StringT(wide_chars);
        strm_fmt << wstr;
        strm_fmt.flush();

        BOOST_CHECK(equal_strings(str_fmt, std::string(narrow_chars)));
    }
}

template< typename StringT >
void test_widening_code_conversion()
{
    std::locale loc(std::locale::classic(), new utf8_codecvt_facet());

    // Test rvalues
    {
        std::wstring str_fmt;
        logging::wformatting_ostream strm_fmt(str_fmt);
        strm_fmt.imbue(loc);
        strm_fmt << (StringT)narrow_chars;
        strm_fmt.flush();

        BOOST_CHECK(equal_strings(str_fmt, std::wstring(wide_chars)));
    }
    // Test lvalues
    {
        std::wstring str_fmt;
        logging::wformatting_ostream strm_fmt(str_fmt);
        strm_fmt.imbue(loc);
        StringT str = StringT(narrow_chars);
        strm_fmt << str;
        strm_fmt.flush();

        BOOST_CHECK(equal_strings(str_fmt, std::wstring(wide_chars)));
    }
    // Test const lvalues
    {
        std::wstring str_fmt;
        logging::wformatting_ostream strm_fmt(str_fmt);
        strm_fmt.imbue(loc);
        const StringT str = StringT(narrow_chars);
        strm_fmt << str;
        strm_fmt.flush();

        BOOST_CHECK(equal_strings(str_fmt, std::wstring(wide_chars)));
    }
}

} // namespace

// Test character code conversion
BOOST_AUTO_TEST_CASE(character_code_conversion)
{
    test_narrowing_code_conversion< const wchar_t* >();
    test_widening_code_conversion< const char* >();
    test_narrowing_code_conversion< std::wstring >();
    test_widening_code_conversion< std::string >();
    test_narrowing_code_conversion< boost::wstring_view >();
    test_widening_code_conversion< boost::string_view >();
#if !defined(BOOST_NO_CXX17_HDR_STRING_VIEW)
    test_narrowing_code_conversion< std::wstring_view >();
    test_widening_code_conversion< std::string_view >();
#endif
}

#endif
