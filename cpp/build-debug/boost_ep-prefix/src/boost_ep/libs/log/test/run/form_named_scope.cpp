/*
 *          Copyright Andrey Semashev 2007 - 2015.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   form_named_scope.cpp
 * \author Andrey Semashev
 * \date   07.02.2009
 *
 * \brief  This header contains tests for the \c named_scope formatter.
 */

#define BOOST_TEST_MODULE form_named_scope

#include <string>
#include <boost/config.hpp>
#include <boost/preprocessor/cat.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/log/attributes/constant.hpp>
#include <boost/log/attributes/attribute_set.hpp>
#include <boost/log/attributes/named_scope.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/utility/formatting_ostream.hpp>
#include <boost/log/utility/string_literal.hpp>
#include <boost/log/core/record.hpp>
#include "char_definitions.hpp"
#include "make_record.hpp"

namespace logging = boost::log;
namespace attrs = logging::attributes;
namespace expr = logging::expressions;
namespace keywords = logging::keywords;

namespace {

    template< typename CharT >
    struct named_scope_test_data;

    struct named_scope_test_data_base
    {
        static logging::string_literal scope1() { return logging::str_literal("scope1"); }
        static logging::string_literal scope2() { return logging::str_literal("scope2"); }

        static logging::string_literal file() { return logging::str_literal(__FILE__); }
        static logging::string_literal posix_file() { return logging::str_literal("/home/user/posix_file.cpp"); }
        static logging::string_literal windows_file1() { return logging::str_literal("C:\\user\\windows_file1.cpp"); }
        static logging::string_literal windows_file2() { return logging::str_literal("C:/user/windows_file2.cpp"); }
    };

#ifdef BOOST_LOG_USE_CHAR
    template< >
    struct named_scope_test_data< char > :
        public test_data< char >,
        public named_scope_test_data_base
    {
        static logging::string_literal default_format() { return logging::str_literal("%n"); }
        static logging::string_literal full_format() { return logging::str_literal("%n (%f:%l)"); }
        static logging::string_literal short_filename_format() { return logging::str_literal("%n (%F:%l)"); }
        static logging::string_literal scope_function_name_format() { return logging::str_literal("%c"); }
        static logging::string_literal function_name_format() { return logging::str_literal("%C"); }
        static logging::string_literal delimiter1() { return logging::str_literal("|"); }
        static logging::string_literal incomplete_marker() { return logging::str_literal("<<and more>>"); }
        static logging::string_literal empty_marker() { return logging::str_literal("[empty]"); }
    };
#endif // BOOST_LOG_USE_CHAR

#ifdef BOOST_LOG_USE_WCHAR_T
    template< >
    struct named_scope_test_data< wchar_t > :
        public test_data< wchar_t >,
        public named_scope_test_data_base
    {
        static logging::wstring_literal default_format() { return logging::str_literal(L"%n"); }
        static logging::wstring_literal full_format() { return logging::str_literal(L"%n (%f:%l)"); }
        static logging::wstring_literal short_filename_format() { return logging::str_literal(L"%n (%F:%l)"); }
        static logging::wstring_literal scope_function_name_format() { return logging::str_literal(L"%c"); }
        static logging::wstring_literal function_name_format() { return logging::str_literal(L"%C"); }
        static logging::wstring_literal delimiter1() { return logging::str_literal(L"|"); }
        static logging::wstring_literal incomplete_marker() { return logging::str_literal(L"<<and more>>"); }
        static logging::wstring_literal empty_marker() { return logging::str_literal(L"[empty]"); }
    };
#endif // BOOST_LOG_USE_WCHAR_T

    template< typename CharT >
    inline bool check_formatting(logging::basic_string_literal< CharT > const& format, logging::record_view const& rec, std::basic_string< CharT > const& expected)
    {
        typedef logging::basic_formatter< CharT > formatter;
        typedef std::basic_string< CharT > string;
        typedef logging::basic_formatting_ostream< CharT > osstream;
        typedef named_scope_test_data< CharT > data;

        string str;
        osstream strm(str);
        formatter f = expr::stream << expr::format_named_scope(data::attr1(), keywords::format = format.c_str());
        f(rec, strm);
        return equal_strings(strm.str(), expected);
    }

} // namespace

// The test checks that named scopes stack formatting works
BOOST_AUTO_TEST_CASE_TEMPLATE(scopes_formatting, CharT, char_types)
{
    typedef attrs::named_scope named_scope;
    typedef named_scope::sentry sentry;

    typedef logging::attribute_set attr_set;
    typedef std::basic_string< CharT > string;
    typedef logging::basic_formatting_ostream< CharT > osstream;
    typedef logging::basic_formatter< CharT > formatter;
    typedef logging::record_view record_view;
    typedef named_scope_test_data< CharT > data;

    named_scope attr;

    // First scope
    const unsigned int line1 = __LINE__;
    sentry scope1(data::scope1(), data::file(), line1);
    const unsigned int line2 = __LINE__;
    sentry scope2(data::scope2(), data::file(), line2);

    attr_set set1;
    set1[data::attr1()] = attr;

    record_view rec = make_record_view(set1);

    // Default format
    {
        string str;
        osstream strm(str);
        strm << data::scope1() << "->" << data::scope2();
        BOOST_CHECK(check_formatting(data::default_format(), rec, strm.str()));
    }
    // Full format
    {
        string str;
        osstream strm(str);
        strm << data::scope1() << " (" << data::file() << ":" << line1 << ")->"
             << data::scope2() << " (" << data::file() << ":" << line2 << ")";
        BOOST_CHECK(check_formatting(data::full_format(), rec, strm.str()));
    }
    // Different delimiter
    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        formatter f = expr::stream << expr::format_named_scope(data::attr1(),
            keywords::format = data::default_format().c_str(),
            keywords::delimiter = data::delimiter1().c_str());
        f(rec, strm1);
        strm2 << data::scope1() << "|" << data::scope2();
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }
    // Different direction
    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        formatter f = expr::stream << expr::format_named_scope(data::attr1(),
            keywords::format = data::default_format().c_str(),
            keywords::iteration = expr::reverse);
        f(rec, strm1);
        strm2 << data::scope2() << "<-" << data::scope1();
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }
    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        formatter f = expr::stream << expr::format_named_scope(data::attr1(),
            keywords::format = data::default_format().c_str(),
            keywords::delimiter = data::delimiter1().c_str(),
            keywords::iteration = expr::reverse);
        f(rec, strm1);
        strm2 << data::scope2() << "|" << data::scope1();
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }
    // Limiting the number of scopes
    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        formatter f = expr::stream << expr::format_named_scope(data::attr1(),
            keywords::format = data::default_format().c_str(),
            keywords::depth = 1);
        f(rec, strm1);
        strm2 << "..." << data::scope2();
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }
    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        formatter f = expr::stream << expr::format_named_scope(data::attr1(),
            keywords::format = data::default_format().c_str(),
            keywords::depth = 1,
            keywords::iteration = expr::reverse);
        f(rec, strm1);
        strm2 << data::scope2() << "...";
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }
    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        formatter f = expr::stream << expr::format_named_scope(data::attr1(),
            keywords::format = data::default_format().c_str(),
            keywords::delimiter = data::delimiter1().c_str(),
            keywords::depth = 1);
        f(rec, strm1);
        strm2 << "..." << data::scope2();
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }
    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        formatter f = expr::stream << expr::format_named_scope(data::attr1(),
            keywords::format = data::default_format().c_str(),
            keywords::delimiter = data::delimiter1().c_str(),
            keywords::depth = 1,
            keywords::iteration = expr::reverse);
        f(rec, strm1);
        strm2 << data::scope2() << "...";
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }
    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        formatter f = expr::stream << expr::format_named_scope(data::attr1(),
            keywords::format = data::default_format().c_str(),
            keywords::incomplete_marker = data::incomplete_marker().c_str(),
            keywords::depth = 1);
        f(rec, strm1);
        strm2 << "<<and more>>" << data::scope2();
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }
    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        formatter f = expr::stream << expr::format_named_scope(data::attr1(),
            keywords::format = data::default_format().c_str(),
            keywords::incomplete_marker = data::incomplete_marker().c_str(),
            keywords::depth = 1,
            keywords::iteration = expr::reverse);
        f(rec, strm1);
        strm2 << data::scope2() << "<<and more>>";
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }
}

// The test checks that empty named scopes stack formatting works
BOOST_AUTO_TEST_CASE_TEMPLATE(empty_scopes_formatting, CharT, char_types)
{
    typedef attrs::named_scope named_scope;
    typedef named_scope::sentry sentry;

    typedef logging::attribute_set attr_set;
    typedef std::basic_string< CharT > string;
    typedef logging::basic_formatting_ostream< CharT > osstream;
    typedef logging::basic_formatter< CharT > formatter;
    typedef logging::record_view record_view;
    typedef named_scope_test_data< CharT > data;

    named_scope attr;

    attr_set set1;
    set1[data::attr1()] = attr;

    record_view rec = make_record_view(set1);

    formatter f = expr::stream << expr::format_named_scope(data::attr1(),
        keywords::format = data::default_format().c_str(),
        keywords::empty_marker = data::empty_marker().c_str());

    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        f(rec, strm1);
        strm2 << "[empty]";
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }

    const unsigned int line1 = __LINE__;
    sentry scope1(data::scope1(), data::file(), line1);
    const unsigned int line2 = __LINE__;
    sentry scope2(data::scope2(), data::file(), line2);

    {
        string str1, str2;
        osstream strm1(str1), strm2(str2);
        f(rec, strm1);
        strm2 << data::scope1() << "->" << data::scope2();
        BOOST_CHECK(equal_strings(strm1.str(), strm2.str()));
    }
}

BOOST_AUTO_TEST_CASE_TEMPLATE(scopes_filename_formatting_posix, CharT, char_types)
{
    typedef attrs::named_scope named_scope;
    typedef named_scope::sentry sentry;

    typedef logging::attribute_set attr_set;
    typedef std::basic_string< CharT > string;
    typedef logging::basic_formatting_ostream< CharT > osstream;
    typedef logging::record_view record_view;
    typedef named_scope_test_data< CharT > data;

    named_scope attr;

    // First scope
    const unsigned int line1 = __LINE__;
    sentry scope1(data::scope1(), data::posix_file(), line1);

    attr_set set1;
    set1[data::attr1()] = attr;

    record_view rec = make_record_view(set1);

    // File names without the full path
    {
        string str;
        osstream strm(str);
        strm << data::scope1() << " (posix_file.cpp:" << line1 << ")";
        BOOST_CHECK(check_formatting(data::short_filename_format(), rec, strm.str()));
    }
}

#if defined(BOOST_WINDOWS)

BOOST_AUTO_TEST_CASE_TEMPLATE(scopes_filename_formatting_windows, CharT, char_types)
{
    typedef attrs::named_scope named_scope;
    typedef named_scope::sentry sentry;

    typedef logging::attribute_set attr_set;
    typedef std::basic_string< CharT > string;
    typedef logging::basic_formatting_ostream< CharT > osstream;
    typedef logging::record_view record_view;
    typedef named_scope_test_data< CharT > data;

    named_scope attr;

    // First scope
    const unsigned int line1 = __LINE__;
    sentry scope1(data::scope1(), data::windows_file1(), line1);
    const unsigned int line2 = __LINE__;
    sentry scope2(data::scope2(), data::windows_file2(), line2);

    attr_set set1;
    set1[data::attr1()] = attr;

    record_view rec = make_record_view(set1);

    // File names without the full path
    {
        string str;
        osstream strm(str);
        strm << data::scope1() << " (windows_file1.cpp:" << line1 << ")->"
             << data::scope2() << " (windows_file2.cpp:" << line2 << ")";
        BOOST_CHECK(check_formatting(data::short_filename_format(), rec, strm.str()));
    }
}

#endif // defined(BOOST_WINDOWS)

namespace {

struct named_scope_test_case
{
    logging::string_literal scope_name;
    const char* function_name;
    const char* function_name_no_scope;
};

const named_scope_test_case named_scope_test_cases[] =
{
    // Generic signatures
    { logging::str_literal("int main(int, char *[])"), "main", "main" },
    { logging::str_literal("namespace_name::type foo()"), "foo", "foo" },
    { logging::str_literal("namespace_name::type& foo::bar(int[], std::string const&)"), "foo::bar", "bar" },
    { logging::str_literal("void* namespc::foo<char>::bar()"), "namespc::foo<char>::bar", "bar" },
    { logging::str_literal("void* namespc::foo<char>::bar<int>(int) const"), "namespc::foo<char>::bar<int>", "bar<int>" },

    // MSVC-specific
    { logging::str_literal("int __cdecl main(int, char *[])"), "main", "main" },
    { logging::str_literal("struct namespc::strooct __cdecl foo3(int [])"), "foo3", "foo3" },
    { logging::str_literal("void (__cdecl *__cdecl foo4(void))(void)"), "foo4", "foo4" }, // function returning pointer to function
    { logging::str_literal("void (__cdecl *__cdecl foo5(void (__cdecl *)(void)))(void)"), "foo5", "foo5" },
    { logging::str_literal("void (__cdecl *__cdecl namespc::my_class<int>::member1(void (__cdecl *)(void)))(void)"), "namespc::my_class<int>::member1", "member1" },
    { logging::str_literal("void (__cdecl *__cdecl namespc::my_class<int>::member2<int>(int))(void)"), "namespc::my_class<int>::member2<int>", "member2<int>" },
    { logging::str_literal("void (__cdecl *__cdecl namespc::my_class<int>::member2<void(__cdecl *)(void)>(void (__cdecl *)(void)))(void)"), "namespc::my_class<int>::member2<void(__cdecl *)(void)>", "member2<void(__cdecl *)(void)>" },
    { logging::str_literal("void (__cdecl *__cdecl namespc::my_class<int>::member3<void __cdecl foo1(void)>(void))(void)"), "namespc::my_class<int>::member3<void __cdecl foo1(void)>", "member3<void __cdecl foo1(void)>" },
    { logging::str_literal("void (__cdecl *__cdecl namespc::my_class<void (__cdecl*)(void)>::member1(void (__cdecl *)(void)))(void)"), "namespc::my_class<void (__cdecl*)(void)>::member1", "member1" },
    { logging::str_literal("void (__cdecl *__cdecl namespc::my_class<void (__cdecl*)(void)>::member2<int>(int))(void)"), "namespc::my_class<void (__cdecl*)(void)>::member2<int>", "member2<int>" },
    { logging::str_literal("void (__cdecl *__cdecl namespc::my_class<void (__cdecl*)(void)>::member2<void(__cdecl *)(void)>(void (__cdecl *)(void)))(void)"), "namespc::my_class<void (__cdecl*)(void)>::member2<void(__cdecl *)(void)>", "member2<void(__cdecl *)(void)>" },
    { logging::str_literal("void (__cdecl *__cdecl namespc::my_class<void (__cdecl*)(void)>::member3<void __cdecl foo1(void)>(void))(void)"), "namespc::my_class<void (__cdecl*)(void)>::member3<void __cdecl foo1(void)>", "member3<void __cdecl foo1(void)>" },
    { logging::str_literal("void (__cdecl namespc::my_class2::* __cdecl namespc::foo6(void (__cdecl *)(void)))(void)"), "namespc::foo6", "foo6" },
    { logging::str_literal("struct namespc::my_class<void __cdecl(int)> __cdecl namespc::foo7(void)"), "namespc::foo7", "foo7" },
    { logging::str_literal("void (__cdecl namespc::my_class2::* const (&__cdecl namespc::foo8(void (__cdecl *)(void)))[2])(void)"), "namespc::foo8", "foo8" },
    { logging::str_literal("__cdecl namespc::my_class2::my_class2(void)"), "namespc::my_class2::my_class2", "my_class2" },
    { logging::str_literal("__cdecl namespc::my_class2::~my_class2(void)"), "namespc::my_class2::~my_class2", "~my_class2" },
    { logging::str_literal("void __cdecl namespc::my_class2::operator =(const struct namespc::my_class2 &)"), "namespc::my_class2::operator =", "operator =" },
    { logging::str_literal("void __cdecl namespc::my_class2::operator *(void) const"), "namespc::my_class2::operator *", "operator *" },
    { logging::str_literal("void __cdecl namespc::my_class2::operator ()(void)"), "namespc::my_class2::operator ()", "operator ()" },
    { logging::str_literal("bool __cdecl namespc::my_class2::operator <(int) const"), "namespc::my_class2::operator <", "operator <" },
    { logging::str_literal("bool __cdecl namespc::my_class2::operator >(int) const"), "namespc::my_class2::operator >", "operator >" },
    { logging::str_literal("bool __cdecl namespc::my_class2::operator <=(int) const"), "namespc::my_class2::operator <=", "operator <=" },
    { logging::str_literal("bool __cdecl namespc::my_class2::operator >=(int) const"), "namespc::my_class2::operator >=", "operator >=" },
    { logging::str_literal("__cdecl namespc::my_class2::operator bool(void) const"), "namespc::my_class2::operator bool", "operator bool" },
    // MSVC generates incorrect strings in case of conversion operators to function types. We don't support these.
//    { logging::str_literal("__cdecl namespc::my_class2::operator char (__cdecl *)(double)(__cdecl *(void) const)(double)"), "namespc::my_class2::operator char (__cdecl *)(double)", "operator char (__cdecl *)(double)" },
//    { logging::str_literal("__cdecl namespc::my_class2::operator char (__cdecl namespc::my_class2::* )(double)(__cdecl namespc::my_class2::* (void) const)(double)"), "namespc::my_class2::operator char (__cdecl namespc::my_class2::* )(double)", "operator char (__cdecl namespc::my_class2::* )(double)" },
    { logging::str_literal("class std::basic_ostream<char,struct std::char_traits<char> > &__cdecl namespc::operator <<<char,struct std::char_traits<char>>(class std::basic_ostream<char,struct std::char_traits<char> > &,const struct namespc::my_class2 &)"), "namespc::operator <<<char,struct std::char_traits<char>>", "operator <<<char,struct std::char_traits<char>>" },
    { logging::str_literal("class std::basic_istream<char,struct std::char_traits<char> > &__cdecl namespc::operator >><char,struct std::char_traits<char>>(class std::basic_istream<char,struct std::char_traits<char> > &,struct namespc::my_class2 &)"), "namespc::operator >><char,struct std::char_traits<char>>", "operator >><char,struct std::char_traits<char>>" },

    // GCC-specific
    { logging::str_literal("namespc::strooct foo3(int*)"), "foo3", "foo3" },
    { logging::str_literal("void (* foo4())()"), "foo4", "foo4" }, // function returning pointer to function
    { logging::str_literal("void (* foo5(pfun2_t))()"), "foo5", "foo5" },
    { logging::str_literal("static void (* namespc::my_class<T>::member1(pfun2_t))() [with T = int; pfun1_t = void (*)(); pfun2_t = void (*)()]"), "namespc::my_class<T>::member1", "member1" },
    { logging::str_literal("static void (* namespc::my_class<T>::member2(U))() [with U = int; T = int; pfun2_t = void (*)()]"), "namespc::my_class<T>::member2", "member2" },
    { logging::str_literal("static void (* namespc::my_class<T>::member2(U))() [with U = void (*)(); T = int; pfun2_t = void (*)()]"), "namespc::my_class<T>::member2", "member2" },
    { logging::str_literal("static void (* namespc::my_class<T>::member3())() [with void (* Fun)() = foo1; T = int; pfun2_t = void (*)()]"), "namespc::my_class<T>::member3", "member3" },
    { logging::str_literal("static void (* namespc::my_class<T>::member1(pfun2_t))() [with T = void (*)(); pfun1_t = void (*)(); pfun2_t = void (*)()]"), "namespc::my_class<T>::member1", "member1" },
    { logging::str_literal("static void (* namespc::my_class<T>::member2(U))() [with U = int; T = void (*)(); pfun2_t = void (*)()]"), "namespc::my_class<T>::member2", "member2" },
    { logging::str_literal("static void (* namespc::my_class<T>::member2(U))() [with U = void (*)(); T = void (*)(); pfun2_t = void (*)()]"), "namespc::my_class<T>::member2", "member2" },
    { logging::str_literal("static void (* namespc::my_class<T>::member3())() [with void (* Fun)() = foo1; T = void (*)(); pfun2_t = void (*)()]"), "namespc::my_class<T>::member3", "member3" },
    { logging::str_literal("void (namespc::my_class2::* namespc::foo6(pfun2_t))()"), "namespc::foo6", "foo6" },
    { logging::str_literal("namespc::my_class<void(int)> namespc::foo7()"), "namespc::foo7", "foo7" },
    { logging::str_literal("void (namespc::my_class2::* const (& namespc::foo8(pfun2_t))[2])()"), "namespc::foo8", "foo8" },
    { logging::str_literal("namespc::my_class2::my_class2()"), "namespc::my_class2::my_class2", "my_class2" }, // constructor
    { logging::str_literal("namespc::my_class2::~my_class2()"), "namespc::my_class2::~my_class2", "~my_class2" }, // destructor
    { logging::str_literal("void namespc::my_class2::operator=(const namespc::my_class2&)"), "namespc::my_class2::operator=", "operator=" },
    { logging::str_literal("void namespc::my_class2::operator*() const"), "namespc::my_class2::operator*", "operator*" },
    { logging::str_literal("void namespc::my_class2::operator()()"), "namespc::my_class2::operator()", "operator()" },
    { logging::str_literal("bool namespc::my_class2::operator<(int) const"), "namespc::my_class2::operator<", "operator<" },
    { logging::str_literal("bool namespc::my_class2::operator>(int) const"), "namespc::my_class2::operator>", "operator>" },
    { logging::str_literal("bool namespc::my_class2::operator<=(int) const"), "namespc::my_class2::operator<=", "operator<=" },
    { logging::str_literal("bool namespc::my_class2::operator>=(int) const"), "namespc::my_class2::operator>=", "operator>=" },
    { logging::str_literal("namespc::my_class2::operator bool() const"), "namespc::my_class2::operator bool", "operator bool" },
    { logging::str_literal("namespc::my_class2::operator pfun1_t() const"), "namespc::my_class2::operator pfun1_t", "operator pfun1_t" },
    { logging::str_literal("std::basic_ostream<_CharT, _Traits>& namespc::operator<<(std::basic_ostream<_CharT, _Traits>&, const namespc::my_class2&) [with CharT = char; TraitsT = std::char_traits<char>]"), "namespc::operator<<", "operator<<" },
    { logging::str_literal("std::basic_istream<_CharT, _Traits>& namespc::operator>>(std::basic_istream<_CharT, _Traits>&, namespc::my_class2&) [with CharT = char; TraitsT = std::char_traits<char>]"), "namespc::operator>>", "operator>>" },

    // BOOST_CURRENT_FUNCTION fallback value
    { logging::str_literal("(unknown)"), "(unknown)", "(unknown)" }
};

} // namespace

// Function name formatting
BOOST_AUTO_TEST_CASE_TEMPLATE(scopes_scope_function_name_formatting, CharT, char_types)
{
    typedef attrs::named_scope named_scope;
    typedef named_scope::sentry sentry;

    typedef logging::attribute_set attr_set;
    typedef std::basic_string< CharT > string;
    typedef logging::basic_formatting_ostream< CharT > osstream;
    typedef logging::record_view record_view;
    typedef named_scope_test_data< CharT > data;

    named_scope attr;

    // First scope
    const unsigned int line1 = __LINE__;

    attr_set set1;
    set1[data::attr1()] = attr;

    record_view rec = make_record_view(set1);

    for (unsigned int i = 0; i < sizeof(named_scope_test_cases) / sizeof(*named_scope_test_cases); ++i)
    {
        sentry scope1(named_scope_test_cases[i].scope_name, data::file(), line1, attrs::named_scope_entry::function);
        string str;
        osstream strm(str);
        strm << named_scope_test_cases[i].function_name;
        BOOST_CHECK_MESSAGE(check_formatting(data::scope_function_name_format(), rec, strm.str()), "Scope name: " << named_scope_test_cases[i].scope_name);
    }
}

// Function name without scope formatting
BOOST_AUTO_TEST_CASE_TEMPLATE(scopes_function_name_formatting, CharT, char_types)
{
    typedef attrs::named_scope named_scope;
    typedef named_scope::sentry sentry;

    typedef logging::attribute_set attr_set;
    typedef std::basic_string< CharT > string;
    typedef logging::basic_formatting_ostream< CharT > osstream;
    typedef logging::record_view record_view;
    typedef named_scope_test_data< CharT > data;

    named_scope attr;

    // First scope
    const unsigned int line1 = __LINE__;

    attr_set set1;
    set1[data::attr1()] = attr;

    record_view rec = make_record_view(set1);

    for (unsigned int i = 0; i < sizeof(named_scope_test_cases) / sizeof(*named_scope_test_cases); ++i)
    {
        sentry scope1(named_scope_test_cases[i].scope_name, data::file(), line1, attrs::named_scope_entry::function);
        string str;
        osstream strm(str);
        strm << named_scope_test_cases[i].function_name_no_scope;
        BOOST_CHECK_MESSAGE(check_formatting(data::function_name_format(), rec, strm.str()), "Scope name: " << named_scope_test_cases[i].scope_name);
    }
}

// The test checks that function name formatters do not affect scopes denoted with BOOST_LOG_NAMED_SCOPE
BOOST_AUTO_TEST_CASE_TEMPLATE(function_name_does_not_affect_non_function_scopes, CharT, char_types)
{
    typedef attrs::named_scope named_scope;

    typedef logging::attribute_set attr_set;
    typedef std::basic_string< CharT > string;
    typedef logging::basic_formatting_ostream< CharT > osstream;
    typedef logging::record_view record_view;
    typedef named_scope_test_data< CharT > data;

    named_scope attr;

    attr_set set1;
    set1[data::attr1()] = attr;

    record_view rec = make_record_view(set1);

    {
        BOOST_LOG_NAMED_SCOPE("void foo()");
        string str;
        osstream strm(str);
        strm << "void foo()";
        BOOST_CHECK(check_formatting(data::scope_function_name_format(), rec, strm.str()));
        BOOST_CHECK(check_formatting(data::function_name_format(), rec, strm.str()));
    }
}
