/*
 *             Copyright Andrey Semashev 2022.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   util_manip_invoke.cpp
 * \author Andrey Semashev
 * \date   27.02.2022
 *
 * \brief  This header contains tests for the invoke manipulator.
 */

#define BOOST_TEST_MODULE util_manip_invoke

#include <string>
#include <sstream>
#include <boost/config.hpp>
#include <boost/core/ref.hpp>
#include <boost/bind/bind.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/log/utility/manipulators/invoke.hpp>
#include "char_definitions.hpp"

namespace logging = boost::log;

struct my_function0
{
    typedef void result_type;

    template< typename StreamT >
    result_type operator() (StreamT& stream) const
    {
        stream << "my_function0";
    }
};

struct my_function1
{
    typedef void result_type;

    template< typename StreamT >
    result_type operator() (StreamT& stream, int n) const
    {
        stream << "my_function1(" << n << ")";
    }
};

template< typename StreamT >
void free_function(StreamT& stream, int n)
{
    stream << "free_function(" << n << ")";
}

BOOST_AUTO_TEST_CASE_TEMPLATE(invoke_my_function0, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;

    ostream_type strm_dump;
    strm_dump << logging::invoke_manip(my_function0());

    ostream_type strm_correct;
    my_function0()(strm_correct);

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(invoke_boost_bind, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;

    {
        ostream_type strm_dump;
        strm_dump << logging::invoke_manip(boost::bind(my_function1(), boost::placeholders::_1, 10));

        ostream_type strm_correct;
        boost::bind(my_function1(), boost::placeholders::_1, 10)(strm_correct);

        BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
    }
    {
        ostream_type strm_dump;
        strm_dump << logging::invoke_manip(boost::bind(&free_function< ostream_type >, boost::placeholders::_1, 10));

        ostream_type strm_correct;
        boost::bind(&free_function< ostream_type >, boost::placeholders::_1, 10)(strm_correct);

        BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
    }
}

#if !defined(BOOST_NO_CXX11_LAMBDAS)

BOOST_AUTO_TEST_CASE_TEMPLATE(invoke_lambda, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;

    ostream_type strm_dump;
    strm_dump << logging::invoke_manip([](ostream_type& strm) { strm << "lambda"; });

    ostream_type strm_correct;
    strm_correct << "lambda";

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
}

#endif // !defined(BOOST_NO_CXX11_LAMBDAS)

// This list of config macros matches the similar list in boost/log/utility/manipulators/invoke.hpp
#if !defined(BOOST_NO_CXX11_VARIADIC_TEMPLATES) && \
    !defined(BOOST_NO_CXX14_GENERIC_LAMBDAS) && \
    !defined(BOOST_NO_CXX14_RETURN_TYPE_DEDUCTION)

BOOST_AUTO_TEST_CASE_TEMPLATE(invoke_lambda_args, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;

    ostream_type strm_dump;
    strm_dump << logging::invoke_manip([](ostream_type& strm, int x, int y) { strm << "lambda(" << x << ", " << y << ")"; }, 10, 20);

    ostream_type strm_correct;
    strm_correct << "lambda(" << 10 << ", " << 20 << ")";

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
}

BOOST_AUTO_TEST_CASE_TEMPLATE(invoke_lambda_args_ref, CharT, char_types)
{
    typedef CharT char_type;
    typedef std::basic_ostringstream< char_type > ostream_type;

    int external_var = 30;
    ostream_type strm_dump;
    strm_dump << logging::invoke_manip([](ostream_type& strm, int x, int y, int& z)
    {
        strm << "lambda(" << x << ", " << y << ", " << z << ")";
        ++z;
    }, 10, 20, boost::ref(external_var));

    ostream_type strm_correct;
    strm_correct << "lambda(" << 10 << ", " << 20 << ", " << 30 << ")";

    BOOST_CHECK(equal_strings(strm_dump.str(), strm_correct.str()));
    BOOST_CHECK_EQUAL(external_var, 31);
}

#endif // !defined(BOOST_NO_CXX11_VARIADIC_TEMPLATES) ...
