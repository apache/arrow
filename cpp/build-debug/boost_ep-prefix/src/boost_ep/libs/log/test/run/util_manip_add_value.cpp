/*
 *          Copyright Andrey Semashev 2007 - 2015.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   util_manip_add_value.cpp
 * \author Andrey Semashev
 * \date   07.11.2013
 *
 * \brief  This header contains tests for the \c add_value manipulator.
 */

#define BOOST_TEST_MODULE util_manip_add_value

#include <iomanip>
#include <iostream>
#include <boost/move/core.hpp>
#include <boost/io/ios_state.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/log/core.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/attributes/attribute_set.hpp>
#include <boost/log/attributes/value_extraction.hpp>
#include <boost/log/expressions/keyword.hpp>
#include <boost/log/utility/manipulators/add_value.hpp>
#include "make_record.hpp"

namespace logging = boost::log;

struct my_type
{
    BOOST_COPYABLE_AND_MOVABLE(my_type)

public:
    unsigned int value;

    explicit my_type(unsigned int n = 0) : value(n) {}
    my_type(my_type const& that) : value(that.value) {}
    my_type(BOOST_RV_REF(my_type) that) : value(that.value) { that.value = 0xbaadbaad; }
    ~my_type() { value = 0xdeaddead; }

    my_type& operator= (BOOST_COPY_ASSIGN_REF(my_type) that) { value = that.value; return *this; }
    my_type& operator= (BOOST_RV_REF(my_type) that) { value = that.value; that.value = 0xbaadbaad; return *this; }
};

inline bool operator== (my_type const& left, my_type const& right)
{
    return left.value == right.value;
}

inline bool operator!= (my_type const& left, my_type const& right)
{
    return left.value != right.value;
}

template< typename CharT, typename TraitsT >
inline std::basic_ostream< CharT, TraitsT >& operator<< (std::basic_ostream< CharT, TraitsT >& strm, my_type const& val)
{
    if (strm.good())
    {
        boost::io::ios_flags_saver flags(strm);
        boost::io::basic_ios_fill_saver< CharT, TraitsT > fill(strm);
        strm << std::hex << std::internal << std::setfill(static_cast< CharT >('0')) << std::setw(10) << val.value;
    }
    return strm;
}

struct my_pod_type
{
    unsigned int value;
};

inline bool operator== (my_pod_type const& left, my_pod_type const& right)
{
    return left.value == right.value;
}

inline bool operator!= (my_pod_type const& left, my_pod_type const& right)
{
    return left.value != right.value;
}

template< typename CharT, typename TraitsT >
inline std::basic_ostream< CharT, TraitsT >& operator<< (std::basic_ostream< CharT, TraitsT >& strm, my_pod_type const& val)
{
    if (strm.good())
    {
        boost::io::ios_flags_saver flags(strm);
        boost::io::basic_ios_fill_saver< CharT, TraitsT > fill(strm);
        strm << std::hex << std::internal << std::setfill(static_cast< CharT >('0')) << std::setw(10) << val.value;
    }
    return strm;
}

BOOST_AUTO_TEST_CASE(manual_add_attr)
{
    logging::record rec = make_record(logging::attribute_set());
    BOOST_REQUIRE(!!rec);
    logging::record_ostream strm(rec);

    my_type val(0xaaaaaaaa);
    const my_type const_val(0xbbbbbbbb);
    strm << logging::add_value("MyAttr1", val) << logging::add_value("MyAttr2", const_val) << logging::add_value("MyAttr3", my_type(0xcccccccc));

    // Test for MSVC bug: if the value is a scalar type, it saves a dangling reference to the add_value_manip,
    // which results in garbage in the attribute value
    strm << logging::add_value("MyAttr4", 100u);
    strm << logging::add_value("MyAttr5", my_pod_type());

    strm.detach_from_record();

    BOOST_CHECK_EQUAL(rec["MyAttr1"].extract< my_type >(), val);
    BOOST_CHECK_EQUAL(rec["MyAttr2"].extract< my_type >(), const_val);
    BOOST_CHECK_EQUAL(rec["MyAttr3"].extract< my_type >(), my_type(0xcccccccc));
    BOOST_CHECK_EQUAL(rec["MyAttr4"].extract< unsigned int >(), 100u);
    BOOST_CHECK_EQUAL(rec["MyAttr5"].extract< my_pod_type >(), my_pod_type());
}

BOOST_LOG_ATTRIBUTE_KEYWORD(a_my1, "MyAttr1", my_type)
BOOST_LOG_ATTRIBUTE_KEYWORD(a_my2, "MyAttr2", my_type)
BOOST_LOG_ATTRIBUTE_KEYWORD(a_my3, "MyAttr3", my_type)

BOOST_AUTO_TEST_CASE(keyword_add_attr)
{
    logging::record rec = make_record(logging::attribute_set());
    BOOST_REQUIRE(!!rec);
    logging::record_ostream strm(rec);

    my_type val(0xaaaaaaaa);
    const my_type const_val(0xbbbbbbbb);
    strm << logging::add_value(a_my1, val) << logging::add_value(a_my2, const_val) << logging::add_value(a_my3, my_type(0xcccccccc));
    strm.detach_from_record();

    BOOST_CHECK_EQUAL(rec[a_my1], val);
    BOOST_CHECK_EQUAL(rec[a_my2], const_val);
    BOOST_CHECK_EQUAL(rec[a_my3], my_type(0xcccccccc));
}
