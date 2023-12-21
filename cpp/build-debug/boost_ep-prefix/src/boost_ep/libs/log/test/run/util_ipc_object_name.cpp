/*
 *            Copyright Andrey Semashev 2016.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   util_ipc_object_name.cpp
 * \author Andrey Semashev
 * \date   07.03.2016
 *
 * \brief  The test verifies that \c ipc::object_name works.
 */

#if !defined(BOOST_LOG_WITHOUT_IPC)

#define BOOST_TEST_MODULE util_ipc_object_name

#include <boost/log/utility/ipc/object_name.hpp>
#include <boost/test/unit_test.hpp>
#include <string>
#include <iostream>
#include <boost/move/utility_core.hpp>
#include "char_definitions.hpp"

const char test_object_name1[] = "boost_log_test_object_name1";
const char test_object_name2[] = "boost_log_test_object_name2";

BOOST_AUTO_TEST_CASE(basic_functionality)
{
    // Default constructor.
    {
        boost::log::ipc::object_name name;
        BOOST_CHECK(name.empty());
        BOOST_CHECK(equal_strings(name.c_str(), ""));
    }

    // Initializing constructor
    {
        boost::log::ipc::object_name name(boost::log::ipc::object_name::global, test_object_name1);
        BOOST_CHECK(!name.empty());
    }

    // Copy constructor
    {
        boost::log::ipc::object_name name1(boost::log::ipc::object_name::global, test_object_name1);
        boost::log::ipc::object_name name2 = name1;
        BOOST_CHECK_EQUAL(name1, name2);
    }

    // Move constructor
    {
        boost::log::ipc::object_name name1(boost::log::ipc::object_name::global, test_object_name1);
        std::string name_str = name1.c_str();
        boost::log::ipc::object_name name2 = boost::move(name1);
        BOOST_CHECK(equal_strings(name2.c_str(), name_str.c_str()));
    }

    // Copy assignment
    {
        boost::log::ipc::object_name name1(boost::log::ipc::object_name::global, test_object_name1);
        boost::log::ipc::object_name name2;
        name2 = name1;
        BOOST_CHECK_EQUAL(name1, name2);
    }

    // Move assignment
    {
        boost::log::ipc::object_name name1(boost::log::ipc::object_name::global, test_object_name1);
        std::string name_str = name1.c_str();
        boost::log::ipc::object_name name2;
        name2 = boost::move(name1);
        BOOST_CHECK(equal_strings(name2.c_str(), name_str.c_str()));
    }

    // Output
    {
        std::cout << boost::log::ipc::object_name(boost::log::ipc::object_name::global, test_object_name1) << std::endl;
        std::cout << boost::log::ipc::object_name(boost::log::ipc::object_name::user, test_object_name1) << std::endl;
        std::cout << boost::log::ipc::object_name(boost::log::ipc::object_name::session, test_object_name1) << std::endl;
        std::cout << boost::log::ipc::object_name(boost::log::ipc::object_name::process_group, test_object_name1) << std::endl;
    }
}

BOOST_AUTO_TEST_CASE(from_native)
{
    boost::log::ipc::object_name name = boost::log::ipc::object_name::from_native(test_object_name1);
    BOOST_CHECK(equal_strings(name.c_str(), test_object_name1));
}

BOOST_AUTO_TEST_CASE(name_equivalence)
{
    // Test that the same names are equal
    {
        boost::log::ipc::object_name name1(boost::log::ipc::object_name::global, test_object_name1);
        boost::log::ipc::object_name name2(boost::log::ipc::object_name::global, test_object_name1);
        BOOST_CHECK_EQUAL(name1, name2);
    }
    {
        boost::log::ipc::object_name name1(boost::log::ipc::object_name::user, test_object_name1);
        boost::log::ipc::object_name name2(boost::log::ipc::object_name::user, test_object_name1);
        BOOST_CHECK_EQUAL(name1, name2);
    }
    {
        boost::log::ipc::object_name name1(boost::log::ipc::object_name::session, test_object_name1);
        boost::log::ipc::object_name name2(boost::log::ipc::object_name::session, test_object_name1);
        BOOST_CHECK_EQUAL(name1, name2);
    }
    {
        boost::log::ipc::object_name name1(boost::log::ipc::object_name::process_group, test_object_name1);
        boost::log::ipc::object_name name2(boost::log::ipc::object_name::process_group, test_object_name1);
        BOOST_CHECK_EQUAL(name1, name2);
    }

    // Test that different names don't clash
    {
        boost::log::ipc::object_name name1(boost::log::ipc::object_name::global, test_object_name1);
        boost::log::ipc::object_name name2(boost::log::ipc::object_name::global, test_object_name2);
        BOOST_CHECK_NE(name1, name2);
    }
    {
        boost::log::ipc::object_name name1(boost::log::ipc::object_name::user, test_object_name1);
        boost::log::ipc::object_name name2(boost::log::ipc::object_name::user, test_object_name2);
        BOOST_CHECK_NE(name1, name2);
    }
    {
        boost::log::ipc::object_name name1(boost::log::ipc::object_name::session, test_object_name1);
        boost::log::ipc::object_name name2(boost::log::ipc::object_name::session, test_object_name2);
        BOOST_CHECK_NE(name1, name2);
    }
    {
        boost::log::ipc::object_name name1(boost::log::ipc::object_name::process_group, test_object_name1);
        boost::log::ipc::object_name name2(boost::log::ipc::object_name::process_group, test_object_name2);
        BOOST_CHECK_NE(name1, name2);
    }

    // Test that same named in different scopes don't clash
    {
        boost::log::ipc::object_name name1(boost::log::ipc::object_name::global, test_object_name1);
        boost::log::ipc::object_name name2(boost::log::ipc::object_name::user, test_object_name1);
        BOOST_CHECK_NE(name1, name2);
    }
    {
        boost::log::ipc::object_name name1(boost::log::ipc::object_name::global, test_object_name1);
        boost::log::ipc::object_name name2(boost::log::ipc::object_name::session, test_object_name1);
        BOOST_CHECK_NE(name1, name2);
    }
    {
        boost::log::ipc::object_name name1(boost::log::ipc::object_name::global, test_object_name1);
        boost::log::ipc::object_name name2(boost::log::ipc::object_name::process_group, test_object_name1);
        BOOST_CHECK_NE(name1, name2);
    }
    {
        boost::log::ipc::object_name name1(boost::log::ipc::object_name::user, test_object_name1);
        boost::log::ipc::object_name name2(boost::log::ipc::object_name::session, test_object_name1);
        BOOST_CHECK_NE(name1, name2);
    }
    {
        boost::log::ipc::object_name name1(boost::log::ipc::object_name::user, test_object_name1);
        boost::log::ipc::object_name name2(boost::log::ipc::object_name::process_group, test_object_name1);
        BOOST_CHECK_NE(name1, name2);
    }
    {
        boost::log::ipc::object_name name1(boost::log::ipc::object_name::session, test_object_name1);
        boost::log::ipc::object_name name2(boost::log::ipc::object_name::process_group, test_object_name1);
        BOOST_CHECK_NE(name1, name2);
    }
}

#else // !defined(BOOST_LOG_WITHOUT_IPC)

int main()
{
    return 0;
}

#endif // !defined(BOOST_LOG_WITHOUT_IPC)
