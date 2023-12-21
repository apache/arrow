/*
 *                Copyright Lingxi Li 2015.
 *             Copyright Andrey Semashev 2016.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   sink_text_ipc_mq_backend.cpp
 * \author Lingxi Li
 * \author Andrey Semashev
 * \date   19.10.2015
 *
 * \brief  The test verifies that \c text_ipc_message_queue_backend works as expected.
 */

#if !defined(BOOST_LOG_WITHOUT_IPC)

#define BOOST_TEST_MODULE sink_text_ipc_mq_backend

#include <boost/log/sinks/text_ipc_message_queue_backend.hpp>
#include <boost/log/utility/ipc/reliable_message_queue.hpp>
#include <boost/log/utility/ipc/object_name.hpp>
#include <boost/log/utility/open_mode.hpp>
#include <boost/log/core/record_view.hpp>
#include <boost/test/unit_test.hpp>
#include <string>
#include "make_record.hpp"
#include "char_definitions.hpp"

const boost::log::ipc::object_name ipc_queue_name(boost::log::ipc::object_name::session, "boost_log_test_text_ipc_mq_backend");
const unsigned int capacity = 512;
const unsigned int block_size = 1024;
const char message[] = "Hello, world!";

// The test checks that `text_ipc_message_queue_backend` works.
BOOST_AUTO_TEST_CASE(text_ipc_message_queue_backend)
{
    typedef boost::log::ipc::reliable_message_queue queue_t;
    typedef boost::log::sinks::text_ipc_message_queue_backend< queue_t > backend_t;

    // Do a remove in case if a previous test failed
    queue_t::remove(ipc_queue_name);

    backend_t backend;
    BOOST_CHECK(!backend.is_open());

    backend.message_queue().create(ipc_queue_name, capacity, block_size);
    BOOST_CHECK(backend.is_open());

    queue_t queue(boost::log::open_mode::open_only, ipc_queue_name);
    boost::log::record_view rec = make_record_view();
    backend.consume(rec, message);

    std::string msg;
    BOOST_CHECK(queue.try_receive(msg));
    BOOST_CHECK(equal_strings(msg, message));
}

#else // !defined(BOOST_LOG_WITHOUT_IPC)

int main()
{
    return 0;
}

#endif // !defined(BOOST_LOG_WITHOUT_IPC)
