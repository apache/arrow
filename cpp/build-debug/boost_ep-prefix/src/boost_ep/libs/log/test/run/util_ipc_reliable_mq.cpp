/*
 *               Copyright Lingxi Li 2015.
 *            Copyright Andrey Semashev 2016.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   util_ipc_reliable_mq.cpp
 * \author Lingxi Li
 * \author Andrey Semashev
 * \date   19.10.2015
 *
 * \brief  The test verifies that \c ipc::reliable_message_queue works.
 */

#if !defined(BOOST_LOG_WITHOUT_IPC)

#define BOOST_TEST_MODULE util_ipc_reliable_mq

#include <boost/log/utility/ipc/reliable_message_queue.hpp>
#include <boost/log/utility/ipc/object_name.hpp>
#include <boost/log/utility/permissions.hpp>
#include <boost/log/utility/open_mode.hpp>
#include <boost/log/exceptions.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/config.hpp>
#if defined(BOOST_WINDOWS)
#include <boost/winapi/get_current_process_id.hpp>
#else
#include <unistd.h>
#endif
#include <cstddef>
#include <cstring>
#include <string>
#include <vector>
#include <sstream>
#include <iostream>
#include <stdexcept>
#include <boost/move/utility_core.hpp>
#if !defined(BOOST_LOG_NO_THREADS)
#include <algorithm>
#include <boost/core/ref.hpp>
#include <boost/atomic/fences.hpp>
#include <boost/thread/thread.hpp>
#include <boost/chrono/duration.hpp>
#endif
#include "char_definitions.hpp"

typedef boost::log::ipc::reliable_message_queue queue_t;
typedef queue_t::size_type size_type;

inline boost::log::ipc::object_name generate_ipc_queue_name()
{
    // Make sure IPC queue name is specific to the current process. This is useful when running
    // multiple instances of the test concurrently (e.g. debug and release).
    std::ostringstream strm;
    strm << "boost_log_test_ipc_reliable_mq"
#if defined(BOOST_WINDOWS)
        << +boost::winapi::GetCurrentProcessId();
#else
        << +getpid();
#endif
    return boost::log::ipc::object_name(boost::log::ipc::object_name::session, strm.str());
}

const boost::log::ipc::object_name ipc_queue_name = generate_ipc_queue_name();
const unsigned int capacity = 512;
const size_type block_size = 1024;
const char message1[] = "Hello, world!";
const char message2[] = "Hello, the brand new world!";

struct queue_cleanup
{
    ~queue_cleanup()
    {
        try
        {
            queue_t::remove(ipc_queue_name);
        }
        catch (...)
        {
        }
    }
};
#if !defined(BOOST_MSVC) || BOOST_MSVC >= 1800
const queue_cleanup queue_cleanup_guard = {};
#else
// MSVC prior to 12.0 ICEs on the aggregate initialization of the constant
const queue_cleanup queue_cleanup_guard;
#endif

BOOST_AUTO_TEST_CASE(basic_functionality)
{
    // Default constructor.
    {
        queue_t queue;
        BOOST_CHECK(!queue.is_open());
    }

    // Do a remove in case if a previous test crashed
    queue_t::remove(ipc_queue_name);

    // Opening a non-existing queue
    try
    {
        queue_t queue(boost::log::open_mode::open_only, ipc_queue_name);
        BOOST_FAIL("Non-existing queue open succeeded, although it shouldn't have");
    }
    catch (std::exception&)
    {
        BOOST_TEST_PASSPOINT();
    }

    // Create constructor and destructor.
    {
        queue_t queue(boost::log::open_mode::create_only, ipc_queue_name, capacity, block_size);
        BOOST_CHECK(equal_strings(queue.name().c_str(), ipc_queue_name.c_str()));
        BOOST_CHECK(queue.is_open());
        BOOST_CHECK_EQUAL(queue.capacity(), capacity);
        BOOST_CHECK_EQUAL(queue.block_size(), block_size);
    }

    // Creating a duplicate queue
    try
    {
        queue_t queue_a(boost::log::open_mode::create_only, ipc_queue_name, capacity, block_size);
        queue_t queue_b(boost::log::open_mode::create_only, ipc_queue_name, capacity, block_size);
        BOOST_FAIL("Creating a duplicate queue succeeded, although it shouldn't have");
    }
    catch (std::exception&)
    {
        BOOST_TEST_PASSPOINT();
    }

    // Opening an existing queue
    {
        queue_t queue_a(boost::log::open_mode::create_only, ipc_queue_name, capacity, block_size);
        BOOST_CHECK(queue_a.is_open());

        queue_t queue_b(boost::log::open_mode::open_or_create, ipc_queue_name, capacity * 2u, block_size * 2u); // queue geometry differs from the existing queue
        BOOST_CHECK(queue_b.is_open());
        BOOST_CHECK(equal_strings(queue_b.name().c_str(), ipc_queue_name.c_str()));
        BOOST_CHECK_EQUAL(queue_b.capacity(), capacity);
        BOOST_CHECK_EQUAL(queue_b.block_size(), block_size);

        queue_t queue_c(boost::log::open_mode::open_only, ipc_queue_name);
        BOOST_CHECK(queue_c.is_open());
        BOOST_CHECK(equal_strings(queue_c.name().c_str(), ipc_queue_name.c_str()));
        BOOST_CHECK_EQUAL(queue_c.capacity(), capacity);
        BOOST_CHECK_EQUAL(queue_c.block_size(), block_size);
    }
    // Closing a queue
    {
        queue_t queue_a(boost::log::open_mode::create_only, ipc_queue_name, capacity, block_size);
        BOOST_CHECK(queue_a.is_open());
        queue_a.close();
        BOOST_CHECK(!queue_a.is_open());
        // Duplicate close()
        queue_a.close();
        BOOST_CHECK(!queue_a.is_open());
    }
    // Move constructor.
    {
        queue_t queue_a(boost::log::open_mode::create_only, ipc_queue_name, capacity, block_size);
        queue_t queue_b(boost::move(queue_a));
        BOOST_CHECK(!queue_a.is_open());
        BOOST_CHECK(equal_strings(queue_b.name().c_str(), ipc_queue_name.c_str()));
        BOOST_CHECK(queue_b.is_open());
        BOOST_CHECK_EQUAL(queue_b.capacity(), capacity);
        BOOST_CHECK_EQUAL(queue_b.block_size(), block_size);
    }
    // Move assignment operator.
    {
        queue_t queue_a(boost::log::open_mode::create_only, ipc_queue_name, capacity, block_size);
        queue_t queue_b;
        queue_b = boost::move(queue_a);
        BOOST_CHECK(!queue_a.is_open());
        BOOST_CHECK(equal_strings(queue_b.name().c_str(), ipc_queue_name.c_str()));
        BOOST_CHECK(queue_b.is_open());
        BOOST_CHECK_EQUAL(queue_b.capacity(), capacity);
        BOOST_CHECK_EQUAL(queue_b.block_size(), block_size);
    }
    // Member and non-member swaps.
    {
        queue_t queue_a(boost::log::open_mode::create_only, ipc_queue_name, capacity, block_size);
        queue_a.swap(queue_a);
        BOOST_CHECK(queue_a.is_open());
        BOOST_CHECK(equal_strings(queue_a.name().c_str(), ipc_queue_name.c_str()));
        BOOST_CHECK_EQUAL(queue_a.capacity(), capacity);
        BOOST_CHECK_EQUAL(queue_a.block_size(), block_size);

        queue_t queue_b;
        swap(queue_a, queue_b);
        BOOST_CHECK(!queue_a.is_open());
        BOOST_CHECK(queue_b.is_open());
        BOOST_CHECK(equal_strings(queue_b.name().c_str(), ipc_queue_name.c_str()));
        BOOST_CHECK_EQUAL(queue_b.capacity(), capacity);
        BOOST_CHECK_EQUAL(queue_b.block_size(), block_size);
    }
}

BOOST_AUTO_TEST_CASE(message_passing)
{
    // try_send() and try_receive()
    {
        queue_t queue_a(boost::log::open_mode::create_only, ipc_queue_name, 1u, block_size);
        queue_t queue_b(boost::log::open_mode::open_only, ipc_queue_name);
        BOOST_CHECK(queue_a.try_send(message1, sizeof(message1) - 1u));
        BOOST_CHECK(!queue_a.try_send(message2, sizeof(message2) - 1u));
        char buffer[block_size] = {};
        size_type message_size = 0u;
        BOOST_CHECK(queue_b.try_receive(buffer, sizeof(buffer), message_size));
        BOOST_CHECK_EQUAL(message_size, sizeof(message1) - 1u);
        BOOST_CHECK(std::memcmp(buffer, message1, message_size) == 0);
        BOOST_CHECK(!queue_b.try_receive(buffer, sizeof(buffer), message_size));

        BOOST_CHECK(queue_a.try_send(message2, sizeof(message2) - 1u));
        std::string msg;
        BOOST_CHECK(queue_b.try_receive(msg));
        BOOST_CHECK_EQUAL(msg.size(), sizeof(message2) - 1u);
        BOOST_CHECK_EQUAL(msg, message2);

        BOOST_CHECK(queue_a.try_send(message2, sizeof(message2) - 1u));
        std::vector< unsigned char > buf;
        BOOST_CHECK(queue_b.try_receive(buf));
        BOOST_CHECK_EQUAL(buf.size(), sizeof(message2) - 1u);
        BOOST_CHECK(std::memcmp(&buf[0], message2, buf.size()) == 0);
    }

    // send() and receive() without blocking
    {
        queue_t queue_a(boost::log::open_mode::create_only, ipc_queue_name, 1u, block_size);
        queue_t queue_b(boost::log::open_mode::open_only, ipc_queue_name);
        BOOST_CHECK(queue_a.send(message1, sizeof(message1) - 1u) == queue_t::succeeded);
        char buffer[block_size] = {};
        size_type message_size = 0u;
        BOOST_CHECK(queue_b.receive(buffer, sizeof(buffer), message_size) == queue_t::succeeded);
        BOOST_CHECK_EQUAL(message_size, sizeof(message1) - 1u);
        BOOST_CHECK(std::memcmp(buffer, message1, message_size) == 0);

        BOOST_CHECK(queue_a.send(message2, sizeof(message2) - 1u) == queue_t::succeeded);
        std::string msg;
        BOOST_CHECK(queue_b.receive(msg) == queue_t::succeeded);
        BOOST_CHECK_EQUAL(msg.size(), sizeof(message2) - 1u);
        BOOST_CHECK_EQUAL(msg, message2);

        BOOST_CHECK(queue_a.send(message2, sizeof(message2) - 1u) == queue_t::succeeded);
        std::vector< unsigned char > buf;
        BOOST_CHECK(queue_b.receive(buf) == queue_t::succeeded);
        BOOST_CHECK_EQUAL(buf.size(), sizeof(message2) - 1u);
        BOOST_CHECK(std::memcmp(&buf[0], message2, buf.size()) == 0);
    }

    // send() with an error code on overflow
    {
        queue_t queue_a(boost::log::open_mode::create_only, ipc_queue_name, 1u, block_size, queue_t::fail_on_overflow);
        BOOST_TEST_PASSPOINT();
        BOOST_CHECK(queue_a.send(message1, sizeof(message1) - 1u) == queue_t::succeeded);
        BOOST_TEST_PASSPOINT();

        queue_t::operation_result res = queue_a.send(message1, sizeof(message1) - 1u);
        BOOST_CHECK_EQUAL(res, queue_t::no_space);
    }

    // send() with an exception on overflow
    {
        queue_t queue_a(boost::log::open_mode::create_only, ipc_queue_name, 1u, block_size, queue_t::throw_on_overflow);
        BOOST_TEST_PASSPOINT();
        BOOST_CHECK(queue_a.send(message1, sizeof(message1) - 1u) == queue_t::succeeded);
        BOOST_TEST_PASSPOINT();
        try
        {
            queue_a.send(message1, sizeof(message1) - 1u);
            BOOST_FAIL("Owerflowing the queue succeeded, although it shouldn't have");
        }
        catch (boost::log::capacity_limit_reached&)
        {
            BOOST_TEST_PASSPOINT();
        }
    }

    // send() and receive() for messages larger than block_size. The message size and queue capacity below are such
    // that the last enqueued message is expected to be split in the queue storage.
    {
        queue_t queue_a(boost::log::open_mode::create_only, ipc_queue_name, 5u, block_size);
        queue_t queue_b(boost::log::open_mode::open_only, ipc_queue_name);

        const size_type message_size = block_size * 3u / 2u;
        std::vector< unsigned char > send_data;
        send_data.resize(message_size);
        for (unsigned int i = 0; i < message_size; ++i)
            send_data[i] = static_cast< unsigned char >(i & 0xFF);

        BOOST_CHECK(queue_a.send(&send_data[0], static_cast< size_type >(send_data.size())) == queue_t::succeeded);

        for (unsigned int i = 0; i < 3; ++i)
        {
            BOOST_CHECK(queue_a.send(&send_data[0], static_cast< size_type >(send_data.size())) == queue_t::succeeded);

            std::vector< unsigned char > receive_data;
            BOOST_CHECK(queue_b.receive(receive_data) == queue_t::succeeded);
            BOOST_CHECK_EQUAL_COLLECTIONS(send_data.begin(), send_data.end(), receive_data.begin(), receive_data.end());
        }

        std::vector< unsigned char > receive_data;
        BOOST_CHECK(queue_b.receive(receive_data) == queue_t::succeeded);
        BOOST_CHECK_EQUAL_COLLECTIONS(send_data.begin(), send_data.end(), receive_data.begin(), receive_data.end());
    }

    // clear()
    {
        queue_t queue_a(boost::log::open_mode::create_only, ipc_queue_name, 1u, block_size);
        queue_t queue_b(boost::log::open_mode::open_only, ipc_queue_name);
        BOOST_CHECK(queue_a.try_send(message1, sizeof(message1) - 1u));
        BOOST_CHECK(!queue_a.try_send(message2, sizeof(message2) - 1u));

        queue_a.clear();

        BOOST_CHECK(queue_a.try_send(message2, sizeof(message2) - 1u));
        char buffer[block_size] = {};
        size_type message_size = 0u;
        BOOST_CHECK(queue_b.try_receive(buffer, sizeof(buffer), message_size));
        BOOST_CHECK_EQUAL(message_size, sizeof(message2) - 1u);
        BOOST_CHECK(std::memcmp(buffer, message2, message_size) == 0);
    }
}

#if !defined(BOOST_LOG_NO_THREADS)

namespace {

const unsigned int message_count = 100000;

void multithreaded_message_passing_feeding_thread(const char* message, unsigned int& failure_count)
{
    const size_type len = static_cast< size_type >(std::strlen(message));
    queue_t queue(boost::log::open_mode::open_or_create, ipc_queue_name, capacity, block_size);
    for (unsigned int i = 0; i < message_count; ++i)
    {
        failure_count += queue.send(message, len) != queue_t::succeeded;
    }

    boost::atomic_thread_fence(boost::memory_order_release);
}

} // namespace

BOOST_AUTO_TEST_CASE(multithreaded_message_passing)
{
    unsigned int failure_count1 = 0, failure_count2 = 0, failure_count3 = 0;
    boost::atomic_thread_fence(boost::memory_order_release);

    boost::thread thread1(&multithreaded_message_passing_feeding_thread, "Thread 1", boost::ref(failure_count1));
    boost::thread thread2(&multithreaded_message_passing_feeding_thread, "Thread 2", boost::ref(failure_count2));
    boost::thread thread3(&multithreaded_message_passing_feeding_thread, "Thread 3", boost::ref(failure_count3));

    BOOST_TEST_PASSPOINT();

    queue_t queue(boost::log::open_mode::open_or_create, ipc_queue_name, capacity, block_size);
    unsigned int receive_failures = 0, receive_corruptions = 0;
    unsigned int message_count1 = 0, message_count2 = 0, message_count3 = 0;
    std::string msg;

    BOOST_TEST_PASSPOINT();

    for (unsigned int i = 0; i < message_count * 3u; ++i)
    {
        msg.clear();
        if (queue.receive(msg) == queue_t::succeeded)
        {
            if (msg == "Thread 1")
                ++message_count1;
            else if (msg == "Thread 2")
                ++message_count2;
            else if (msg == "Thread 3")
                ++message_count3;
            else
                ++receive_corruptions;
        }
        else
            ++receive_failures;
    }

    BOOST_TEST_PASSPOINT();
    thread1.join();

    BOOST_TEST_PASSPOINT();
    thread2.join();

    BOOST_TEST_PASSPOINT();
    thread3.join();

    boost::atomic_thread_fence(boost::memory_order_acquire);

    BOOST_CHECK_EQUAL(failure_count1, 0u);
    BOOST_CHECK_EQUAL(message_count1, message_count);
    BOOST_CHECK_EQUAL(failure_count2, 0u);
    BOOST_CHECK_EQUAL(message_count2, message_count);
    BOOST_CHECK_EQUAL(failure_count3, 0u);
    BOOST_CHECK_EQUAL(message_count3, message_count);
    BOOST_CHECK_EQUAL(receive_failures, 0u);
    BOOST_CHECK_EQUAL(receive_corruptions, 0u);
}

namespace {

void stop_reset_feeding_thread(queue_t& queue, queue_t::operation_result* results, unsigned int count)
{
    for (unsigned int i = 0; i < count; ++i)
    {
        results[i] = queue.send(message1, sizeof(message1) - 1u);
        if (results[i] != queue_t::succeeded)
            break;
    }

    boost::atomic_thread_fence(boost::memory_order_release);
}

void stop_reset_reading_thread(queue_t& queue, queue_t::operation_result* results, unsigned int count)
{
    std::string msg;
    for (unsigned int i = 0; i < count; ++i)
    {
        msg.clear();
        results[i] = queue.receive(msg);
        if (results[i] != queue_t::succeeded)
            break;
    }

    boost::atomic_thread_fence(boost::memory_order_release);
}

} // namespace

BOOST_AUTO_TEST_CASE(stop_reset_local)
{
    queue_t feeder_queue(boost::log::open_mode::open_or_create, ipc_queue_name, 1u, block_size);
    queue_t::operation_result feeder_results[3];
    queue_t reader_queue(boost::log::open_mode::open_only, ipc_queue_name);
    queue_t::operation_result reader_results[3];

    std::fill_n(feeder_results, sizeof(feeder_results) / sizeof(*feeder_results), queue_t::succeeded);
    std::fill_n(reader_results, sizeof(reader_results) / sizeof(*reader_results), queue_t::succeeded);
    boost::atomic_thread_fence(boost::memory_order_release);

    BOOST_TEST_PASSPOINT();

    // Case 1: Let the feeder block and then we unblock it with stop_local()
    boost::thread feeder_thread(&stop_reset_feeding_thread, boost::ref(feeder_queue), feeder_results, 3);
    boost::thread reader_thread(&stop_reset_reading_thread, boost::ref(reader_queue), reader_results, 1);

    BOOST_TEST_PASSPOINT();

    reader_thread.join();
    BOOST_TEST_PASSPOINT();
    boost::this_thread::sleep_for(boost::chrono::milliseconds(500));

    BOOST_TEST_PASSPOINT();

    feeder_queue.stop_local();
    BOOST_TEST_PASSPOINT();
    feeder_thread.join();

    boost::atomic_thread_fence(boost::memory_order_acquire);

    BOOST_CHECK_EQUAL(feeder_results[0], queue_t::succeeded);
    BOOST_CHECK_EQUAL(feeder_results[1], queue_t::succeeded);
    BOOST_CHECK_EQUAL(feeder_results[2], queue_t::aborted);
    BOOST_CHECK_EQUAL(reader_results[0], queue_t::succeeded);

    // Reset the aborted queue
    feeder_queue.reset_local();
    feeder_queue.clear();

    std::fill_n(feeder_results, sizeof(feeder_results) / sizeof(*feeder_results), queue_t::succeeded);
    std::fill_n(reader_results, sizeof(reader_results) / sizeof(*reader_results), queue_t::succeeded);
    boost::atomic_thread_fence(boost::memory_order_release);

    BOOST_TEST_PASSPOINT();

    // Case 2: Let the reader block and then we unblock it with stop_local()
    boost::thread(&stop_reset_feeding_thread, boost::ref(feeder_queue), feeder_results, 1).swap(feeder_thread);
    boost::thread(&stop_reset_reading_thread, boost::ref(reader_queue), reader_results, 2).swap(reader_thread);

    BOOST_TEST_PASSPOINT();

    feeder_thread.join();
    BOOST_TEST_PASSPOINT();
    boost::this_thread::sleep_for(boost::chrono::milliseconds(500));

    BOOST_TEST_PASSPOINT();

    reader_queue.stop_local();
    BOOST_TEST_PASSPOINT();
    reader_thread.join();

    boost::atomic_thread_fence(boost::memory_order_acquire);

    BOOST_CHECK_EQUAL(feeder_results[0], queue_t::succeeded);
    BOOST_CHECK_EQUAL(feeder_results[1], queue_t::succeeded);
    BOOST_CHECK_EQUAL(reader_results[0], queue_t::succeeded);
    BOOST_CHECK_EQUAL(reader_results[1], queue_t::aborted);
}

#endif // !defined(BOOST_LOG_NO_THREADS)

#else // !defined(BOOST_LOG_WITHOUT_IPC)

int main()
{
    return 0;
}

#endif // !defined(BOOST_LOG_WITHOUT_IPC)
