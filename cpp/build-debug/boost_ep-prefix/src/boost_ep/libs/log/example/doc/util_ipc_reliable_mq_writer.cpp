/*
 *             Copyright Andrey Semashev 2016.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */

#if !defined(BOOST_LOG_WITHOUT_IPC)

#include <iostream>
#include <string>
#include <boost/log/utility/ipc/reliable_message_queue.hpp>
#include <boost/log/utility/ipc/object_name.hpp>
#include <boost/log/utility/open_mode.hpp>

namespace logging = boost::log;
namespace keywords = boost::log::keywords;

//[ example_util_ipc_reliable_mq_writer
int main()
{
    typedef logging::ipc::reliable_message_queue queue_t;

    // Create a message_queue_type object that is associated with the interprocess
    // message queue named "ipc_message_queue".
    queue_t queue
    (
        keywords::name = logging::ipc::object_name(logging::ipc::object_name::user, "ipc_message_queue"),
        keywords::open_mode = logging::open_mode::open_or_create,  /*< create the queue, if not yet created >*/
        keywords::capacity = 256,                                  /*< if the queue has to be created, allocate 256 blocks... >*/
        keywords::block_size = 1024,                               /*< ... of 1 KiB each for messages >*/
        keywords::overflow_policy = queue_t::fail_on_overflow      /*< if the queue is full, return error to the writer >*/
    );

    // Send a message through the queue
    std::string message = "Hello, Viewer!";
    queue_t::operation_result result = queue.send(message.data(), static_cast< queue_t::size_type >(message.size()));

//<-
#if !defined(BOOST_NO_CXX11_SCOPED_ENUMS)
//->
    // See if the message was sent
    switch (result)
    {
    case queue_t::operation_result::succeeded:
        std::cout << "Message sent successfully" << std::endl;
        break;

    case queue_t::operation_result::no_space:
        std::cout << "Message could not be sent because the queue is full" << std::endl;
        break;

    case queue_t::operation_result::aborted:
        // This can happen is overflow_policy is block_on_overflow
        std::cout << "Message sending operation has been interrupted" << std::endl;
        break;
    }
//<-
#else // !defined(BOOST_NO_CXX11_SCOPED_ENUMS)

    // Strict C++03 compilers do not allow to use enum type as a scope for its values. Otherwise, the code is the same as above.
    switch (result)
    {
    case queue_t::succeeded:
        std::cout << "Message sent successfully" << std::endl;
        break;

    case queue_t::no_space:
        std::cout << "Message could not be sent because the queue is full" << std::endl;
        break;

    case queue_t::aborted:
        // This can happen is overflow_policy is block_on_overflow
        std::cout << "Message sending operation has been interrupted" << std::endl;
        break;
    }

#endif // !defined(BOOST_NO_CXX11_SCOPED_ENUMS)
//->

    return 0;
}
//]

#else // !defined(BOOST_LOG_WITHOUT_IPC)

int main()
{
    return 0;
}

#endif // !defined(BOOST_LOG_WITHOUT_IPC)
