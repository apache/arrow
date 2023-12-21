/*
 *                Copyright Lingxi Li 2015.
 *             Copyright Andrey Semashev 2016.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */

#if !defined(BOOST_LOG_WITHOUT_IPC)

#include <iostream>
#include <string>
#include <exception>
#include <boost/log/utility/ipc/reliable_message_queue.hpp>
#include <boost/log/utility/ipc/object_name.hpp>
#include <boost/log/utility/open_mode.hpp>

namespace logging = boost::log;
namespace keywords = boost::log::keywords;

//[ example_sinks_ipc_receiver
int main()
{
    try
    {
        typedef logging::ipc::reliable_message_queue queue_t;

        // Create a message_queue_type object that is associated with the interprocess
        // message queue named "ipc_message_queue".
        queue_t queue
        (
            keywords::name = logging::ipc::object_name(logging::ipc::object_name::user, "ipc_message_queue"),
            keywords::open_mode = logging::open_mode::open_or_create,
            keywords::capacity = 256,
            keywords::block_size = 1024,
            keywords::overflow_policy = queue_t::block_on_overflow
        );

        std::cout << "Viewer process running..." << std::endl;

        // Keep reading log messages from the associated message queue and print them on the console.
        // queue.receive() will block if the queue is empty.
        std::string message;
        while (queue.receive(message) == queue_t::succeeded)
        {
            std::cout << message << std::endl;

            // Clear the buffer for the next message
            message.clear();
        }
    }
    catch (std::exception& e)
    {
        std::cout << "Failure: " << e.what() << std::endl;
    }

    return 0;
}
//]

#else // !defined(BOOST_LOG_WITHOUT_IPC)

int main()
{
    return 0;
}

#endif // !defined(BOOST_LOG_WITHOUT_IPC)
