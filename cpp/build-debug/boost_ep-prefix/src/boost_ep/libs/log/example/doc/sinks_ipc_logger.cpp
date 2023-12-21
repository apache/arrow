/*
 *                Copyright Lingxi Li 2015.
 *             Copyright Andrey Semashev 2016.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */

#if !defined(BOOST_LOG_WITHOUT_IPC)

#include <iostream>
#include <sstream>
#include <exception>
#include <boost/smart_ptr/shared_ptr.hpp>
#include <boost/smart_ptr/make_shared_object.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/log/core.hpp>
#include <boost/log/attributes.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sources/logger.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sinks/sync_frontend.hpp>
#include <boost/log/sinks/text_ipc_message_queue_backend.hpp>
#include <boost/log/utility/ipc/reliable_message_queue.hpp>
#include <boost/log/utility/ipc/object_name.hpp>
#include <boost/log/utility/open_mode.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>

namespace logging = boost::log;
namespace expr = boost::log::expressions;
namespace attrs = boost::log::attributes;
namespace src = boost::log::sources;
namespace sinks = boost::log::sinks;
namespace keywords = boost::log::keywords;

//[ example_sinks_ipc_logger
BOOST_LOG_ATTRIBUTE_KEYWORD(a_timestamp, "TimeStamp", attrs::local_clock::value_type)
BOOST_LOG_ATTRIBUTE_KEYWORD(a_process_id, "ProcessID", attrs::current_process_id::value_type)
BOOST_LOG_ATTRIBUTE_KEYWORD(a_thread_id, "ThreadID", attrs::current_thread_id::value_type)

int main()
{
    try
    {
        typedef logging::ipc::reliable_message_queue queue_t;
        typedef sinks::text_ipc_message_queue_backend< queue_t > backend_t;
        typedef sinks::synchronous_sink< backend_t > sink_t;

        // Create a sink that is associated with the interprocess message queue
        // named "ipc_message_queue".
        boost::shared_ptr< sink_t > sink = boost::make_shared< sink_t >
        (
            keywords::name = logging::ipc::object_name(logging::ipc::object_name::user, "ipc_message_queue"),
            keywords::open_mode = logging::open_mode::open_or_create,
            keywords::capacity = 256,
            keywords::block_size = 1024,
            keywords::overflow_policy = queue_t::block_on_overflow
        );

        // Set the formatter
        sink->set_formatter
        (
            expr::stream << "[" << a_timestamp << "] [" << a_process_id << ":" << a_thread_id << "] " << expr::smessage
        );

        logging::core::get()->add_sink(sink);

        // Add the commonly used attributes, including TimeStamp, ProcessID and ThreadID
        logging::add_common_attributes();

        // Do some logging
        src::logger logger;
        for (unsigned int i = 1; i <= 10; ++i)
        {
            BOOST_LOG(logger) << "Message #" << i;
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
