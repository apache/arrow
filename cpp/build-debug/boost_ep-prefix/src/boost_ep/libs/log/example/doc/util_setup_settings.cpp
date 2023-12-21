/*
 *             Copyright Andrey Semashev 2019.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          https://www.boost.org/LICENSE_1_0.txt)
 */

#include <cstddef>
#include <string>
#include <iostream>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/settings.hpp>
#include <boost/log/utility/setup/from_settings.hpp>
#include <boost/log/utility/setup/filter_parser.hpp>
#include <boost/log/utility/setup/formatter_parser.hpp>

namespace logging = boost::log;
namespace src = boost::log::sources;

// Let's define our own severity levels
enum severity_level
{
    normal,
    notification,
    warning,
    error,
    critical
};

const char* const severity_strings[] =
{
    "normal",
    "notification",
    "warning",
    "error",
    "critical"
};

// The operator puts a human-friendly representation of the severity level to the stream
std::ostream& operator<< (std::ostream& strm, severity_level level)
{
    if (static_cast< std::size_t >(level) < sizeof(severity_strings) / sizeof(*severity_strings))
        strm << severity_strings[level];
    else
        strm << static_cast< int >(level);

    return strm;
}

// The operator parses the severity level from the stream
std::istream& operator>> (std::istream& strm, severity_level& level)
{
    if (strm.good())
    {
        std::string str;
        strm >> str;

        for (unsigned int i = 0; i < sizeof(severity_strings) / sizeof(*severity_strings); ++i)
        {
            if (str == severity_strings[i])
            {
                level = static_cast< severity_level >(i);
                return strm;
            }
        }

        strm.setstate(std::ios_base::failbit);
    }

    return strm;
}

void init_logging()
{
    // Before initializing the library from settings, we need to register any custom filter and formatter factories
    logging::register_simple_filter_factory< severity_level >("Severity");
    logging::register_simple_formatter_factory< severity_level, char >("Severity");

//[ example_util_setup_settings
    logging::settings setts;

    setts["Core"]["Filter"] = "%Severity% >= warning";
    setts["Core"]["DisableLogging"] = false;

    // Subsections can be referred to with a single path
    setts["Sinks.Console"]["Destination"] = "Console";
    setts["Sinks.Console"]["Filter"] = "%Severity% >= critical";
    setts["Sinks.Console"]["Format"] = "%TimeStamp% [%Severity%] %Message%";
    setts["Sinks.Console"]["AutoFlush"] = true;

    // ...as well as the individual parameters
    setts["Sinks.File.Destination"] = "TextFile";
    setts["Sinks.File.FileName"] = "MyApp_%3N.log";
    setts["Sinks.File.AutoFlush"] = true;
    setts["Sinks.File.RotationSize"] = 10 * 1024 * 1024; // 10 MiB
    setts["Sinks.File.Format"] = "%TimeStamp% [%Severity%] %Message%";

    logging::init_from_settings(setts);
//]

    // Add attributes
    logging::add_common_attributes();
}

int main(int, char*[])
{
    init_logging();

    src::severity_logger< severity_level > lg;

    BOOST_LOG_SEV(lg, normal) << "A regular message";
    BOOST_LOG_SEV(lg, warning) << "Something bad is going on but I can handle it";
    BOOST_LOG_SEV(lg, critical) << "Everything crumbles, shoot me now!";

    return 0;
}
