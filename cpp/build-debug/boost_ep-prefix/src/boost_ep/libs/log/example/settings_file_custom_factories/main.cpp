/*
 *          Copyright Andrey Semashev 2007 - 2015.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
/*!
 * \file   main.cpp
 * \author Andrey Semashev
 * \date   12.05.2010
 *
 * \brief  An example of initializing the library from a settings file,
 *         with custom filter and formatter factories for attributes.
 */

// #define BOOST_ALL_DYN_LINK 1

#include <string>
#include <iostream>
#include <fstream>
#include <stdexcept>
#include <boost/core/ref.hpp>
#include <boost/bind/bind.hpp>
#include <boost/smart_ptr/make_shared_object.hpp>
#include <boost/log/core.hpp>
#include <boost/log/common.hpp>
#include <boost/log/attributes.hpp>
#include <boost/log/core/record.hpp>
#include <boost/log/attributes/value_visitation.hpp>
#include <boost/log/utility/setup/from_stream.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/filter_parser.hpp>
#include <boost/log/utility/setup/formatter_parser.hpp>

namespace logging = boost::log;
namespace attrs = boost::log::attributes;
namespace src = boost::log::sources;

//! Enum for our custom severity levels
enum severity_level
{
    normal,
    notification,
    warning,
    error,
    critical
};

//! Formatting operator for severity levels
inline std::ostream& operator<< (std::ostream& strm, severity_level level)
{
    switch (level)
    {
    case normal:
        strm << "normal";
        break;
    case notification:
        strm << "notification";
        break;
    case warning:
        strm << "warning";
        break;
    case error:
        strm << "error";
        break;
    case critical:
        strm << "critical";
        break;
    default:
        strm << static_cast< int >(level);
        break;
    }

    return strm;
}

//! Parsing operator for severity levels
inline std::istream& operator>> (std::istream& strm, severity_level& level)
{
    if (strm.good())
    {
        std::string str;
        strm >> str;
        if (str == "normal")
            level = normal;
        else if (str == "notification")
            level = notification;
        else if (str == "warning")
            level = warning;
        else if (str == "error")
            level = error;
        else if (str == "critical")
            level = critical;
        else
            strm.setstate(std::ios_base::failbit);
    }

    return strm;
}

//! Our custom formatter for the scope list
struct scope_list_formatter
{
    typedef void result_type;
    typedef attrs::named_scope::value_type scope_stack;

    explicit scope_list_formatter(logging::attribute_name const& name) :
        name_(name)
    {
    }
    void operator()(logging::record_view const& rec, logging::formatting_ostream& strm) const
    {
        // We need to acquire the attribute value from the log record
        logging::visit< scope_stack >
        (
            name_,
            rec.attribute_values(),
            boost::bind(&scope_list_formatter::format, boost::placeholders::_1, boost::ref(strm))
        );
    }

private:
    //! This is where our custom formatting takes place
    static void format(scope_stack const& scopes, logging::formatting_ostream& strm)
    {
        scope_stack::const_iterator it = scopes.begin(), end = scopes.end();
        for (; it != end; ++it)
        {
            strm << "\t" << it->scope_name << " [" << it->file_name << ":" << it->line << "]\n";
        }
    }

private:
    logging::attribute_name name_;
};

class my_scopes_formatter_factory :
    public logging::formatter_factory< char >
{
public:
    /*!
     * This function creates a formatter for the MyScopes attribute.
     * It effectively associates the attribute with the scope_list_formatter class
     */
    formatter_type create_formatter(
        logging::attribute_name const& attr_name, args_map const& args)
    {
        return formatter_type(scope_list_formatter(attr_name));
    }
};

//! The function initializes the logging library
void init_logging()
{
    // First thing - register the custom formatter for MyScopes
    logging::register_formatter_factory("MyScopes", boost::make_shared< my_scopes_formatter_factory >());

    // Also register filter and formatter factories for our custom severity level enum. Since our operator<< and operator>> implement
    // all required behavior, simple factories provided by Boost.Log will do.
    logging::register_simple_filter_factory< severity_level >("Severity");
    logging::register_simple_formatter_factory< severity_level, char >("Severity");

    // Then load the settings from the file
    std::ifstream settings("settings.txt");
    if (!settings.is_open())
        throw std::runtime_error("Could not open settings.txt file");
    logging::init_from_stream(settings);

    // Add some attributes. Note that severity level will be provided by the logger, so we don't need to add it here.
    logging::add_common_attributes();

    logging::core::get()->add_global_attribute("MyScopes", attrs::named_scope());
}

//! Global logger, which we will use to write log messages
BOOST_LOG_INLINE_GLOBAL_LOGGER_DEFAULT(test_lg, src::severity_logger< severity_level >)

//! The function tests logging
void try_logging()
{
    BOOST_LOG_FUNCTION();

    src::severity_logger< severity_level >& lg = test_lg::get();

    BOOST_LOG_SEV(lg, critical) << "This is a critical severity record";

    BOOST_LOG_NAMED_SCOPE("random name");
    BOOST_LOG_SEV(lg, error) << "This is a error severity record";
}

int main(int argc, char* argv[])
{
    try
    {
        init_logging();
        try_logging();
    }
    catch (std::exception& e)
    {
        std::cout << "FAILURE: " << e.what() << std::endl;
        return -1;
    }

    return 0;
}
