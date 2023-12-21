//  Copyright (c) 2021 Andrey Semashev
//
//  Distributed under the Boost Software License, Version 1.0.
//  See accompanying file LICENSE_1_0.txt or copy at
//  https://www.boost.org/LICENSE_1_0.txt)

#include <boost/log/utility/setup.hpp>
#include <sstream>

int main()
{
#if !defined(BOOST_LOG_WITHOUT_SETTINGS_PARSERS)
    std::istringstream strm;
    boost::log::init_from_stream(strm);
#endif
}
