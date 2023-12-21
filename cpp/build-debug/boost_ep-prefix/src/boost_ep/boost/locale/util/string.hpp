//
// Copyright (c) 2022 Alexander Grund
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_LOCALE_UTIL_STRING_HPP
#define BOOST_LOCALE_UTIL_STRING_HPP

#include <boost/locale/config.hpp>

namespace boost { namespace locale { namespace util {
    /// Return the end of a C-string, i.e. the pointer to the trailing NULL byte
    template<typename Char>
    Char* str_end(Char* str)
    {
        while(*str)
            ++str;
        return str;
    }
}}} // namespace boost::locale::util

#endif