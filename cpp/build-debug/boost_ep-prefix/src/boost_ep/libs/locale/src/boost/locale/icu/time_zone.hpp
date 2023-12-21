//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_LOCALE_IMPL_ICU_GET_TIME_ZONE_HPP
#define BOOST_LOCALE_IMPL_ICU_GET_TIME_ZONE_HPP

#include <boost/locale/config.hpp>
#ifdef BOOST_HAS_STDINT_H
#    include <stdint.h> // Avoid ICU defining e.g. INT8_MIN causing macro redefinition warnings
#endif
#include <string>
#include <unicode/timezone.h>

namespace boost { namespace locale { namespace impl_icu {

    // Provides a workaround for an ICU default timezone bug and also
    // handles time_zone string correctly - if empty returns default
    // otherwise returns the instance created with time_zone
    icu::TimeZone* get_time_zone(const std::string& time_zone);
}}} // namespace boost::locale::impl_icu
#endif
