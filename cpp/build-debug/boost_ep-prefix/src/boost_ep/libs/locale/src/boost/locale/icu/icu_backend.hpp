//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_LOCALE_IMPL_ICU_LOCALIZATION_BACKEND_HPP
#define BOOST_LOCALE_IMPL_ICU_LOCALIZATION_BACKEND_HPP

#include <boost/locale/config.hpp>

namespace boost { namespace locale {
    class localization_backend;
    namespace impl_icu {
        localization_backend* create_localization_backend();
    } // namespace impl_icu
}}    // namespace boost::locale
#endif
