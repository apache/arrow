//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_LOCALE_IMPL_STD_ALL_GENERATOR_HPP
#define BOOST_LOCALE_IMPL_STD_ALL_GENERATOR_HPP

#include <boost/locale/generator.hpp>
#include <locale>
#include <string>

namespace boost { namespace locale { namespace impl_std {
    enum class utf8_support { none, native, native_with_wide, from_wide };

    std::locale create_convert(const std::locale& in,
                               const std::string& locale_name,
                               char_facet_t type,
                               utf8_support utf = utf8_support::none);

    std::locale create_collate(const std::locale& in,
                               const std::string& locale_name,
                               char_facet_t type,
                               utf8_support utf = utf8_support::none);

    std::locale create_formatting(const std::locale& in,
                                  const std::string& locale_name,
                                  char_facet_t type,
                                  utf8_support utf = utf8_support::none);

    std::locale create_parsing(const std::locale& in,
                               const std::string& locale_name,
                               char_facet_t type,
                               utf8_support utf = utf8_support::none);

    std::locale create_codecvt(const std::locale& in,
                               const std::string& locale_name,
                               char_facet_t type,
                               utf8_support utf = utf8_support::none);

}}} // namespace boost::locale::impl_std

#endif
