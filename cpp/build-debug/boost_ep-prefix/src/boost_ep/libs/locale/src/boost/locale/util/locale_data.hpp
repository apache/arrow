//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_LOCALE_IMPL_UTIL_LOCALE_DATA_HPP
#define BOOST_LOCALE_IMPL_UTIL_LOCALE_DATA_HPP

#include <string>

namespace boost { namespace locale { namespace util {

    class locale_data {
    public:
        locale_data() : language("C"), encoding("us-ascii"), utf8(false) {}

        std::string language;
        std::string country;
        std::string variant;
        std::string encoding;
        bool utf8;

        void parse(const std::string& locale_name);

    private:
        void parse_from_lang(const std::string& locale_name);
        void parse_from_country(const std::string& locale_name);
        void parse_from_encoding(const std::string& locale_name);
        void parse_from_variant(const std::string& locale_name);
    };

}}} // namespace boost::locale::util

#endif
