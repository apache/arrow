//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#define BOOST_LOCALE_SOURCE
#include <boost/locale/util.hpp>
#include <cstdlib>

#if defined(BOOST_WINDOWS) || defined(__CYGWIN__)
#    ifndef NOMINMAX
#        define NOMINMAX
#    endif
#    include <windows.h>
#    define BOOST_LOCALE_USE_WIN32_API
#endif

namespace boost { namespace locale { namespace util {
    std::string get_system_locale(bool use_utf8)
    {
        const char* lang = 0;
        if(!lang || !*lang)
            lang = getenv("LC_CTYPE");
        if(!lang || !*lang)
            lang = getenv("LC_ALL");
        if(!lang || !*lang)
            lang = getenv("LANG");
#ifndef BOOST_LOCALE_USE_WIN32_API
        (void)use_utf8; // not relevant for non-windows
        if(!lang || !*lang)
            lang = "C";
        return lang;
#else
        if(lang && *lang) {
            return lang;
        }
        char buf[10];
        if(GetLocaleInfoA(LOCALE_USER_DEFAULT, LOCALE_SISO639LANGNAME, buf, sizeof(buf)) == 0)
            return "C";
        std::string lc_name = buf;
        if(GetLocaleInfoA(LOCALE_USER_DEFAULT, LOCALE_SISO3166CTRYNAME, buf, sizeof(buf)) != 0) {
            lc_name += "_";
            lc_name += buf;
        }
        if(!use_utf8) {
            if(GetLocaleInfoA(LOCALE_USER_DEFAULT, LOCALE_IDEFAULTANSICODEPAGE, buf, sizeof(buf)) != 0) {
                if(atoi(buf) == 0)
                    lc_name += ".UTF-8";
                else {
                    lc_name += ".windows-";
                    lc_name += buf;
                }
            } else {
                lc_name += "UTF-8";
            }
        } else {
            lc_name += ".UTF-8";
        }
        return lc_name;

#endif
    }
}}} // namespace boost::locale::util
