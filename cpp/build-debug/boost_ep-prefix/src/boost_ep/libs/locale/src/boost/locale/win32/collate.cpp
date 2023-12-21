//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#define BOOST_LOCALE_SOURCE
#include <boost/locale/encoding.hpp>
#include <boost/locale/generator.hpp>
#include "boost/locale/shared/mo_hash.hpp"
#include "boost/locale/win32/api.hpp"
#include <ios>
#include <locale>
#include <string>

namespace boost { namespace locale { namespace impl_win {

    class utf8_collator : public collator<char> {
    public:
        utf8_collator(winlocale lc, size_t refs = 0) : collator<char>(refs), lc_(lc) {}
        int
        do_compare(collate_level level, const char* lb, const char* le, const char* rb, const char* re) const override
        {
            std::wstring l = conv::to_utf<wchar_t>(lb, le, "UTF-8");
            std::wstring r = conv::to_utf<wchar_t>(rb, re, "UTF-8");
            return wcscoll_l(level, l.c_str(), l.c_str() + l.size(), r.c_str(), r.c_str() + r.size(), lc_);
        }
        long do_hash(collate_level level, const char* b, const char* e) const override
        {
            std::string key = do_transform(level, b, e);
            return gnu_gettext::pj_winberger_hash_function(key.c_str(), key.c_str() + key.size());
        }
        std::string do_transform(collate_level level, const char* b, const char* e) const override
        {
            std::wstring tmp = conv::to_utf<wchar_t>(b, e, "UTF-8");
            std::wstring wkey = wcsxfrm_l(level, tmp.c_str(), tmp.c_str() + tmp.size(), lc_);
            std::string key;
            BOOST_LOCALE_START_CONST_CONDITION
            if(sizeof(wchar_t) == 2)
                key.reserve(wkey.size() * 2);
            else
                key.reserve(wkey.size() * 3);
            for(unsigned i = 0; i < wkey.size(); i++) {
                if(sizeof(wchar_t) == 2) {
                    uint16_t tv = static_cast<uint16_t>(wkey[i]);
                    key += char(tv >> 8);
                    key += char(tv & 0xFF);
                } else { // 4
                    uint32_t tv = static_cast<uint32_t>(wkey[i]);
                    // 21 bit
                    key += char((tv >> 16) & 0xFF);
                    key += char((tv >> 8) & 0xFF);
                    key += char(tv & 0xFF);
                }
            }
            BOOST_LOCALE_END_CONST_CONDITION
            return key;
        }

    private:
        winlocale lc_;
    };

    class utf16_collator : public collator<wchar_t> {
    public:
        typedef std::collate<wchar_t> wfacet;
        utf16_collator(winlocale lc, size_t refs = 0) : collator<wchar_t>(refs), lc_(lc) {}
        int do_compare(collate_level level,
                       const wchar_t* lb,
                       const wchar_t* le,
                       const wchar_t* rb,
                       const wchar_t* re) const override
        {
            return wcscoll_l(level, lb, le, rb, re, lc_);
        }
        long do_hash(collate_level level, const wchar_t* b, const wchar_t* e) const override
        {
            std::wstring key = do_transform(level, b, e);
            const char* begin = reinterpret_cast<const char*>(key.c_str());
            const char* end = begin + key.size() * sizeof(wchar_t);
            return gnu_gettext::pj_winberger_hash_function(begin, end);
        }
        std::wstring do_transform(collate_level level, const wchar_t* b, const wchar_t* e) const override
        {
            return wcsxfrm_l(level, b, e, lc_);
        }

    private:
        winlocale lc_;
    };

    std::locale create_collate(const std::locale& in, const winlocale& lc, char_facet_t type)
    {
        if(lc.is_c()) {
            switch(type) {
                case char_facet_t::nochar: break;
                case char_facet_t::char_f: return std::locale(in, new std::collate_byname<char>("C"));
                case char_facet_t::wchar_f: return std::locale(in, new std::collate_byname<wchar_t>("C"));
#ifdef BOOST_LOCALE_ENABLE_CHAR16_T
                case char_facet_t::char16_f: return std::locale(in, new collate_byname<char16_t>("C"));
#endif
#ifdef BOOST_LOCALE_ENABLE_CHAR32_T
                case char_facet_t::char32_f: return std::locale(in, new collate_byname<char32_t>("C"));
#endif
            }
        } else {
            switch(type) {
                case char_facet_t::nochar: break;
                case char_facet_t::char_f: return std::locale(in, new utf8_collator(lc));
                case char_facet_t::wchar_f: return std::locale(in, new utf16_collator(lc));
#ifdef BOOST_LOCALE_ENABLE_CHAR16_T
                case char_facet_t::char16_f: break;
#endif
#ifdef BOOST_LOCALE_ENABLE_CHAR32_T
                case char_facet_t::char32_f: break;
#endif
            }
        }
        return in;
    }

}}} // namespace boost::locale::impl_win
