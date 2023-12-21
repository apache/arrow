//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#define BOOST_LOCALE_SOURCE
#include <boost/locale/encoding.hpp>
#include "boost/locale/std/all_generator.hpp"
#include <ios>
#include <locale>
#include <string>

namespace boost { namespace locale { namespace impl_std {

    class utf8_collator_from_wide : public std::collate<char> {
    public:
        typedef std::collate<wchar_t> wfacet;
        utf8_collator_from_wide(const std::locale& base, size_t refs = 0) : std::collate<char>(refs), base_(base) {}
        int do_compare(const char* lb, const char* le, const char* rb, const char* re) const override
        {
            std::wstring l = conv::to_utf<wchar_t>(lb, le, "UTF-8");
            std::wstring r = conv::to_utf<wchar_t>(rb, re, "UTF-8");
            return std::use_facet<wfacet>(base_).compare(l.c_str(),
                                                         l.c_str() + l.size(),
                                                         r.c_str(),
                                                         r.c_str() + r.size());
        }
        long do_hash(const char* b, const char* e) const override
        {
            std::wstring tmp = conv::to_utf<wchar_t>(b, e, "UTF-8");
            return std::use_facet<wfacet>(base_).hash(tmp.c_str(), tmp.c_str() + tmp.size());
        }
        std::string do_transform(const char* b, const char* e) const override
        {
            std::wstring tmp = conv::to_utf<wchar_t>(b, e, "UTF-8");
            std::wstring wkey = std::use_facet<wfacet>(base_).transform(tmp.c_str(), tmp.c_str() + tmp.size());
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
        std::locale base_;
    };

    std::locale
    create_collate(const std::locale& in, const std::string& locale_name, char_facet_t type, utf8_support utf)
    {
        switch(type) {
            case char_facet_t::nochar: break;
            case char_facet_t::char_f: {
                if(utf == utf8_support::from_wide) {
                    std::locale base =
                      std::locale(std::locale::classic(), new std::collate_byname<wchar_t>(locale_name.c_str()));
                    return std::locale(in, new utf8_collator_from_wide(base));
                } else {
                    return std::locale(in, new std::collate_byname<char>(locale_name.c_str()));
                }
            }

            case char_facet_t::wchar_f: return std::locale(in, new std::collate_byname<wchar_t>(locale_name.c_str()));

#ifdef BOOST_LOCALE_ENABLE_CHAR16_T
            case char_facet_t::char16_f: return std::locale(in, new std::collate_byname<char16_t>(locale_name.c_str()));
#endif

#ifdef BOOST_LOCALE_ENABLE_CHAR32_T
            case char_facet_t::char32_f: return std::locale(in, new std::collate_byname<char32_t>(locale_name.c_str()));
#endif
        }
        return in;
    }

}}} // namespace boost::locale::impl_std
