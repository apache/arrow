//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#define BOOST_LOCALE_SOURCE
#include "boost/locale/win32/win_backend.hpp"
#include <boost/locale/gnu_gettext.hpp>
#include <boost/locale/info.hpp>
#include <boost/locale/localization_backend.hpp>
#include <boost/locale/util.hpp>
#include "boost/locale/util/gregorian.hpp"
#include "boost/locale/util/locale_data.hpp"
#include "boost/locale/win32/all_generator.hpp"
#include "boost/locale/win32/api.hpp"
#include <algorithm>
#include <iterator>
#include <vector>

namespace boost { namespace locale { namespace impl_win {

    class winapi_localization_backend : public localization_backend {
    public:
        winapi_localization_backend() : invalid_(true) {}
        winapi_localization_backend(const winapi_localization_backend& other) :
            localization_backend(), paths_(other.paths_), domains_(other.domains_), locale_id_(other.locale_id_),
            invalid_(true)
        {}
        winapi_localization_backend* clone() const override { return new winapi_localization_backend(*this); }

        void set_option(const std::string& name, const std::string& value) override
        {
            invalid_ = true;
            if(name == "locale")
                locale_id_ = value;
            else if(name == "message_path")
                paths_.push_back(value);
            else if(name == "message_application")
                domains_.push_back(value);
        }
        void clear_options() override
        {
            invalid_ = true;
            locale_id_.clear();
            paths_.clear();
            domains_.clear();
        }

        void prepare_data()
        {
            if(!invalid_)
                return;
            invalid_ = false;
            if(locale_id_.empty()) {
                real_id_ = util::get_system_locale(true); // always UTF-8
                lc_ = winlocale(real_id_);
            } else {
                lc_ = winlocale(locale_id_);
                real_id_ = locale_id_;
            }
            util::locale_data d;
            d.parse(real_id_);
            if(!d.utf8) {
                lc_ = winlocale();
                // Make it C as non-UTF8 locales are not supported
            }
        }

        std::locale install(const std::locale& base, category_t category, char_facet_t type) override
        {
            prepare_data();

            switch(category) {
                case category_t::convert: return create_convert(base, lc_, type);
                case category_t::collation: return create_collate(base, lc_, type);
                case category_t::formatting: return create_formatting(base, lc_, type);
                case category_t::parsing: return create_parsing(base, lc_, type);
                case category_t::calendar: {
                    util::locale_data inf;
                    inf.parse(real_id_);
                    return util::install_gregorian_calendar(base, inf.country);
                }
                case category_t::message: {
                    gnu_gettext::messages_info minf;
                    std::locale tmp = util::create_info(std::locale::classic(), real_id_);
                    const boost::locale::info& inf = std::use_facet<boost::locale::info>(tmp);
                    minf.language = inf.language();
                    minf.country = inf.country();
                    minf.variant = inf.variant();
                    minf.encoding = inf.encoding();
                    std::copy(domains_.begin(),
                              domains_.end(),
                              std::back_inserter<gnu_gettext::messages_info::domains_type>(minf.domains));
                    minf.paths = paths_;
                    switch(type) {
                        case char_facet_t::nochar: break;
                        case char_facet_t::char_f:
                            return std::locale(base, gnu_gettext::create_messages_facet<char>(minf));
                        case char_facet_t::wchar_f:
                            return std::locale(base, gnu_gettext::create_messages_facet<wchar_t>(minf));
#ifdef BOOST_LOCALE_ENABLE_CHAR16_T
                        case char_facet_t::char16_f:
                            return std::locale(base, gnu_gettext::create_messages_facet<char16_t>(minf));
#endif
#ifdef BOOST_LOCALE_ENABLE_CHAR32_T
                        case char_facet_t::char32_f:
                            return std::locale(base, gnu_gettext::create_messages_facet<char32_t>(minf));
#endif
                    }
                    return base;
                }
                case category_t::information: return util::create_info(base, real_id_);
                case category_t::codepage: return util::create_utf8_codecvt(base, type);
                case category_t::boundary: break; // Not implemented
            }
            return base;
        }

    private:
        std::vector<std::string> paths_;
        std::vector<std::string> domains_;
        std::string locale_id_;
        std::string real_id_;

        bool invalid_;
        winlocale lc_;
    };

    localization_backend* create_localization_backend()
    {
        return new winapi_localization_backend();
    }

}}} // namespace boost::locale::impl_win
