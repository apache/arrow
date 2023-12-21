//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#define BOOST_LOCALE_SOURCE
#include "boost/locale/icu/icu_backend.hpp"
#include <boost/locale/gnu_gettext.hpp>
#include <boost/locale/localization_backend.hpp>
#include <boost/locale/util.hpp>
#include "boost/locale/icu/all_generator.hpp"
#include "boost/locale/icu/cdata.hpp"
#include "boost/locale/util/locale_data.hpp"
#include <algorithm>
#include <iterator>

#include <unicode/ucnv.h>

namespace boost { namespace locale { namespace impl_icu {
    class icu_localization_backend : public localization_backend {
    public:
        icu_localization_backend() : invalid_(true), use_ansi_encoding_(false) {}
        icu_localization_backend(const icu_localization_backend& other) :
            localization_backend(), paths_(other.paths_), domains_(other.domains_), locale_id_(other.locale_id_),
            invalid_(true), use_ansi_encoding_(other.use_ansi_encoding_)
        {}
        icu_localization_backend* clone() const override { return new icu_localization_backend(*this); }

        void set_option(const std::string& name, const std::string& value) override
        {
            invalid_ = true;
            if(name == "locale")
                locale_id_ = value;
            else if(name == "message_path")
                paths_.push_back(value);
            else if(name == "message_application")
                domains_.push_back(value);
            else if(name == "use_ansi_encoding")
                use_ansi_encoding_ = value == "true";
        }
        void clear_options() override
        {
            invalid_ = true;
            use_ansi_encoding_ = false;
            locale_id_.clear();
            paths_.clear();
            domains_.clear();
        }

        void prepare_data()
        {
            if(!invalid_)
                return;
            invalid_ = false;
            real_id_ = locale_id_;
            if(real_id_.empty()) {
                bool utf8 = !use_ansi_encoding_;
                real_id_ = util::get_system_locale(utf8);
            }

            util::locale_data d;
            d.parse(real_id_);

            data_.locale = icu::Locale::createCanonical(real_id_.c_str());
            data_.encoding = d.encoding;
            data_.utf8 = d.utf8;
            language_ = d.language;
            country_ = d.country;
            variant_ = d.variant;
        }

        std::locale install(const std::locale& base, category_t category, char_facet_t type) override
        {
            prepare_data();

            switch(category) {
                case category_t::convert: return create_convert(base, data_, type);
                case category_t::collation: return create_collate(base, data_, type);
                case category_t::formatting: return create_formatting(base, data_, type);
                case category_t::parsing: return create_parsing(base, data_, type);
                case category_t::codepage: return create_codecvt(base, data_.encoding, type);
                case category_t::message: {
                    gnu_gettext::messages_info minf;
                    minf.language = language_;
                    minf.country = country_;
                    minf.variant = variant_;
                    minf.encoding = data_.encoding;
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
                case category_t::boundary: return create_boundary(base, data_, type);
                case category_t::calendar: return create_calendar(base, data_);
                case category_t::information: return util::create_info(base, real_id_);
            }
            return base;
        }

    private:
        std::vector<std::string> paths_;
        std::vector<std::string> domains_;
        std::string locale_id_;

        cdata data_;
        std::string language_;
        std::string country_;
        std::string variant_;
        std::string real_id_;
        bool invalid_;
        bool use_ansi_encoding_;
    };

    localization_backend* create_localization_backend()
    {
        return new icu_localization_backend();
    }

}}} // namespace boost::locale::impl_icu
