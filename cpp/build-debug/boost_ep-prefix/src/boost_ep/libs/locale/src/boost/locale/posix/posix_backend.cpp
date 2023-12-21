//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#define BOOST_LOCALE_SOURCE
#include "boost/locale/posix/posix_backend.hpp"
#include <boost/locale/gnu_gettext.hpp>
#include <boost/locale/info.hpp>
#include <boost/locale/localization_backend.hpp>
#include <boost/locale/util.hpp>
#include <algorithm>
#include <iterator>
#include <langinfo.h>
#include <vector>
#if defined(__FreeBSD__)
#    include <xlocale.h>
#endif

#include "boost/locale/posix/all_generator.hpp"
#include "boost/locale/util/gregorian.hpp"
#include "boost/locale/util/locale_data.hpp"

namespace boost { namespace locale { namespace impl_posix {

    class posix_localization_backend : public localization_backend {
    public:
        posix_localization_backend() : invalid_(true) {}
        posix_localization_backend(const posix_localization_backend& other) :
            localization_backend(), paths_(other.paths_), domains_(other.domains_), locale_id_(other.locale_id_),
            invalid_(true)
        {}
        posix_localization_backend* clone() const override { return new posix_localization_backend(*this); }

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

        static void free_locale_by_ptr(locale_t* lc)
        {
            freelocale(*lc);
            delete lc;
        }

        void prepare_data()
        {
            if(!invalid_)
                return;
            invalid_ = false;
            lc_.reset();
            real_id_ = locale_id_;
            if(real_id_.empty())
                real_id_ = util::get_system_locale();

            locale_t tmp = newlocale(LC_ALL_MASK, real_id_.c_str(), 0);

            if(!tmp) {
                tmp = newlocale(LC_ALL_MASK, "C", 0);
            }
            if(!tmp) {
                throw std::runtime_error("newlocale failed");
            }

            locale_t* tmp_p = 0;

            try {
                tmp_p = new locale_t();
            } catch(...) {
                freelocale(tmp);
                throw;
            }

            *tmp_p = tmp;
            lc_ = std::shared_ptr<locale_t>(tmp_p, free_locale_by_ptr);
        }

        std::locale install(const std::locale& base, category_t category, char_facet_t type) override
        {
            prepare_data();

            switch(category) {
                case category_t::convert: return create_convert(base, lc_, type);
                case category_t::collation: return create_collate(base, lc_, type);
                case category_t::formatting: return create_formatting(base, lc_, type);
                case category_t::parsing: return create_parsing(base, lc_, type);
                case category_t::codepage: return create_codecvt(base, nl_langinfo_l(CODESET, *lc_), type);
                case category_t::calendar: {
                    util::locale_data inf;
                    inf.parse(real_id_);
                    return util::install_gregorian_calendar(base, inf.country);
                }
                case category_t::message: {
                    gnu_gettext::messages_info minf;
                    util::locale_data inf;
                    inf.parse(real_id_);
                    minf.language = inf.language;
                    minf.country = inf.country;
                    minf.variant = inf.variant;
                    minf.encoding = inf.encoding;
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
        std::shared_ptr<locale_t> lc_;
    };

    localization_backend* create_localization_backend()
    {
        return new posix_localization_backend();
    }

}}} // namespace boost::locale::impl_posix
