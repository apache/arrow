//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#define BOOST_LOCALE_SOURCE
#include <boost/locale/encoding.hpp>
#include <boost/locale/formatting.hpp>
#include <boost/locale/generator.hpp>
#include <cstdlib>
#include <ios>
#include <locale>
#include <sstream>
#include <string>

#include "boost/locale/std/all_generator.hpp"
#include "boost/locale/util/numeric.hpp"

namespace boost { namespace locale { namespace impl_std {

    template<typename CharType>
    class time_put_from_base : public std::time_put<CharType> {
    public:
        time_put_from_base(const std::locale& base, size_t refs = 0) : std::time_put<CharType>(refs), base_(base) {}
        typedef typename std::time_put<CharType>::iter_type iter_type;

        iter_type do_put(iter_type out,
                         std::ios_base& /*ios*/,
                         CharType fill,
                         const std::tm* tm,
                         char format,
                         char modifier) const override
        {
            std::basic_stringstream<CharType> ss;
            ss.imbue(base_);
            return std::use_facet<std::time_put<CharType>>(base_).put(out, ss, fill, tm, format, modifier);
        }

    private:
        std::locale base_;
    };

    class utf8_time_put_from_wide : public std::time_put<char> {
    public:
        utf8_time_put_from_wide(const std::locale& base, size_t refs = 0) : std::time_put<char>(refs), base_(base) {}
        iter_type do_put(iter_type out,
                         std::ios_base& /*ios*/,
                         char fill,
                         const std::tm* tm,
                         char format,
                         char modifier = 0) const override
        {
            std::basic_ostringstream<wchar_t> wtmps;
            wtmps.imbue(base_);
            std::use_facet<std::time_put<wchar_t>>(base_)
              .put(wtmps, wtmps, wchar_t(fill), tm, wchar_t(format), wchar_t(modifier));
            std::wstring wtmp = wtmps.str();
            std::string const tmp = conv::from_utf<wchar_t>(wtmp, "UTF-8");
            for(unsigned i = 0; i < tmp.size(); i++) {
                *out++ = tmp[i];
            }
            return out;
        }

    private:
        std::locale base_;
    };

    class utf8_numpunct_from_wide : public std::numpunct<char> {
    public:
        utf8_numpunct_from_wide(const std::locale& base, size_t refs = 0) : std::numpunct<char>(refs)
        {
            typedef std::numpunct<wchar_t> wfacet_type;
            const wfacet_type& wfacet = std::use_facet<wfacet_type>(base);

            truename_ = conv::from_utf<wchar_t>(wfacet.truename(), "UTF-8");
            falsename_ = conv::from_utf<wchar_t>(wfacet.falsename(), "UTF-8");

            wchar_t tmp_decimal_point = wfacet.decimal_point();
            wchar_t tmp_thousands_sep = wfacet.thousands_sep();
            std::string tmp_grouping = wfacet.grouping();

            if(32 <= tmp_thousands_sep && tmp_thousands_sep <= 126 && 32 <= tmp_decimal_point
               && tmp_decimal_point <= 126) {
                thousands_sep_ = static_cast<char>(tmp_thousands_sep);
                decimal_point_ = static_cast<char>(tmp_decimal_point);
                grouping_ = tmp_grouping;
            } else if(32 <= tmp_decimal_point && tmp_decimal_point <= 126 && tmp_thousands_sep == 0xA0) {
                // workaround common bug - substitute NBSP with ordinary space
                thousands_sep_ = ' ';
                decimal_point_ = static_cast<char>(tmp_decimal_point);
                grouping_ = tmp_grouping;
            } else if(32 <= tmp_decimal_point && tmp_decimal_point <= 126) {
                thousands_sep_ = ',';
                decimal_point_ = static_cast<char>(tmp_decimal_point);
                grouping_ = std::string();
            } else {
                thousands_sep_ = ',';
                decimal_point_ = '.';
                grouping_ = std::string();
            }
        }

        char do_decimal_point() const override { return decimal_point_; }
        char do_thousands_sep() const override { return thousands_sep_; }
        std::string do_grouping() const override { return grouping_; }
        std::string do_truename() const override { return truename_; }
        std::string do_falsename() const override { return falsename_; }

    private:
        std::string truename_;
        std::string falsename_;
        char thousands_sep_;
        char decimal_point_;
        std::string grouping_;
    };

    template<bool Intl>
    class utf8_moneypunct_from_wide : public std::moneypunct<char, Intl> {
    public:
        utf8_moneypunct_from_wide(const std::locale& base, size_t refs = 0) : std::moneypunct<char, Intl>(refs)
        {
            typedef std::moneypunct<wchar_t, Intl> wfacet_type;
            const wfacet_type& wfacet = std::use_facet<wfacet_type>(base);

            curr_symbol_ = conv::from_utf<wchar_t>(wfacet.curr_symbol(), "UTF-8");
            positive_sign_ = conv::from_utf<wchar_t>(wfacet.positive_sign(), "UTF-8");
            negative_sign_ = conv::from_utf<wchar_t>(wfacet.negative_sign(), "UTF-8");
            frac_digits_ = wfacet.frac_digits();
            pos_format_ = wfacet.pos_format();
            neg_format_ = wfacet.neg_format();

            wchar_t tmp_decimal_point = wfacet.decimal_point();
            wchar_t tmp_thousands_sep = wfacet.thousands_sep();
            std::string tmp_grouping = wfacet.grouping();
            if(32 <= tmp_thousands_sep && tmp_thousands_sep <= 126 && 32 <= tmp_decimal_point
               && tmp_decimal_point <= 126) {
                thousands_sep_ = static_cast<char>(tmp_thousands_sep);
                decimal_point_ = static_cast<char>(tmp_decimal_point);
                grouping_ = tmp_grouping;
            } else if(32 <= tmp_decimal_point && tmp_decimal_point <= 126 && tmp_thousands_sep == 0xA0) {
                // workaround common bug - substitute NBSP with ordinary space
                thousands_sep_ = ' ';
                decimal_point_ = static_cast<char>(tmp_decimal_point);
                grouping_ = tmp_grouping;
            } else if(32 <= tmp_decimal_point && tmp_decimal_point <= 126) {
                thousands_sep_ = ',';
                decimal_point_ = static_cast<char>(tmp_decimal_point);
                grouping_ = std::string();
            } else {
                thousands_sep_ = ',';
                decimal_point_ = '.';
                grouping_ = std::string();
            }
        }

        char do_decimal_point() const override { return decimal_point_; }

        char do_thousands_sep() const override { return thousands_sep_; }

        std::string do_grouping() const override { return grouping_; }

        std::string do_curr_symbol() const override { return curr_symbol_; }
        std::string do_positive_sign() const override { return positive_sign_; }
        std::string do_negative_sign() const override { return negative_sign_; }

        int do_frac_digits() const override { return frac_digits_; }

        std::money_base::pattern do_pos_format() const override { return pos_format_; }

        std::money_base::pattern do_neg_format() const override { return neg_format_; }

    private:
        char thousands_sep_;
        char decimal_point_;
        std::string grouping_;
        std::string curr_symbol_;
        std::string positive_sign_;
        std::string negative_sign_;
        int frac_digits_;
        std::money_base::pattern pos_format_, neg_format_;
    };

    class utf8_numpunct : public std::numpunct_byname<char> {
    public:
        typedef std::numpunct_byname<char> base_type;
        utf8_numpunct(const char* name, size_t refs = 0) : std::numpunct_byname<char>(name, refs) {}
        char do_thousands_sep() const override
        {
            unsigned char bs = base_type::do_thousands_sep();
            if(bs > 127)
                if(bs == 0xA0)
                    return ' ';
                else
                    return 0;
            else
                return bs;
        }
        std::string do_grouping() const override
        {
            unsigned char bs = base_type::do_thousands_sep();
            if(bs > 127 && bs != 0xA0)
                return std::string();
            return base_type::do_grouping();
        }
    };

    template<bool Intl>
    class utf8_moneypunct : public std::moneypunct_byname<char, Intl> {
    public:
        typedef std::moneypunct_byname<char, Intl> base_type;
        utf8_moneypunct(const char* name, size_t refs = 0) : std::moneypunct_byname<char, Intl>(name, refs) {}
        char do_thousands_sep() const override
        {
            unsigned char bs = base_type::do_thousands_sep();
            if(bs > 127)
                if(bs == 0xA0)
                    return ' ';
                else
                    return 0;
            else
                return bs;
        }
        std::string do_grouping() const override
        {
            unsigned char bs = base_type::do_thousands_sep();
            if(bs > 127 && bs != 0xA0)
                return std::string();
            return base_type::do_grouping();
        }
    };

    template<typename CharType>
    std::locale create_basic_parsing(const std::locale& in, const std::string& locale_name)
    {
        std::locale tmp = std::locale(in, new std::numpunct_byname<CharType>(locale_name.c_str()));
        tmp = std::locale(tmp, new std::moneypunct_byname<CharType, true>(locale_name.c_str()));
        tmp = std::locale(tmp, new std::moneypunct_byname<CharType, false>(locale_name.c_str()));
        tmp = std::locale(tmp, new std::ctype_byname<CharType>(locale_name.c_str()));
        return tmp;
    }

    template<typename CharType>
    std::locale create_basic_formatting(const std::locale& in, const std::string& locale_name)
    {
        std::locale tmp = create_basic_parsing<CharType>(in, locale_name);
        std::locale base(locale_name.c_str());
        tmp = std::locale(tmp, new time_put_from_base<CharType>(base));
        return tmp;
    }

    std::locale
    create_formatting(const std::locale& in, const std::string& locale_name, char_facet_t type, utf8_support utf)
    {
        switch(type) {
            case char_facet_t::nochar: break;
            case char_facet_t::char_f: {
                switch(utf) {
                    case utf8_support::from_wide: {
                        std::locale base = std::locale(locale_name.c_str());

                        std::locale tmp = std::locale(in, new utf8_time_put_from_wide(base));
                        tmp = std::locale(tmp, new utf8_numpunct_from_wide(base));
                        tmp = std::locale(tmp, new utf8_moneypunct_from_wide<true>(base));
                        tmp = std::locale(tmp, new utf8_moneypunct_from_wide<false>(base));
                        return std::locale(tmp, new util::base_num_format<char>());
                    }
                    case utf8_support::native: {
                        std::locale base = std::locale(locale_name.c_str());

                        std::locale tmp = std::locale(in, new time_put_from_base<char>(base));
                        tmp = std::locale(tmp, new utf8_numpunct(locale_name.c_str()));
                        tmp = std::locale(tmp, new utf8_moneypunct<true>(locale_name.c_str()));
                        tmp = std::locale(tmp, new utf8_moneypunct<false>(locale_name.c_str()));
                        return std::locale(tmp, new util::base_num_format<char>());
                    }
                    case utf8_support::native_with_wide: {
                        std::locale base = std::locale(locale_name.c_str());

                        std::locale tmp = std::locale(in, new time_put_from_base<char>(base));
                        tmp = std::locale(tmp, new utf8_numpunct_from_wide(base));
                        tmp = std::locale(tmp, new utf8_moneypunct_from_wide<true>(base));
                        tmp = std::locale(tmp, new utf8_moneypunct_from_wide<false>(base));
                        return std::locale(tmp, new util::base_num_format<char>());
                    }
                    case utf8_support::none: break;
                }
                std::locale tmp = create_basic_formatting<char>(in, locale_name);
                tmp = std::locale(tmp, new util::base_num_format<char>());
                return tmp;
            }
            case char_facet_t::wchar_f: {
                std::locale tmp = create_basic_formatting<wchar_t>(in, locale_name);
                tmp = std::locale(tmp, new util::base_num_format<wchar_t>());
                return tmp;
            }
#ifdef BOOST_LOCALE_ENABLE_CHAR16_T
            case char_facet_t::char16_f: {
                std::locale tmp = create_basic_formatting<char16_t>(in, locale_name);
                tmp = std::locale(tmp, new util::base_num_format<char16_t>());
                return tmp;
            }
#endif
#ifdef BOOST_LOCALE_ENABLE_CHAR32_T
            case char_facet_t::char32_f: {
                std::locale tmp = create_basic_formatting<char32_t>(in, locale_name);
                tmp = std::locale(tmp, new util::base_num_format<char32_t>());
                return tmp;
            }
#endif
        }
        return in;
    }

    std::locale
    create_parsing(const std::locale& in, const std::string& locale_name, char_facet_t type, utf8_support utf)
    {
        switch(type) {
            case char_facet_t::nochar: break;
            case char_facet_t::char_f: {
                switch(utf) {
                    case utf8_support::from_wide: {
                        std::locale base = std::locale::classic();

                        base = std::locale(base, new std::numpunct_byname<wchar_t>(locale_name.c_str()));
                        base = std::locale(base, new std::moneypunct_byname<wchar_t, true>(locale_name.c_str()));
                        base = std::locale(base, new std::moneypunct_byname<wchar_t, false>(locale_name.c_str()));

                        std::locale tmp = std::locale(in, new utf8_numpunct_from_wide(base));
                        tmp = std::locale(tmp, new utf8_moneypunct_from_wide<true>(base));
                        tmp = std::locale(tmp, new utf8_moneypunct_from_wide<false>(base));
                        return std::locale(tmp, new util::base_num_parse<char>());
                    }
                    case utf8_support::native: {
                        std::locale tmp = std::locale(in, new utf8_numpunct(locale_name.c_str()));
                        tmp = std::locale(tmp, new utf8_moneypunct<true>(locale_name.c_str()));
                        tmp = std::locale(tmp, new utf8_moneypunct<false>(locale_name.c_str()));
                        return std::locale(tmp, new util::base_num_parse<char>());
                    }
                    case utf8_support::native_with_wide: {
                        std::locale base = std::locale(locale_name.c_str());

                        std::locale tmp = std::locale(in, new utf8_numpunct_from_wide(base));
                        tmp = std::locale(tmp, new utf8_moneypunct_from_wide<true>(base));
                        tmp = std::locale(tmp, new utf8_moneypunct_from_wide<false>(base));
                        return std::locale(tmp, new util::base_num_parse<char>());
                    }
                    case utf8_support::none: break;
                }
                std::locale tmp = create_basic_parsing<char>(in, locale_name);
                tmp = std::locale(in, new util::base_num_parse<char>());
                return tmp;
            }
            case char_facet_t::wchar_f: {
                std::locale tmp = create_basic_parsing<wchar_t>(in, locale_name);
                tmp = std::locale(in, new util::base_num_parse<wchar_t>());
                return tmp;
            }
#ifdef BOOST_LOCALE_ENABLE_CHAR16_T
            case char_facet_t::char16_f: {
                std::locale tmp = create_basic_parsing<char16_t>(in, locale_name);
                tmp = std::locale(in, new util::base_num_parse<char16_t>());
                return tmp;
            }
#endif
#ifdef BOOST_LOCALE_ENABLE_CHAR32_T
            case char_facet_t::char32_f: {
                std::locale tmp = create_basic_parsing<char32_t>(in, locale_name);
                tmp = std::locale(in, new util::base_num_parse<char32_t>());
                return tmp;
            }
#endif
        }
        return in;
    }

}}} // namespace boost::locale::impl_std
