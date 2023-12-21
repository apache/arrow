//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#define BOOST_LOCALE_SOURCE

#include <boost/locale/conversion.hpp>
#include <boost/locale/encoding.hpp>
#include <boost/locale/generator.hpp>
#include "boost/locale/win32/all_generator.hpp"
#include "boost/locale/win32/api.hpp"
#include <cstring>
#include <locale>
#include <stdexcept>

namespace boost { namespace locale { namespace impl_win {

    class utf16_converter : public converter<wchar_t> {
    public:
        utf16_converter(const winlocale& lc, size_t refs = 0) : converter<wchar_t>(refs), lc_(lc) {}
        std::wstring convert(converter_base::conversion_type how,
                             const wchar_t* begin,
                             const wchar_t* end,
                             int flags = 0) const override
        {
            switch(how) {
                case converter_base::upper_case: return towupper_l(begin, end, lc_);
                case converter_base::lower_case: return towlower_l(begin, end, lc_);
                case converter_base::case_folding: return wcsfold(begin, end);
                case converter_base::normalization: return wcsnormalize(static_cast<norm_type>(flags), begin, end);
                case converter_base::title_case: break;
            }
            return std::wstring(begin, end - begin);
        }

    private:
        winlocale lc_;
    };

    class utf8_converter : public converter<char> {
    public:
        utf8_converter(const winlocale& lc, size_t refs = 0) : converter<char>(refs), lc_(lc) {}
        std::string
        convert(converter_base::conversion_type how, const char* begin, const char* end, int flags = 0) const override
        {
            std::wstring tmp = conv::to_utf<wchar_t>(begin, end, "UTF-8");
            const wchar_t* wb = tmp.c_str();
            const wchar_t* we = wb + tmp.size();

            std::wstring res;

            switch(how) {
                case upper_case: res = towupper_l(wb, we, lc_); break;
                case lower_case: res = towlower_l(wb, we, lc_); break;
                case case_folding: res = wcsfold(wb, we); break;
                case normalization: res = wcsnormalize(static_cast<norm_type>(flags), wb, we); break;
                case title_case: break;
            }
            return conv::from_utf(res, "UTF-8");
        }

    private:
        winlocale lc_;
    };

    std::locale create_convert(const std::locale& in, const winlocale& lc, char_facet_t type)
    {
        switch(type) {
            case char_facet_t::nochar: break;
            case char_facet_t::char_f: return std::locale(in, new utf8_converter(lc));
            case char_facet_t::wchar_f: return std::locale(in, new utf16_converter(lc));
#ifdef BOOST_LOCALE_ENABLE_CHAR16_T
            case char_facet_t::char16_f: break;
#endif
#ifdef BOOST_LOCALE_ENABLE_CHAR32_T
            case char_facet_t::char32_f: break;
#endif
        }
        return in;
    }

}}} // namespace boost::locale::impl_win
