//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#define BOOST_LOCALE_SOURCE
#include <boost/locale/conversion.hpp>
#include "boost/locale/icu/all_generator.hpp"
#include "boost/locale/icu/cdata.hpp"
#include "boost/locale/icu/icu_util.hpp"
#include "boost/locale/icu/uconv.hpp"
#include <limits>
#include <unicode/locid.h>
#include <unicode/normlzr.h>
#include <unicode/ustring.h>
#if BOOST_LOCALE_ICU_VERSION >= 308
#    include <unicode/ucasemap.h>
#    define BOOST_LOCALE_WITH_CASEMAP
#endif
#include <vector>

namespace boost { namespace locale { namespace impl_icu {

    namespace {
        void normalize_string(icu::UnicodeString& str, int flags)
        {
            UErrorCode code = U_ZERO_ERROR;
            UNormalizationMode mode = UNORM_DEFAULT;
            switch(flags) {
                case norm_nfd: mode = UNORM_NFD; break;
                case norm_nfc: mode = UNORM_NFC; break;
                case norm_nfkd: mode = UNORM_NFKD; break;
                case norm_nfkc: mode = UNORM_NFKC; break;
            }
            icu::UnicodeString tmp;
            icu::Normalizer::normalize(str, mode, 0, tmp, code);

            check_and_throw_icu_error(code);

            str = tmp;
        }
    } // namespace

    template<typename CharType>
    class converter_impl : public converter<CharType> {
    public:
        typedef CharType char_type;
        typedef std::basic_string<char_type> string_type;

        converter_impl(const cdata& d) : locale_(d.locale), encoding_(d.encoding) {}

        string_type convert(converter_base::conversion_type how,
                            const char_type* begin,
                            const char_type* end,
                            int flags = 0) const override
        {
            icu_std_converter<char_type> cvt(encoding_);
            icu::UnicodeString str = cvt.icu(begin, end);
            switch(how) {
                case converter_base::normalization: normalize_string(str, flags); break;
                case converter_base::upper_case: str.toUpper(locale_); break;
                case converter_base::lower_case: str.toLower(locale_); break;
                case converter_base::title_case: str.toTitle(0, locale_); break;
                case converter_base::case_folding: str.foldCase(); break;
            }
            return cvt.std(str);
        }

    private:
        icu::Locale locale_;
        std::string encoding_;
    }; // converter_impl

#ifdef BOOST_LOCALE_WITH_CASEMAP
    template<typename T>
    struct get_casemap_size_type;

    template<typename TRes, typename TCaseMap, typename TSize>
    struct get_casemap_size_type<TRes (*)(TCaseMap*, char*, TSize, const char*, TSize, UErrorCode*)> {
        using type = TSize;
    };

    class raii_casemap {
    public:
        raii_casemap(const raii_casemap&) = delete;
        void operator=(const raii_casemap&) = delete;

        raii_casemap(const std::string& locale_id) : map_(0)
        {
            UErrorCode err = U_ZERO_ERROR;
            map_ = ucasemap_open(locale_id.c_str(), 0, &err);
            check_and_throw_icu_error(err);
            if(!map_)
                throw std::runtime_error("Failed to create UCaseMap");
        }
        template<typename Conv>
        std::string convert(Conv func, const char* begin, const char* end) const
        {
            using size_type = typename get_casemap_size_type<Conv>::type;
            if((end - begin) >= std::numeric_limits<std::ptrdiff_t>::max() / 11)
                throw std::range_error("String to long to be converted by ICU");
            const auto max_converted_size = (end - begin) * 11 / 10 + 1;
            if(max_converted_size >= std::numeric_limits<size_type>::max())
                throw std::range_error("String to long to be converted by ICU");
            std::vector<char> buf(max_converted_size);
            UErrorCode err = U_ZERO_ERROR;
            auto size = func(map_,
                             &buf.front(),
                             static_cast<size_type>(buf.size()),
                             begin,
                             static_cast<size_type>(end - begin),
                             &err);
            if(err == U_BUFFER_OVERFLOW_ERROR) {
                err = U_ZERO_ERROR;
                buf.resize(size + 1);
                size = func(map_,
                            &buf.front(),
                            static_cast<size_type>(buf.size()),
                            begin,
                            static_cast<size_type>(end - begin),
                            &err);
            }
            check_and_throw_icu_error(err);
            return std::string(&buf.front(), size);
        }
        ~raii_casemap() { ucasemap_close(map_); }

    private:
        UCaseMap* map_;
    };

    class utf8_converter_impl : public converter<char> {
    public:
        utf8_converter_impl(const cdata& d) : locale_id_(d.locale.getName()), map_(locale_id_) {}

        std::string
        convert(converter_base::conversion_type how, const char* begin, const char* end, int flags = 0) const override
        {
            switch(how) {
                case converter_base::upper_case: return map_.convert(ucasemap_utf8ToUpper, begin, end);
                case converter_base::lower_case: return map_.convert(ucasemap_utf8ToLower, begin, end);
                case converter_base::title_case: {
                    // Non-const method, so need to create a separate map
                    raii_casemap map(locale_id_);
                    return map.convert(ucasemap_utf8ToTitle, begin, end);
                }
                case converter_base::case_folding: return map_.convert(ucasemap_utf8FoldCase, begin, end);
                case converter_base::normalization: {
                    icu_std_converter<char> cvt("UTF-8");
                    icu::UnicodeString str = cvt.icu(begin, end);
                    normalize_string(str, flags);
                    return cvt.std(str);
                }
            }
            return std::string(begin, end - begin);
        }

    private:
        std::string locale_id_;
        raii_casemap map_;
    }; // converter_impl

#endif // BOOST_LOCALE_WITH_CASEMAP

    std::locale create_convert(const std::locale& in, const cdata& cd, char_facet_t type)
    {
        switch(type) {
            case char_facet_t::nochar: break;
            case char_facet_t::char_f:
#ifdef BOOST_LOCALE_WITH_CASEMAP
                if(cd.utf8)
                    return std::locale(in, new utf8_converter_impl(cd));
#endif
                return std::locale(in, new converter_impl<char>(cd));
            case char_facet_t::wchar_f: return std::locale(in, new converter_impl<wchar_t>(cd));
#ifdef BOOST_LOCALE_ENABLE_CHAR16_T
            case char_facet_t::char16_f: return std::locale(in, new converter_impl<char16_t>(cd));
#endif
#ifdef BOOST_LOCALE_ENABLE_CHAR32_T
            case char_facet_t::char32_f: return std::locale(in, new converter_impl<char32_t>(cd));
#endif
        }
        return in;
    }

}}} // namespace boost::locale::impl_icu
