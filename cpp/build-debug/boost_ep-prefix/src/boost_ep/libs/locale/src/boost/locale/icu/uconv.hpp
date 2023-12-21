//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_SRC_LOCALE_ICU_UCONV_HPP
#define BOOST_SRC_LOCALE_ICU_UCONV_HPP

#include <boost/locale/encoding.hpp>
#include "boost/locale/icu/icu_util.hpp"

#include <memory>
#include <string>
#include <unicode/ucnv.h>
#include <unicode/unistr.h>
#include <unicode/ustring.h>
#include <unicode/utf.h>
#include <unicode/utf16.h>

#ifdef BOOST_MSVC
#    pragma warning(push)
#    pragma warning(disable : 4244) // 'argument' : conversion from 'int'
#    pragma warning(disable : 4267) // 'argument' : conversion from 'size_t'
#endif

namespace boost { namespace locale { namespace impl_icu {

    enum class cpcvt_type { skip, stop };

    template<typename CharType, int char_size = sizeof(CharType)>
    class icu_std_converter {
    public:
        typedef CharType char_type;
        typedef std::basic_string<char_type> string_type;

        icu_std_converter(std::string charset, cpcvt_type cv = cpcvt_type::skip);
        icu::UnicodeString icu(const char_type* begin, const char_type* end) const;
        string_type std(const icu::UnicodeString& str) const;
        size_t cut(const icu::UnicodeString& str,
                   const char_type* begin,
                   const char_type* end,
                   size_t n,
                   size_t from_u = 0,
                   size_t from_c = 0) const;
    };

    template<typename CharType>
    class icu_std_converter<CharType, 1> {
    public:
        typedef CharType char_type;
        typedef std::basic_string<char_type> string_type;

        icu::UnicodeString icu_checked(const char_type* vb, const char_type* ve) const
        {
            return icu(vb, ve); // Already done
        }
        icu::UnicodeString icu(const char_type* vb, const char_type* ve) const
        {
            const char* begin = reinterpret_cast<const char*>(vb);
            const char* end = reinterpret_cast<const char*>(ve);
            uconv cvt(charset_, cvt_type_);
            UErrorCode err = U_ZERO_ERROR;
            icu::UnicodeString tmp(begin, end - begin, cvt.cvt(), err);
            check_and_throw_icu_error(err);
            return tmp;
        }

        string_type std(const icu::UnicodeString& str) const
        {
            uconv cvt(charset_, cvt_type_);
            return cvt.go(str.getBuffer(), str.length(), max_len_);
        }

        icu_std_converter(std::string charset, cpcvt_type cvt_type = cpcvt_type::skip) :
            charset_(charset), cvt_type_(cvt_type)
        {
            uconv cvt(charset_, cvt_type);
            max_len_ = cvt.max_char_size();
        }

        size_t cut(const icu::UnicodeString& str,
                   const char_type* begin,
                   const char_type* end,
                   size_t n,
                   size_t from_u = 0,
                   size_t from_char = 0) const
        {
            size_t code_points = str.countChar32(from_u, n);
            uconv cvt(charset_, cvt_type_);
            return cvt.cut(code_points, begin + from_char, end);
        }

        struct uconv {
            uconv(const uconv& other) = delete;
            void operator=(const uconv& other) = delete;

            uconv(const std::string& charset, cpcvt_type cvt_type = cpcvt_type::skip)
            {
                UErrorCode err = U_ZERO_ERROR;
                cvt_ = ucnv_open(charset.c_str(), &err);
                if(!cvt_ || U_FAILURE(err)) {
                    if(cvt_)
                        ucnv_close(cvt_);
                    throw conv::invalid_charset_error(charset);
                }

                try {
                    if(cvt_type == cpcvt_type::skip) {
                        ucnv_setFromUCallBack(cvt_, UCNV_FROM_U_CALLBACK_SKIP, 0, 0, 0, &err);
                        check_and_throw_icu_error(err);

                        err = U_ZERO_ERROR;
                        ucnv_setToUCallBack(cvt_, UCNV_TO_U_CALLBACK_SKIP, 0, 0, 0, &err);
                        check_and_throw_icu_error(err);
                    } else {
                        ucnv_setFromUCallBack(cvt_, UCNV_FROM_U_CALLBACK_STOP, 0, 0, 0, &err);
                        check_and_throw_icu_error(err);

                        err = U_ZERO_ERROR;
                        ucnv_setToUCallBack(cvt_, UCNV_TO_U_CALLBACK_STOP, 0, 0, 0, &err);
                        check_and_throw_icu_error(err);
                    }
                } catch(...) {
                    ucnv_close(cvt_);
                    throw;
                }
            }

            int max_char_size() { return ucnv_getMaxCharSize(cvt_); }

            string_type go(const UChar* buf, int length, int max_size)
            {
                string_type res;
                res.resize(UCNV_GET_MAX_BYTES_FOR_STRING(length, max_size));
                char* ptr = reinterpret_cast<char*>(&res[0]);
                UErrorCode err = U_ZERO_ERROR;
                int n = ucnv_fromUChars(cvt_, ptr, res.size(), buf, length, &err);
                check_and_throw_icu_error(err);
                res.resize(n);
                return res;
            }

            size_t cut(size_t n, const char_type* begin, const char_type* end)
            {
                const char_type* saved = begin;
                while(n > 0 && begin < end) {
                    UErrorCode err = U_ZERO_ERROR;
                    ucnv_getNextUChar(cvt_, &begin, end, &err);
                    if(U_FAILURE(err))
                        return 0;
                    n--;
                }
                return begin - saved;
            }

            UConverter* cvt() { return cvt_; }

            ~uconv() { ucnv_close(cvt_); }

        private:
            UConverter* cvt_;
        };

    private:
        int max_len_;
        std::string charset_;
        cpcvt_type cvt_type_;
    };

    template<typename CharType>
    class icu_std_converter<CharType, 2> {
    public:
        typedef CharType char_type;
        typedef std::basic_string<char_type> string_type;

        icu::UnicodeString icu_checked(const char_type* begin, const char_type* end) const
        {
            icu::UnicodeString tmp(end - begin, 0, 0); // make inital capacity
            while(begin != end) {
                UChar cl = *begin++;
                if(U16_IS_SINGLE(cl))
                    tmp.append(static_cast<UChar32>(cl));
                else if(U16_IS_LEAD(cl)) {
                    if(begin == end) {
                        throw_if_needed();
                    } else {
                        UChar ct = *begin++;
                        if(!U16_IS_TRAIL(ct))
                            throw_if_needed();
                        else {
                            UChar32 c = U16_GET_SUPPLEMENTARY(cl, ct);
                            tmp.append(c);
                        }
                    }
                } else
                    throw_if_needed();
            }
            return tmp;
        }
        void throw_if_needed() const
        {
            if(mode_ == cpcvt_type::stop)
                throw conv::conversion_error();
        }
        icu::UnicodeString icu(const char_type* vb, const char_type* ve) const
        {
            const UChar* begin = reinterpret_cast<const UChar*>(vb);
            const UChar* end = reinterpret_cast<const UChar*>(ve);
            icu::UnicodeString tmp(begin, end - begin);
            return tmp;
        }

        string_type std(const icu::UnicodeString& str) const
        {
            const char_type* ptr = reinterpret_cast<const char_type*>(str.getBuffer());
            return string_type(ptr, str.length());
        }
        size_t cut(const icu::UnicodeString& /*str*/,
                   const char_type* /*begin*/,
                   const char_type* /*end*/,
                   size_t n,
                   size_t /*from_u*/ = 0,
                   size_t /*from_c*/ = 0) const
        {
            return n;
        }

        icu_std_converter(std::string /*charset*/, cpcvt_type mode = cpcvt_type::skip) : mode_(mode) {}

    private:
        cpcvt_type mode_;
    };

    template<typename CharType>
    class icu_std_converter<CharType, 4> {
    public:
        typedef CharType char_type;
        typedef std::basic_string<char_type> string_type;

        icu::UnicodeString icu_checked(const char_type* begin, const char_type* end) const
        {
            icu::UnicodeString tmp(end - begin, 0, 0); // make inital capacity
            while(begin != end) {
                UChar32 c = static_cast<UChar32>(*begin++);
                if(U_IS_UNICODE_CHAR(c))
                    tmp.append(c);
                else
                    throw_if_needed();
            }
            return tmp;
        }
        void throw_if_needed() const
        {
            if(mode_ == cpcvt_type::stop)
                throw conv::conversion_error();
        }

        icu::UnicodeString icu(const char_type* begin, const char_type* end) const
        {
            icu::UnicodeString tmp(end - begin, 0, 0); // make inital capacity
            while(begin != end) {
                UChar32 c = static_cast<UChar32>(*begin++);
                tmp.append(c);
            }
            return tmp;
        }

        string_type std(const icu::UnicodeString& str) const
        {
            string_type tmp;
            tmp.resize(str.length());
            UChar32* ptr = reinterpret_cast<UChar32*>(&tmp[0]);
            int32_t len = 0;
            UErrorCode code = U_ZERO_ERROR;
            u_strToUTF32(ptr, tmp.size(), &len, str.getBuffer(), str.length(), &code);

            check_and_throw_icu_error(code);

            tmp.resize(len);

            return tmp;
        }

        size_t cut(const icu::UnicodeString& str,
                   const char_type* /*begin*/,
                   const char_type* /*end*/,
                   size_t n,
                   size_t from_u = 0,
                   size_t /*from_c*/ = 0) const
        {
            return str.countChar32(from_u, n);
        }

        icu_std_converter(std::string /*charset*/, cpcvt_type mode = cpcvt_type::skip) : mode_(mode) {}

    private:
        cpcvt_type mode_;
    };
}}} // namespace boost::locale::impl_icu

#endif

#ifdef BOOST_MSVC
#pragma warning(pop)
#endif
