//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_LOCALE_CONV_IMPL_HPP
#define BOOST_LOCALE_CONV_IMPL_HPP

#include <boost/locale/config.hpp>
#include <boost/locale/encoding.hpp>

namespace boost { namespace locale { namespace conv { namespace impl {

    template<typename CharType>
    const char* utf_name()
    {
        switch(sizeof(CharType)) {
            case 1: return "UTF-8";
            case 2: {
                const int16_t endianMark = 1;
                const bool isLittleEndian = reinterpret_cast<const char*>(&endianMark)[0] == 1;
                return isLittleEndian ? "UTF-16LE" : "UTF-16BE";
            }
            case 4: {
                const int32_t endianMark = 1;
                const bool isLittleEndian = reinterpret_cast<const char*>(&endianMark)[0] == 1;
                return isLittleEndian ? "UTF-32LE" : "UTF-32BE";
            }
        }
        return "Unknown Character Encoding";
    }

    BOOST_LOCALE_DECL std::string normalize_encoding(const char* encoding);

    inline int compare_encodings(const char* l, const char* r)
    {
        return normalize_encoding(l).compare(normalize_encoding(r));
    }

#if defined(BOOST_WINDOWS) || defined(__CYGWIN__)
    int encoding_to_windows_codepage(const char* ccharset);
#endif

    class converter_between {
    public:
        typedef char char_type;
        typedef std::string string_type;

        virtual bool open(const char* to_charset, const char* from_charset, method_type how) = 0;

        virtual std::string convert(const char* begin, const char* end) = 0;

        virtual ~converter_between() = default;
    };

    template<typename CharType>
    class converter_from_utf {
    public:
        typedef CharType char_type;
        typedef std::basic_string<char_type> string_type;

        virtual bool open(const char* charset, method_type how) = 0;

        virtual std::string convert(const CharType* begin, const CharType* end) = 0;

        virtual ~converter_from_utf() = default;
    };

    template<typename CharType>
    class converter_to_utf {
    public:
        typedef CharType char_type;
        typedef std::basic_string<char_type> string_type;

        virtual bool open(const char* charset, method_type how) = 0;

        virtual string_type convert(const char* begin, const char* end) = 0;

        virtual ~converter_to_utf() = default;
    };
}}}} // namespace boost::locale::conv::impl

#endif
