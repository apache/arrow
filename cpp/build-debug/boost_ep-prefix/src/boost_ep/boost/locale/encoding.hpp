//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_LOCALE_ENCODING_HPP_INCLUDED
#define BOOST_LOCALE_ENCODING_HPP_INCLUDED

#include <boost/locale/config.hpp>
#include <boost/locale/encoding_errors.hpp>
#include <boost/locale/encoding_utf.hpp>
#include <boost/locale/info.hpp>
#include <boost/locale/util/string.hpp>

#ifdef BOOST_MSVC
#    pragma warning(push)
#    pragma warning(disable : 4275 4251 4231 4660)
#endif

namespace boost { namespace locale {

    /// \brief Namespace that contains all functions related to character set conversion
    namespace conv {
        /// \defgroup codepage Character conversion functions
        ///
        /// @{

        /// convert text in range [begin,end) encoded with \a charset to UTF string according to policy \a how
        template<typename CharType>
        std::basic_string<CharType>
        to_utf(const char* begin, const char* end, const std::string& charset, method_type how = default_method);

        /// convert UTF text in range [begin,end) to a text encoded with \a charset according to policy \a how
        template<typename CharType>
        std::string from_utf(const CharType* begin,
                             const CharType* end,
                             const std::string& charset,
                             method_type how = default_method);

        /// convert string to UTF string from text in range [begin,end) encoded according to locale \a loc according to
        /// policy \a how
        ///
        /// \note throws std::bad_cast if the loc does not have \ref info facet installed
        template<typename CharType>
        std::basic_string<CharType>
        to_utf(const char* begin, const char* end, const std::locale& loc, method_type how = default_method)
        {
            return to_utf<CharType>(begin, end, std::use_facet<info>(loc).encoding(), how);
        }

        /// convert UTF text in range [begin,end) to a text encoded according to locale \a loc according to policy \a
        /// how
        ///
        /// \note throws std::bad_cast if the loc does not have \ref info facet installed
        template<typename CharType>
        std::string
        from_utf(const CharType* begin, const CharType* end, const std::locale& loc, method_type how = default_method)
        {
            return from_utf(begin, end, std::use_facet<info>(loc).encoding(), how);
        }

        /// convert a string \a text encoded with \a charset to UTF string
        template<typename CharType>
        std::basic_string<CharType>
        to_utf(const std::string& text, const std::string& charset, method_type how = default_method)
        {
            return to_utf<CharType>(text.c_str(), text.c_str() + text.size(), charset, how);
        }

        /// Convert a \a text from \a charset to UTF string
        template<typename CharType>
        std::string
        from_utf(const std::basic_string<CharType>& text, const std::string& charset, method_type how = default_method)
        {
            return from_utf(text.c_str(), text.c_str() + text.size(), charset, how);
        }

        /// Convert a \a text from \a charset to UTF string
        template<typename CharType>
        std::basic_string<CharType>
        to_utf(const char* text, const std::string& charset, method_type how = default_method)
        {
            return to_utf<CharType>(text, util::str_end(text), charset, how);
        }

        /// Convert a \a text from UTF to \a charset
        template<typename CharType>
        std::string from_utf(const CharType* text, const std::string& charset, method_type how = default_method)
        {
            return from_utf(text, util::str_end(text), charset, how);
        }

        /// Convert a \a text in locale encoding given by \a loc to UTF
        ///
        /// \note throws std::bad_cast if the loc does not have \ref info facet installed
        template<typename CharType>
        std::basic_string<CharType>
        to_utf(const std::string& text, const std::locale& loc, method_type how = default_method)
        {
            return to_utf<CharType>(text.c_str(), text.c_str() + text.size(), loc, how);
        }

        /// Convert a \a text in UTF to locale encoding given by \a loc
        ///
        /// \note throws std::bad_cast if the loc does not have \ref info facet installed
        template<typename CharType>
        std::string
        from_utf(const std::basic_string<CharType>& text, const std::locale& loc, method_type how = default_method)
        {
            return from_utf(text.c_str(), text.c_str() + text.size(), loc, how);
        }

        /// Convert a \a text in locale encoding given by \a loc to UTF
        ///
        /// \note throws std::bad_cast if the loc does not have \ref info facet installed
        template<typename CharType>
        std::basic_string<CharType> to_utf(const char* text, const std::locale& loc, method_type how = default_method)
        {
            return to_utf<CharType>(text, util::str_end(text), loc, how);
        }

        /// Convert a \a text in UTF to locale encoding given by \a loc
        ///
        /// \note throws std::bad_cast if the loc does not have \ref info facet installed
        template<typename CharType>
        std::string from_utf(const CharType* text, const std::locale& loc, method_type how = default_method)
        {
            return from_utf(text, util::str_end(text), loc, how);
        }

        /// Convert a text in range [begin,end) to \a to_encoding from \a from_encoding
        BOOST_LOCALE_DECL
        std::string between(const char* begin,
                            const char* end,
                            const std::string& to_encoding,
                            const std::string& from_encoding,
                            method_type how = default_method);

        /// Convert a \a text to \a to_encoding from \a from_encoding
        inline std::string between(const char* text,
                                   const std::string& to_encoding,
                                   const std::string& from_encoding,
                                   method_type how = default_method)
        {
            return boost::locale::conv::between(text, util::str_end(text), to_encoding, from_encoding, how);
        }

        /// Convert a \a text to \a to_encoding from \a from_encoding
        inline std::string between(const std::string& text,
                                   const std::string& to_encoding,
                                   const std::string& from_encoding,
                                   method_type how = default_method)
        {
            return boost::locale::conv::between(text.c_str(),
                                                text.c_str() + text.size(),
                                                to_encoding,
                                                from_encoding,
                                                how);
        }

        /// \cond INTERNAL

        template<>
        BOOST_LOCALE_DECL std::basic_string<char>
        to_utf(const char* begin, const char* end, const std::string& charset, method_type how);

        template<>
        BOOST_LOCALE_DECL std::string
        from_utf(const char* begin, const char* end, const std::string& charset, method_type how);

        template<>
        BOOST_LOCALE_DECL std::basic_string<wchar_t>
        to_utf(const char* begin, const char* end, const std::string& charset, method_type how);

        template<>
        BOOST_LOCALE_DECL std::string
        from_utf(const wchar_t* begin, const wchar_t* end, const std::string& charset, method_type how);

#ifdef BOOST_LOCALE_ENABLE_CHAR16_T
        template<>
        BOOST_LOCALE_DECL std::basic_string<char16_t>
        to_utf(const char* begin, const char* end, const std::string& charset, method_type how);

        template<>
        BOOST_LOCALE_DECL std::string
        from_utf(const char16_t* begin, const char16_t* end, const std::string& charset, method_type how);
#endif

#ifdef BOOST_LOCALE_ENABLE_CHAR32_T
        template<>
        BOOST_LOCALE_DECL std::basic_string<char32_t>
        to_utf(const char* begin, const char* end, const std::string& charset, method_type how);

        template<>
        BOOST_LOCALE_DECL std::string
        from_utf(const char32_t* begin, const char32_t* end, const std::string& charset, method_type how);
#endif

        /// \endcond

        /// @}

    } // namespace conv

}} // namespace boost::locale

#ifdef BOOST_MSVC
#    pragma warning(pop)
#endif

#endif
