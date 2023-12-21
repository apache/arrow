//  filesystem path.hpp  ---------------------------------------------------------------//

//  Copyright Vladimir Prus 2002
//  Copyright Beman Dawes 2002-2005, 2009
//  Copyright Andrey Semashev 2021

//  Distributed under the Boost Software License, Version 1.0.
//  See http://www.boost.org/LICENSE_1_0.txt

//  Library home page: http://www.boost.org/libs/filesystem

//  path::stem(), extension(), and replace_extension() are based on
//  basename(), extension(), and change_extension() from the original
//  filesystem/convenience.hpp header by Vladimir Prus.

#ifndef BOOST_FILESYSTEM_PATH_HPP
#define BOOST_FILESYSTEM_PATH_HPP

#include <boost/assert.hpp>
#include <boost/filesystem/config.hpp>
#include <boost/filesystem/detail/path_traits.hpp>
#include <boost/iterator/iterator_facade.hpp>
#include <boost/iterator/iterator_categories.hpp>
#include <boost/core/enable_if.hpp>
#include <boost/io/quoted.hpp>
#include <boost/functional/hash_fwd.hpp>
#include <boost/type_traits/negation.hpp>
#include <boost/type_traits/conjunction.hpp>
#include <boost/type_traits/disjunction.hpp>
#include <boost/type_traits/is_same.hpp>
#include <boost/type_traits/remove_cv.hpp>
#include <cstddef>
#include <iosfwd>
#include <locale>
#include <string>
#include <iterator>
#if !defined(BOOST_NO_CXX17_HDR_STRING_VIEW)
#include <string_view>
#endif

#include <boost/filesystem/detail/header.hpp> // must be the last #include

namespace boost {
namespace filesystem {

namespace path_detail { // intentionally don't use filesystem::detail to not bring internal Boost.Filesystem functions into ADL via path_constants

template< typename Char, Char Separator, Char PreferredSeparator, Char Dot >
struct path_constants
{
    typedef path_constants< Char, Separator, PreferredSeparator, Dot > path_constants_base;
    typedef Char value_type;
    static BOOST_CONSTEXPR_OR_CONST value_type separator = Separator;
    static BOOST_CONSTEXPR_OR_CONST value_type preferred_separator = PreferredSeparator;
    static BOOST_CONSTEXPR_OR_CONST value_type dot = Dot;
};

#if defined(BOOST_NO_CXX17_INLINE_VARIABLES)
template< typename Char, Char Separator, Char PreferredSeparator, Char Dot >
BOOST_CONSTEXPR_OR_CONST typename path_constants< Char, Separator, PreferredSeparator, Dot >::value_type
path_constants< Char, Separator, PreferredSeparator, Dot >::separator;
template< typename Char, Char Separator, Char PreferredSeparator, Char Dot >
BOOST_CONSTEXPR_OR_CONST typename path_constants< Char, Separator, PreferredSeparator, Dot >::value_type
path_constants< Char, Separator, PreferredSeparator, Dot >::preferred_separator;
template< typename Char, Char Separator, Char PreferredSeparator, Char Dot >
BOOST_CONSTEXPR_OR_CONST typename path_constants< Char, Separator, PreferredSeparator, Dot >::value_type
path_constants< Char, Separator, PreferredSeparator, Dot >::dot;
#endif

// A struct that denotes a contiguous range of characters in a string. A lightweight alternative to string_view.
struct substring
{
    std::size_t pos;
    std::size_t size;
};

} // namespace path_detail

//------------------------------------------------------------------------------------//
//                                                                                    //
//                                    class path                                      //
//                                                                                    //
//------------------------------------------------------------------------------------//

class path :
    public filesystem::path_detail::path_constants<
#ifdef BOOST_WINDOWS_API
        detail::path_traits::path_native_char_type, L'/', L'\\', L'.'
#else
        detail::path_traits::path_native_char_type, '/', '/', '.'
#endif
    >
{
public:
    //  value_type is the character type used by the operating system API to
    //  represent paths.

    typedef path_constants_base::value_type value_type;
    typedef std::basic_string< value_type > string_type;
    typedef detail::path_traits::codecvt_type codecvt_type;

    //  ----- character encoding conversions -----

    //  Following the principle of least astonishment, path input arguments
    //  passed to or obtained from the operating system via objects of
    //  class path behave as if they were directly passed to or
    //  obtained from the O/S API, unless conversion is explicitly requested.
    //
    //  POSIX specfies that path strings are passed unchanged to and from the
    //  API. Note that this is different from the POSIX command line utilities,
    //  which convert according to a locale.
    //
    //  Thus for POSIX, char strings do not undergo conversion.  wchar_t strings
    //  are converted to/from char using the path locale or, if a conversion
    //  argument is given, using a conversion object modeled on
    //  std::wstring_convert.
    //
    //  The path locale, which is global to the thread, can be changed by the
    //  imbue() function. It is initialized to an implementation defined locale.
    //
    //  For Windows, wchar_t strings do not undergo conversion. char strings
    //  are converted using the "ANSI" or "OEM" code pages, as determined by
    //  the AreFileApisANSI() function, or, if a conversion argument is given,
    //  using a conversion object modeled on std::wstring_convert.
    //
    //  See m_pathname comments for further important rationale.

    //  TODO: rules needed for operating systems that use / or .
    //  differently, or format directory paths differently from file paths.
    //
    //  **********************************************************************************
    //
    //  More work needed: How to handle an operating system that may have
    //  slash characters or dot characters in valid filenames, either because
    //  it doesn't follow the POSIX standard, or because it allows MBCS
    //  filename encodings that may contain slash or dot characters. For
    //  example, ISO/IEC 2022 (JIS) encoding which allows switching to
    //  JIS x0208-1983 encoding. A valid filename in this set of encodings is
    //  0x1B 0x24 0x42 [switch to X0208-1983] 0x24 0x2F [U+304F Kiragana letter KU]
    //                                             ^^^^
    //  Note that 0x2F is the ASCII slash character
    //
    //  **********************************************************************************

    //  Supported source arguments: half-open iterator range, container, c-array,
    //  and single pointer to null terminated string.

    //  All source arguments except pointers to null terminated byte strings support
    //  multi-byte character strings which may have embedded nulls. Embedded null
    //  support is required for some Asian languages on Windows.

    //  "const codecvt_type& cvt=codecvt()" default arguments are not used because this
    //  limits the impact of locale("") initialization failures on POSIX systems to programs
    //  that actually depend on locale(""). It further ensures that exceptions thrown
    //  as a result of such failues occur after main() has started, so can be caught.

private:
    //! Assignment operation
    class assign_op
    {
    private:
        path& m_self;

    public:
        typedef void result_type;

        explicit assign_op(path& self) BOOST_NOEXCEPT : m_self(self) {}

        result_type operator() (const value_type* source, const value_type* source_end, const codecvt_type* = NULL) const
        {
            m_self.m_pathname.assign(source, source_end);
        }

        template< typename OtherChar >
        result_type operator() (const OtherChar* source, const OtherChar* source_end, const codecvt_type* cvt = NULL) const
        {
            m_self.m_pathname.clear();
            detail::path_traits::convert(source, source_end, m_self.m_pathname, cvt);
        }
    };

    //! Concatenation operation
    class concat_op
    {
    private:
        path& m_self;

    public:
        typedef void result_type;

        explicit concat_op(path& self) BOOST_NOEXCEPT : m_self(self) {}

        result_type operator() (const value_type* source, const value_type* source_end, const codecvt_type* = NULL) const
        {
            m_self.m_pathname.append(source, source_end);
        }

        template< typename OtherChar >
        result_type operator() (const OtherChar* source, const OtherChar* source_end, const codecvt_type* cvt = NULL) const
        {
            detail::path_traits::convert(source, source_end, m_self.m_pathname, cvt);
        }
    };

    //! Path appending operation
    class append_op
    {
    private:
        path& m_self;

    public:
        typedef void result_type;

        explicit append_op(path& self) BOOST_NOEXCEPT : m_self(self) {}

        BOOST_FORCEINLINE result_type operator() (const value_type* source, const value_type* source_end, const codecvt_type* = NULL) const
        {
            m_self.append(source, source_end);
        }

        template< typename OtherChar >
        BOOST_FORCEINLINE result_type operator() (const OtherChar* source, const OtherChar* source_end, const codecvt_type* cvt = NULL) const
        {
            string_type src;
            detail::path_traits::convert(source, source_end, src, cvt);
            m_self.append(src.data(), src.data() + src.size());
        }
    };

public:
    class iterator;
    friend class iterator;
    typedef iterator const_iterator;
    class reverse_iterator;
    typedef reverse_iterator const_reverse_iterator;

public:
    //  -----  constructors  -----

    path() BOOST_NOEXCEPT {}
    path(path const& p) : m_pathname(p.m_pathname) {}
    path(path const& p, codecvt_type const&) : m_pathname(p.m_pathname) {}

    path(const value_type* s) : m_pathname(s) {}
    path(const value_type* s, codecvt_type const&) : m_pathname(s) {}
    path(string_type const& s) : m_pathname(s) {}
    path(string_type const& s, codecvt_type const&) : m_pathname(s) {}
#if !defined(BOOST_NO_CXX17_HDR_STRING_VIEW)
    path(std::basic_string_view< value_type > const& s) : m_pathname(s) {}
    path(std::basic_string_view< value_type > const& s, codecvt_type const&) : m_pathname(s) {}
#endif

#if !defined(BOOST_NO_CXX11_FUNCTION_TEMPLATE_DEFAULT_ARGS)
    template<
        typename Source,
        typename = typename boost::enable_if_c<
            boost::conjunction<
                boost::disjunction<
                    detail::path_traits::is_path_source< typename boost::remove_cv< Source >::type >,
                    detail::path_traits::is_convertible_to_path_source< typename boost::remove_cv< Source >::type >
                >,
                boost::negation< detail::path_traits::is_native_path_source< typename boost::remove_cv< Source >::type > >
            >::value
        >::type
    >
    path(Source const& source)
#else
    template< typename Source >
    path(Source const& source, typename boost::enable_if_c<
        boost::conjunction<
            boost::disjunction<
                detail::path_traits::is_path_source< typename boost::remove_cv< Source >::type >,
                detail::path_traits::is_convertible_to_path_source< typename boost::remove_cv< Source >::type >
            >,
            boost::negation< detail::path_traits::is_native_path_source< typename boost::remove_cv< Source >::type > >
        >::value
    >::type* = NULL)
#endif
    {
        assign(source);
    }

#if !defined(BOOST_NO_CXX11_FUNCTION_TEMPLATE_DEFAULT_ARGS)
    template<
        typename Source,
        typename = typename boost::enable_if_c<
            boost::conjunction<
                boost::disjunction<
                    detail::path_traits::is_path_source< typename boost::remove_cv< Source >::type >,
                    detail::path_traits::is_convertible_to_path_source< typename boost::remove_cv< Source >::type >
                >,
                boost::negation< detail::path_traits::is_native_path_source< typename boost::remove_cv< Source >::type > >
            >::value
        >::type
    >
    path(Source const& source, codecvt_type const& cvt)
#else
    template< typename Source >
    path(Source const& source, codecvt_type const& cvt, typename boost::enable_if_c<
        boost::conjunction<
            boost::disjunction<
                detail::path_traits::is_path_source< typename boost::remove_cv< Source >::type >,
                detail::path_traits::is_convertible_to_path_source< typename boost::remove_cv< Source >::type >
            >,
            boost::negation< detail::path_traits::is_native_path_source< typename boost::remove_cv< Source >::type > >
        >::value
    >::type* = NULL)
#endif
    {
        assign(source, cvt);
    }

    //  As of October 2015 the interaction between noexcept and =default is so troublesome
    //  for VC++, GCC, and probably other compilers, that =default is not used with noexcept
    //  functions. GCC is not even consistent for the same release on different platforms.

#if !defined(BOOST_NO_CXX11_RVALUE_REFERENCES)
    path(path&& p) BOOST_NOEXCEPT : m_pathname(static_cast< string_type&& >(p.m_pathname))
    {
    }
    path(path&& p, codecvt_type const&) BOOST_NOEXCEPT : m_pathname(static_cast< string_type&& >(p.m_pathname))
    {
    }
    path& operator=(path&& p) BOOST_NOEXCEPT
    {
        m_pathname = static_cast< string_type&& >(p.m_pathname);
        return *this;
    }
    path& assign(path&& p) BOOST_NOEXCEPT
    {
        m_pathname = static_cast< string_type&& >(p.m_pathname);
        return *this;
    }
    path& assign(path&& p, codecvt_type const&) BOOST_NOEXCEPT
    {
        m_pathname = static_cast< string_type&& >(p.m_pathname);
        return *this;
    }

    path(string_type&& s) BOOST_NOEXCEPT : m_pathname(static_cast< string_type&& >(s))
    {
    }
    path(string_type&& s, codecvt_type const&) BOOST_NOEXCEPT : m_pathname(static_cast< string_type&& >(s))
    {
    }
    path& operator=(string_type&& p) BOOST_NOEXCEPT
    {
        m_pathname = static_cast< string_type&& >(p);
        return *this;
    }
    path& assign(string_type&& p) BOOST_NOEXCEPT
    {
        m_pathname = static_cast< string_type&& >(p);
        return *this;
    }
    path& assign(string_type&& p, codecvt_type const&) BOOST_NOEXCEPT
    {
        m_pathname = static_cast< string_type&& >(p);
        return *this;
    }
#endif

    path(const value_type* begin, const value_type* end) : m_pathname(begin, end) {}
    path(const value_type* begin, const value_type* end, codecvt_type const&) : m_pathname(begin, end) {}

#if !defined(BOOST_NO_CXX11_FUNCTION_TEMPLATE_DEFAULT_ARGS)
    template<
        typename InputIterator,
        typename = typename boost::enable_if_c<
            boost::conjunction<
                detail::path_traits::is_path_source_iterator< InputIterator >,
                boost::negation< detail::path_traits::is_native_char_ptr< InputIterator > >
            >::value
        >::type
    >
    path(InputIterator begin, InputIterator end)
#else
    template< typename InputIterator >
    path(InputIterator begin, InputIterator end, typename boost::enable_if_c<
        boost::conjunction<
            detail::path_traits::is_path_source_iterator< InputIterator >,
            boost::negation< detail::path_traits::is_native_char_ptr< InputIterator > >
        >::value
    >::type* = NULL)
#endif
    {
        if (begin != end)
        {
            typedef std::basic_string< typename std::iterator_traits< InputIterator >::value_type > source_t;
            source_t source(begin, end);
#if !defined(BOOST_NO_CXX11_RVALUE_REFERENCES)
            assign(static_cast< source_t&& >(source));
#else
            assign(source);
#endif
        }
    }

#if !defined(BOOST_NO_CXX11_FUNCTION_TEMPLATE_DEFAULT_ARGS)
    template<
        typename InputIterator,
        typename = typename boost::enable_if_c<
            boost::conjunction<
                detail::path_traits::is_path_source_iterator< InputIterator >,
                boost::negation< detail::path_traits::is_native_char_ptr< InputIterator > >
            >::value
        >::type
    >
    path(InputIterator begin, InputIterator end, codecvt_type const& cvt)
#else
    template< typename InputIterator >
    path(InputIterator begin, InputIterator end, codecvt_type const& cvt, typename boost::enable_if_c<
        boost::conjunction<
            detail::path_traits::is_path_source_iterator< InputIterator >,
            boost::negation< detail::path_traits::is_native_char_ptr< InputIterator > >
        >::value
    >::type* = NULL)
#endif
    {
        if (begin != end)
        {
            typedef std::basic_string< typename std::iterator_traits< InputIterator >::value_type > source_t;
            source_t source(begin, end);
#if !defined(BOOST_NO_CXX11_RVALUE_REFERENCES)
            assign(static_cast< source_t&& >(source), cvt);
#else
            assign(source, cvt);
#endif
        }
    }

    //  -----  assignments  -----

    // We need to explicitly define copy assignment as otherwise it will be implicitly defined as deleted because there is move assignment
    path& operator=(path const& p);

    template< typename Source >
    typename boost::enable_if_c<
        boost::disjunction<
            detail::path_traits::is_path_source< typename boost::remove_cv< Source >::type >,
            detail::path_traits::is_convertible_to_path_source< typename boost::remove_cv< Source >::type >
        >::value,
        path&
    >::type operator=(Source const& source)
    {
        return assign(source);
    }

    path& assign(path const& p)
    {
        m_pathname = p.m_pathname;
        return *this;
    }

    template< typename Source >
    typename boost::enable_if_c<
        detail::path_traits::is_path_source< typename boost::remove_cv< Source >::type >::value,
        path&
    >::type assign(Source const& source)
    {
        detail::path_traits::dispatch(source, assign_op(*this));
        return *this;
    }

    template< typename Source >
    typename boost::enable_if_c<
        boost::conjunction<
            detail::path_traits::is_convertible_to_path_source< typename boost::remove_cv< Source >::type >,
            boost::negation< detail::path_traits::is_path_source< typename boost::remove_cv< Source >::type > >
        >::value,
        path&
    >::type assign(Source const& source)
    {
        detail::path_traits::dispatch_convertible(source, assign_op(*this));
        return *this;
    }

    path& assign(path const& p, codecvt_type const&)
    {
        m_pathname = p.m_pathname;
        return *this;
    }

    template< typename Source >
    typename boost::enable_if_c<
        detail::path_traits::is_path_source< typename boost::remove_cv< Source >::type >::value,
        path&
    >::type assign(Source const& source, codecvt_type const& cvt)
    {
        detail::path_traits::dispatch(source, assign_op(*this), &cvt);
        return *this;
    }

    template< typename Source >
    typename boost::enable_if_c<
        boost::conjunction<
            detail::path_traits::is_convertible_to_path_source< typename boost::remove_cv< Source >::type >,
            boost::negation< detail::path_traits::is_path_source< typename boost::remove_cv< Source >::type > >
        >::value,
        path&
    >::type assign(Source const& source, codecvt_type const& cvt)
    {
        detail::path_traits::dispatch_convertible(source, assign_op(*this), &cvt);
        return *this;
    }

    path& assign(const value_type* begin, const value_type* end)
    {
        m_pathname.assign(begin, end);
        return *this;
    }

    template< typename InputIterator >
    typename boost::enable_if_c<
        boost::conjunction<
            detail::path_traits::is_path_source_iterator< InputIterator >,
            boost::negation< detail::path_traits::is_native_char_ptr< InputIterator > >
        >::value,
        path&
    >::type assign(InputIterator begin, InputIterator end)
    {
        m_pathname.clear();
        if (begin != end)
        {
            typedef std::basic_string< typename std::iterator_traits< InputIterator >::value_type > source_t;
            source_t source(begin, end);
#if !defined(BOOST_NO_CXX11_RVALUE_REFERENCES)
            assign(static_cast< source_t&& >(source));
#else
            assign(source);
#endif
        }
        return *this;
    }

    path& assign(const value_type* begin, const value_type* end, codecvt_type const&)
    {
        m_pathname.assign(begin, end);
        return *this;
    }

    template< typename InputIterator >
    typename boost::enable_if_c<
        boost::conjunction<
            detail::path_traits::is_path_source_iterator< InputIterator >,
            boost::negation< detail::path_traits::is_native_char_ptr< InputIterator > >
        >::value,
        path&
    >::type assign(InputIterator begin, InputIterator end, codecvt_type const& cvt)
    {
        m_pathname.clear();
        if (begin != end)
        {
            typedef std::basic_string< typename std::iterator_traits< InputIterator >::value_type > source_t;
            source_t source(begin, end);
#if !defined(BOOST_NO_CXX11_RVALUE_REFERENCES)
            assign(static_cast< source_t&& >(source), cvt);
#else
            assign(source, cvt);
#endif
        }
        return *this;
    }

    //  -----  concatenation  -----

    path& operator+=(path const& p);

    template< typename Source >
    typename boost::enable_if_c<
        boost::disjunction<
            detail::path_traits::is_path_source< typename boost::remove_cv< Source >::type >,
            detail::path_traits::is_convertible_to_path_source< typename boost::remove_cv< Source >::type >
        >::value,
        path&
    >::type operator+=(Source const& source)
    {
        return concat(source);
    }

    path& operator+=(value_type c)
    {
        m_pathname.push_back(c);
        return *this;
    }

    template< typename CharT >
    typename boost::enable_if_c<
        detail::path_traits::is_path_char_type< CharT >::value,
        path&
    >::type operator+=(CharT c)
    {
        CharT tmp[2];
        tmp[0] = c;
        tmp[1] = static_cast< CharT >(0);
        concat_op(*this)(tmp, tmp + 1);
        return *this;
    }

    path& concat(path const& p)
    {
        m_pathname.append(p.m_pathname);
        return *this;
    }

    template< typename Source >
    typename boost::enable_if_c<
        detail::path_traits::is_path_source< typename boost::remove_cv< Source >::type >::value,
        path&
    >::type concat(Source const& source)
    {
        detail::path_traits::dispatch(source, concat_op(*this));
        return *this;
    }

    template< typename Source >
    typename boost::enable_if_c<
        boost::conjunction<
            detail::path_traits::is_convertible_to_path_source< typename boost::remove_cv< Source >::type >,
            boost::negation< detail::path_traits::is_path_source< typename boost::remove_cv< Source >::type > >
        >::value,
        path&
    >::type concat(Source const& source)
    {
        detail::path_traits::dispatch_convertible(source, concat_op(*this));
        return *this;
    }

    path& concat(path const& p, codecvt_type const&)
    {
        m_pathname.append(p.m_pathname);
        return *this;
    }

    template< typename Source >
    typename boost::enable_if_c<
        detail::path_traits::is_path_source< typename boost::remove_cv< Source >::type >::value,
        path&
    >::type concat(Source const& source, codecvt_type const& cvt)
    {
        detail::path_traits::dispatch(source, concat_op(*this), &cvt);
        return *this;
    }

    template< typename Source >
    typename boost::enable_if_c<
        boost::conjunction<
            detail::path_traits::is_convertible_to_path_source< typename boost::remove_cv< Source >::type >,
            boost::negation< detail::path_traits::is_path_source< typename boost::remove_cv< Source >::type > >
        >::value,
        path&
    >::type concat(Source const& source, codecvt_type const& cvt)
    {
        detail::path_traits::dispatch_convertible(source, concat_op(*this), &cvt);
        return *this;
    }

    path& concat(const value_type* begin, const value_type* end)
    {
        m_pathname.append(begin, end);
        return *this;
    }

    template< typename InputIterator >
    typename boost::enable_if_c<
        boost::conjunction<
            detail::path_traits::is_path_source_iterator< InputIterator >,
            boost::negation< detail::path_traits::is_native_char_ptr< InputIterator > >
        >::value,
        path&
    >::type concat(InputIterator begin, InputIterator end)
    {
        if (begin != end)
        {
            std::basic_string< typename std::iterator_traits< InputIterator >::value_type > source(begin, end);
            detail::path_traits::dispatch(source, concat_op(*this));
        }
        return *this;
    }

    path& concat(const value_type* begin, const value_type* end, codecvt_type const&)
    {
        m_pathname.append(begin, end);
        return *this;
    }

    template< typename InputIterator >
    typename boost::enable_if_c<
        boost::conjunction<
            detail::path_traits::is_path_source_iterator< InputIterator >,
            boost::negation< detail::path_traits::is_native_char_ptr< InputIterator > >
        >::value,
        path&
    >::type concat(InputIterator begin, InputIterator end, codecvt_type const& cvt)
    {
        if (begin != end)
        {
            std::basic_string< typename std::iterator_traits< InputIterator >::value_type > source(begin, end);
            detail::path_traits::dispatch(source, concat_op(*this), &cvt);
        }
        return *this;
    }

    //  -----  appends  -----

    //  if a separator is added, it is the preferred separator for the platform;
    //  slash for POSIX, backslash for Windows

    path& operator/=(path const& p);

    template< typename Source >
    BOOST_FORCEINLINE typename boost::enable_if_c<
        boost::disjunction<
            detail::path_traits::is_path_source< typename boost::remove_cv< Source >::type >,
            detail::path_traits::is_convertible_to_path_source< typename boost::remove_cv< Source >::type >
        >::value,
        path&
    >::type operator/=(Source const& source)
    {
        return append(source);
    }

    BOOST_FORCEINLINE path& append(path const& p)
    {
        BOOST_FILESYSTEM_VERSIONED_SYM(append)(p.m_pathname.data(), p.m_pathname.data() + p.m_pathname.size());
        return *this;
    }

    template< typename Source >
    BOOST_FORCEINLINE typename boost::enable_if_c<
        detail::path_traits::is_path_source< typename boost::remove_cv< Source >::type >::value,
        path&
    >::type append(Source const& source)
    {
        detail::path_traits::dispatch(source, append_op(*this));
        return *this;
    }

    template< typename Source >
    BOOST_FORCEINLINE typename boost::enable_if_c<
        boost::conjunction<
            detail::path_traits::is_convertible_to_path_source< typename boost::remove_cv< Source >::type >,
            boost::negation< detail::path_traits::is_path_source< typename boost::remove_cv< Source >::type > >
        >::value,
        path&
    >::type append(Source const& source)
    {
        detail::path_traits::dispatch_convertible(source, append_op(*this));
        return *this;
    }

    BOOST_FORCEINLINE path& append(path const& p, codecvt_type const&)
    {
        BOOST_FILESYSTEM_VERSIONED_SYM(append)(p.m_pathname.data(), p.m_pathname.data() + p.m_pathname.size());
        return *this;
    }

    template< typename Source >
    BOOST_FORCEINLINE typename boost::enable_if_c<
        detail::path_traits::is_path_source< typename boost::remove_cv< Source >::type >::value,
        path&
    >::type append(Source const& source, codecvt_type const& cvt)
    {
        detail::path_traits::dispatch(source, append_op(*this), &cvt);
        return *this;
    }

    template< typename Source >
    BOOST_FORCEINLINE typename boost::enable_if_c<
        boost::conjunction<
            detail::path_traits::is_convertible_to_path_source< typename boost::remove_cv< Source >::type >,
            boost::negation< detail::path_traits::is_path_source< typename boost::remove_cv< Source >::type > >
        >::value,
        path&
    >::type append(Source const& source, codecvt_type const& cvt)
    {
        detail::path_traits::dispatch_convertible(source, append_op(*this), &cvt);
        return *this;
    }

    BOOST_FORCEINLINE path& append(const value_type* begin, const value_type* end)
    {
        BOOST_FILESYSTEM_VERSIONED_SYM(append)(begin, end);
        return *this;
    }

    template< typename InputIterator >
    BOOST_FORCEINLINE typename boost::enable_if_c<
        boost::conjunction<
            detail::path_traits::is_path_source_iterator< InputIterator >,
            boost::negation< detail::path_traits::is_native_char_ptr< InputIterator > >
        >::value,
        path&
    >::type append(InputIterator begin, InputIterator end)
    {
        std::basic_string< typename std::iterator_traits< InputIterator >::value_type > source(begin, end);
        detail::path_traits::dispatch(source, append_op(*this));
        return *this;
    }

    BOOST_FORCEINLINE path& append(const value_type* begin, const value_type* end, codecvt_type const&)
    {
        BOOST_FILESYSTEM_VERSIONED_SYM(append)(begin, end);
        return *this;
    }

    template< typename InputIterator >
    BOOST_FORCEINLINE typename boost::enable_if_c<
        boost::conjunction<
            detail::path_traits::is_path_source_iterator< InputIterator >,
            boost::negation< detail::path_traits::is_native_char_ptr< InputIterator > >
        >::value,
        path&
    >::type append(InputIterator begin, InputIterator end, const codecvt_type& cvt)
    {
        std::basic_string< typename std::iterator_traits< InputIterator >::value_type > source(begin, end);
        detail::path_traits::dispatch(source, append_op(*this), &cvt);
        return *this;
    }

    //  -----  modifiers  -----

    void clear() BOOST_NOEXCEPT { m_pathname.clear(); }
#ifdef BOOST_POSIX_API
    path& make_preferred()
    {
        // No effect on POSIX
        return *this;
    }
#else // BOOST_WINDOWS_API
    BOOST_FILESYSTEM_DECL path& make_preferred(); // change slashes to backslashes
#endif
    BOOST_FILESYSTEM_DECL path& remove_filename();
    BOOST_FILESYSTEM_DECL path& remove_trailing_separator();
    BOOST_FORCEINLINE path& replace_extension(path const& new_extension = path())
    {
        BOOST_FILESYSTEM_VERSIONED_SYM(replace_extension)(new_extension);
        return *this;
    }
    void swap(path& rhs) BOOST_NOEXCEPT { m_pathname.swap(rhs.m_pathname); }

    //  -----  observers  -----

    //  For operating systems that format file paths differently than directory
    //  paths, return values from observers are formatted as file names unless there
    //  is a trailing separator, in which case returns are formatted as directory
    //  paths. POSIX and Windows make no such distinction.

    //  Implementations are permitted to return const values or const references.

    //  The string or path returned by an observer are specified as being formatted
    //  as "native" or "generic".
    //
    //  For POSIX, these are all the same format; slashes and backslashes are as input and
    //  are not modified.
    //
    //  For Windows,   native:    as input; slashes and backslashes are not modified;
    //                            this is the format of the internally stored string.
    //                 generic:   backslashes are converted to slashes

    //  -----  native format observers  -----

    string_type const& native() const BOOST_NOEXCEPT { return m_pathname; }
    const value_type* c_str() const BOOST_NOEXCEPT { return m_pathname.c_str(); }
    string_type::size_type size() const BOOST_NOEXCEPT { return m_pathname.size(); }

    template< typename String >
    String string() const;

    template< typename String >
    String string(codecvt_type const& cvt) const;

#ifdef BOOST_WINDOWS_API
    std::string string() const
    {
        std::string tmp;
        if (!m_pathname.empty())
            detail::path_traits::convert(m_pathname.data(), m_pathname.data() + m_pathname.size(), tmp);
        return tmp;
    }
    std::string string(codecvt_type const& cvt) const
    {
        std::string tmp;
        if (!m_pathname.empty())
            detail::path_traits::convert(m_pathname.data(), m_pathname.data() + m_pathname.size(), tmp, &cvt);
        return tmp;
    }

    //  string_type is std::wstring, so there is no conversion
    std::wstring const& wstring() const { return m_pathname; }
    std::wstring const& wstring(codecvt_type const&) const { return m_pathname; }
#else // BOOST_POSIX_API
    //  string_type is std::string, so there is no conversion
    std::string const& string() const { return m_pathname; }
    std::string const& string(codecvt_type const&) const { return m_pathname; }

    std::wstring wstring() const
    {
        std::wstring tmp;
        if (!m_pathname.empty())
            detail::path_traits::convert(m_pathname.data(), m_pathname.data() + m_pathname.size(), tmp);
        return tmp;
    }
    std::wstring wstring(codecvt_type const& cvt) const
    {
        std::wstring tmp;
        if (!m_pathname.empty())
            detail::path_traits::convert(m_pathname.data(), m_pathname.data() + m_pathname.size(), tmp, &cvt);
        return tmp;
    }
#endif

    //  -----  generic format observers  -----

    //  Experimental generic function returning generic formatted path (i.e. separators
    //  are forward slashes). Motivation: simpler than a family of generic_*string
    //  functions.
#ifdef BOOST_WINDOWS_API
    BOOST_FILESYSTEM_DECL path generic_path() const;
#else
    path generic_path() const;
#endif

    template< typename String >
    String generic_string() const;

    template< typename String >
    String generic_string(codecvt_type const& cvt) const;

#ifdef BOOST_WINDOWS_API
    std::string generic_string() const { return generic_path().string(); }
    std::string generic_string(codecvt_type const& cvt) const { return generic_path().string(cvt); }
    std::wstring generic_wstring() const { return generic_path().wstring(); }
    std::wstring generic_wstring(codecvt_type const&) const { return generic_wstring(); }
#else // BOOST_POSIX_API
    //  On POSIX-like systems, the generic format is the same as the native format
    std::string const& generic_string() const { return m_pathname; }
    std::string const& generic_string(codecvt_type const&) const { return m_pathname; }
    std::wstring generic_wstring() const { return this->wstring(); }
    std::wstring generic_wstring(codecvt_type const& cvt) const { return this->wstring(cvt); }
#endif

    //  -----  compare  -----

    BOOST_FORCEINLINE int compare(path const& p) const // generic, lexicographical
    {
        return BOOST_FILESYSTEM_VERSIONED_SYM(compare)(p);
    }

    //  -----  decomposition  -----

    path root_path() const { return path(m_pathname.c_str(), m_pathname.c_str() + find_root_path_size()); }
    // returns 0 or 1 element path even on POSIX, root_name() is non-empty() for network paths
    path root_name() const { return path(m_pathname.c_str(), m_pathname.c_str() + find_root_name_size()); }

    // returns 0 or 1 element path
    path root_directory() const
    {
        path_detail::substring root_dir = find_root_directory();
        const value_type* p = m_pathname.c_str() + root_dir.pos;
        return path(p, p + root_dir.size);
    }

    path relative_path() const
    {
        path_detail::substring rel_path = find_relative_path();
        const value_type* p = m_pathname.c_str() + rel_path.pos;
        return path(p, p + rel_path.size);
    }

    path parent_path() const { return path(m_pathname.c_str(), m_pathname.c_str() + find_parent_path_size()); }

    BOOST_FORCEINLINE path filename() const { return BOOST_FILESYSTEM_VERSIONED_SYM(filename)(); }   // returns 0 or 1 element path
    BOOST_FORCEINLINE path stem() const { return BOOST_FILESYSTEM_VERSIONED_SYM(stem)(); }           // returns 0 or 1 element path
    BOOST_FORCEINLINE path extension() const { return BOOST_FILESYSTEM_VERSIONED_SYM(extension)(); } // returns 0 or 1 element path

    //  -----  query  -----

    bool empty() const BOOST_NOEXCEPT { return m_pathname.empty(); }
    bool filename_is_dot() const;
    bool filename_is_dot_dot() const;
    bool has_root_path() const { return find_root_path_size() > 0; }
    bool has_root_name() const { return find_root_name_size() > 0; }
    bool has_root_directory() const { return find_root_directory().size > 0; }
    bool has_relative_path() const { return find_relative_path().size > 0; }
    bool has_parent_path() const { return find_parent_path_size() > 0; }
    BOOST_FORCEINLINE bool has_filename() const { return BOOST_FILESYSTEM_VERSIONED_SYM(has_filename)(); }
    bool has_stem() const { return !stem().empty(); }
    bool has_extension() const { return !extension().empty(); }
    bool is_relative() const { return !is_absolute(); }
    bool is_absolute() const
    {
        // Windows CE has no root name (aka drive letters)
#if defined(BOOST_WINDOWS_API) && !defined(UNDER_CE)
        return has_root_name() && has_root_directory();
#else
        return has_root_directory();
#endif
    }

    //  -----  lexical operations  -----

    BOOST_FORCEINLINE path lexically_normal() const { return BOOST_FILESYSTEM_VERSIONED_SYM(lexically_normal)(); }
    BOOST_FILESYSTEM_DECL path lexically_relative(path const& base) const;
    path lexically_proximate(path const& base) const;

    //  -----  iterators  -----

    BOOST_FILESYSTEM_DECL iterator begin() const;
    BOOST_FILESYSTEM_DECL iterator end() const;
    reverse_iterator rbegin() const;
    reverse_iterator rend() const;

    //  -----  static member functions  -----

    static BOOST_FILESYSTEM_DECL std::locale imbue(std::locale const& loc);
    static BOOST_FILESYSTEM_DECL codecvt_type const& codecvt();

    //  -----  deprecated functions  -----

#if !defined(BOOST_FILESYSTEM_NO_DEPRECATED)
    //  recently deprecated functions supplied by default
    path& normalize();
    BOOST_FILESYSTEM_DETAIL_DEPRECATED("Use path::remove_filename() instead")
    path& remove_leaf() { return remove_filename(); }
    BOOST_FILESYSTEM_DETAIL_DEPRECATED("Use path::filename() instead")
    path leaf() const { return filename(); }
    BOOST_FILESYSTEM_DETAIL_DEPRECATED("Use path::parent_path() instead")
    path branch_path() const { return parent_path(); }
    BOOST_FILESYSTEM_DETAIL_DEPRECATED("Use path::generic_path() instead")
    path generic() const { return generic_path(); }
    BOOST_FILESYSTEM_DETAIL_DEPRECATED("Use !path::empty() instead")
    bool has_leaf() const { return !m_pathname.empty(); }
    BOOST_FILESYSTEM_DETAIL_DEPRECATED("Use path::has_parent_path() instead")
    bool has_branch_path() const { return has_parent_path(); }
    BOOST_FILESYSTEM_DETAIL_DEPRECATED("Use path::is_absolute() instead")
    bool is_complete() const { return is_absolute(); }
#endif

#if defined(BOOST_FILESYSTEM_DEPRECATED)
    //  deprecated functions with enough signature or semantic changes that they are
    //  not supplied by default
    BOOST_FILESYSTEM_DETAIL_DEPRECATED("Use path::string() instead")
    std::string file_string() const { return string(); }
    BOOST_FILESYSTEM_DETAIL_DEPRECATED("Use path::string() instead")
    std::string directory_string() const { return string(); }
    BOOST_FILESYSTEM_DETAIL_DEPRECATED("Use path::string() instead")
    std::string native_file_string() const { return string(); }
    BOOST_FILESYSTEM_DETAIL_DEPRECATED("Use path::string() instead")
    std::string native_directory_string() const { return string(); }
    BOOST_FILESYSTEM_DETAIL_DEPRECATED("Use path::native() instead")
    string_type external_file_string() const { return native(); }
    BOOST_FILESYSTEM_DETAIL_DEPRECATED("Use path::native() instead")
    string_type external_directory_string() const { return native(); }
#endif

    //--------------------------------------------------------------------------------------//
    //                            class path private members                                //
    //--------------------------------------------------------------------------------------//
private:
    bool has_filename_v3() const { return !m_pathname.empty(); }
    bool has_filename_v4() const { return find_filename_v4_size() > 0; }
    BOOST_FILESYSTEM_DECL path filename_v3() const;
    path filename_v4() const
    {
        string_type::size_type filename_size = find_filename_v4_size();
        string_type::size_type pos = m_pathname.size() - filename_size;
        const value_type* p = m_pathname.c_str() + pos;
        return path(p, p + filename_size);
    }
    BOOST_FILESYSTEM_DECL path stem_v3() const;
    BOOST_FILESYSTEM_DECL path stem_v4() const;
    BOOST_FILESYSTEM_DECL path extension_v3() const;
    path extension_v4() const
    {
        string_type::size_type extension_size = find_extension_v4_size();
        string_type::size_type pos = m_pathname.size() - extension_size;
        const value_type* p = m_pathname.c_str() + pos;
        return path(p, p + extension_size);
    }

    BOOST_FILESYSTEM_DECL void replace_extension_v3(path const& new_extension);
    BOOST_FILESYSTEM_DECL void replace_extension_v4(path const& new_extension);

    BOOST_FILESYSTEM_DECL path lexically_normal_v3() const;
    BOOST_FILESYSTEM_DECL path lexically_normal_v4() const;

    BOOST_FILESYSTEM_DECL int compare_v3(path const& p) const;
    BOOST_FILESYSTEM_DECL int compare_v4(path const& p) const;

    BOOST_FILESYSTEM_DECL void append_v3(const value_type* b, const value_type* e);
    BOOST_FILESYSTEM_DECL void append_v4(const value_type* b, const value_type* e);

    //  Returns: If separator is to be appended, m_pathname.size() before append. Otherwise 0.
    //  Note: An append is never performed if size()==0, so a returned 0 is unambiguous.
    BOOST_FILESYSTEM_DECL string_type::size_type append_separator_if_needed();
    BOOST_FILESYSTEM_DECL void erase_redundant_separator(string_type::size_type sep_pos);

    BOOST_FILESYSTEM_DECL string_type::size_type find_root_name_size() const;
    BOOST_FILESYSTEM_DECL string_type::size_type find_root_path_size() const;
    BOOST_FILESYSTEM_DECL path_detail::substring find_root_directory() const;
    BOOST_FILESYSTEM_DECL path_detail::substring find_relative_path() const;
    BOOST_FILESYSTEM_DECL string_type::size_type find_parent_path_size() const;
    BOOST_FILESYSTEM_DECL string_type::size_type find_filename_v4_size() const;
    BOOST_FILESYSTEM_DECL string_type::size_type find_extension_v4_size() const;

private:
    /*
     * m_pathname has the type, encoding, and format required by the native
     * operating system. Thus for POSIX and Windows there is no conversion for
     * passing m_pathname.c_str() to the O/S API or when obtaining a path from the
     * O/S API. POSIX encoding is unspecified other than for dot and slash
     * characters; POSIX just treats paths as a sequence of bytes. Windows
     * encoding is UCS-2 or UTF-16 depending on the version.
     */
    string_type m_pathname;     // Windows: as input; backslashes NOT converted to slashes,
                                // slashes NOT converted to backslashes
};

namespace detail {
BOOST_FILESYSTEM_DECL int lex_compare_v3(path::iterator first1, path::iterator last1, path::iterator first2, path::iterator last2);
BOOST_FILESYSTEM_DECL int lex_compare_v4(path::iterator first1, path::iterator last1, path::iterator first2, path::iterator last2);
BOOST_FILESYSTEM_DECL path const& dot_path();
BOOST_FILESYSTEM_DECL path const& dot_dot_path();
} // namespace detail

#ifndef BOOST_FILESYSTEM_NO_DEPRECATED
typedef path wpath;
#endif

//------------------------------------------------------------------------------------//
//                             class path::iterator                                   //
//------------------------------------------------------------------------------------//

class path::iterator :
    public boost::iterator_facade<
        path::iterator,
        const path,
        boost::bidirectional_traversal_tag
    >
{
private:
    friend class boost::iterator_core_access;
    friend class boost::filesystem::path;
    friend class boost::filesystem::path::reverse_iterator;
    friend BOOST_FILESYSTEM_DECL int detail::lex_compare_v3(path::iterator first1, path::iterator last1, path::iterator first2, path::iterator last2);

    path const& dereference() const { return m_element; }

    bool equal(iterator const& rhs) const
    {
        return m_path_ptr == rhs.m_path_ptr && m_pos == rhs.m_pos;
    }

    BOOST_FORCEINLINE void increment() { BOOST_FILESYSTEM_VERSIONED_SYM(increment)(); }
    BOOST_FORCEINLINE void decrement() { BOOST_FILESYSTEM_VERSIONED_SYM(decrement)(); }

private:
    BOOST_FILESYSTEM_DECL void increment_v3();
    BOOST_FILESYSTEM_DECL void increment_v4();
    BOOST_FILESYSTEM_DECL void decrement_v3();
    BOOST_FILESYSTEM_DECL void decrement_v4();

private:
    // current element
    path m_element;
    // path being iterated over
    const path* m_path_ptr;
    // position of m_element in m_path_ptr->m_pathname.
    // if m_element is implicit dot, m_pos is the
    // position of the last separator in the path.
    // end() iterator is indicated by
    // m_pos == m_path_ptr->m_pathname.size()
    string_type::size_type m_pos;
};

//------------------------------------------------------------------------------------//
//                         class path::reverse_iterator                               //
//------------------------------------------------------------------------------------//

class path::reverse_iterator :
    public boost::iterator_facade<
        path::reverse_iterator,
        const path,
        boost::bidirectional_traversal_tag
    >
{
public:
    explicit reverse_iterator(iterator itr) :
        m_itr(itr)
    {
        if (itr != itr.m_path_ptr->begin())
            m_element = *--itr;
    }

private:
    friend class boost::iterator_core_access;
    friend class boost::filesystem::path;

    path const& dereference() const { return m_element; }
    bool equal(reverse_iterator const& rhs) const { return m_itr == rhs.m_itr; }

    void increment()
    {
        --m_itr;
        if (m_itr != m_itr.m_path_ptr->begin())
        {
            iterator tmp = m_itr;
            m_element = *--tmp;
        }
    }

    void decrement()
    {
        m_element = *m_itr;
        ++m_itr;
    }

private:
    iterator m_itr;
    path m_element;
};

//------------------------------------------------------------------------------------//
//                                                                                    //
//                              non-member functions                                  //
//                                                                                    //
//------------------------------------------------------------------------------------//

//  std::lexicographical_compare would infinitely recurse because path iterators
//  yield paths, so provide a path aware version
BOOST_FORCEINLINE bool lexicographical_compare(path::iterator first1, path::iterator last1, path::iterator first2, path::iterator last2)
{
    return BOOST_FILESYSTEM_VERSIONED_SYM(detail::lex_compare)(first1, last1, first2, last2) < 0;
}

BOOST_FORCEINLINE bool operator==(path const& lhs, path const& rhs)
{
    return lhs.compare(rhs) == 0;
}

BOOST_FORCEINLINE bool operator!=(path const& lhs, path const& rhs)
{
    return lhs.compare(rhs) != 0;
}

BOOST_FORCEINLINE bool operator<(path const& lhs, path const& rhs)
{
    return lhs.compare(rhs) < 0;
}

BOOST_FORCEINLINE bool operator<=(path const& lhs, path const& rhs)
{
    return !(rhs < lhs);
}

BOOST_FORCEINLINE bool operator>(path const& lhs, path const& rhs)
{
    return rhs < lhs;
}

BOOST_FORCEINLINE bool operator>=(path const& lhs, path const& rhs)
{
    return !(lhs < rhs);
}

// Note: Declared as a template to delay binding to Boost.ContainerHash functions and make the dependency optional
template< typename T >
inline typename boost::enable_if_c<
    boost::is_same< T, path >::value,
    std::size_t
>::type hash_value(T const& p) BOOST_NOEXCEPT
{
#ifdef BOOST_WINDOWS_API
    std::size_t seed = 0u;
    for (typename T::value_type const* it = p.c_str(); *it; ++it)
        hash_combine(seed, *it == L'/' ? L'\\' : *it);
    return seed;
#else // BOOST_POSIX_API
    return hash_range(p.native().begin(), p.native().end());
#endif
}

inline void swap(path& lhs, path& rhs) BOOST_NOEXCEPT
{
    lhs.swap(rhs);
}

BOOST_FORCEINLINE path operator/(path lhs, path const& rhs)
{
    lhs.append(rhs);
    return lhs;
}

template< typename Source >
BOOST_FORCEINLINE typename boost::enable_if_c<
    boost::disjunction<
        detail::path_traits::is_path_source< typename boost::remove_cv< Source >::type >,
        detail::path_traits::is_convertible_to_path_source< typename boost::remove_cv< Source >::type >
    >::value,
    path
>::type operator/(path lhs, Source const& rhs)
{
    lhs.append(rhs);
    return lhs;
}

//  inserters and extractors
//    use boost::io::quoted() to handle spaces in paths
//    use '&' as escape character to ease use for Windows paths

template< typename Char, typename Traits >
inline std::basic_ostream< Char, Traits >&
operator<<(std::basic_ostream< Char, Traits >& os, path const& p)
{
    return os << boost::io::quoted(p.template string< std::basic_string< Char > >(), static_cast< Char >('&'));
}

template< typename Char, typename Traits >
inline std::basic_istream< Char, Traits >&
operator>>(std::basic_istream< Char, Traits >& is, path& p)
{
    std::basic_string< Char > str;
    is >> boost::io::quoted(str, static_cast< Char >('&'));
    p = str;
    return is;
}

//  name_checks

//  These functions are holdovers from version 1. It isn't clear they have much
//  usefulness, or how to generalize them for later versions.

BOOST_FILESYSTEM_DECL bool portable_posix_name(std::string const& name);
BOOST_FILESYSTEM_DECL bool windows_name(std::string const& name);
BOOST_FILESYSTEM_DECL bool portable_name(std::string const& name);
BOOST_FILESYSTEM_DECL bool portable_directory_name(std::string const& name);
BOOST_FILESYSTEM_DECL bool portable_file_name(std::string const& name);
BOOST_FILESYSTEM_DECL bool native(std::string const& name);

namespace detail {

//  For POSIX, is_directory_separator() and is_element_separator() are identical since
//  a forward slash is the only valid directory separator and also the only valid
//  element separator. For Windows, forward slash and back slash are the possible
//  directory separators, but colon (example: "c:foo") is also an element separator.
inline bool is_directory_separator(path::value_type c) BOOST_NOEXCEPT
{
    return c == path::separator
#ifdef BOOST_WINDOWS_API
        || c == path::preferred_separator
#endif
        ;
}

inline bool is_element_separator(path::value_type c) BOOST_NOEXCEPT
{
    return c == path::separator
#ifdef BOOST_WINDOWS_API
        || c == path::preferred_separator || c == L':'
#endif
        ;
}

} // namespace detail

//------------------------------------------------------------------------------------//
//                  class path miscellaneous function implementations                 //
//------------------------------------------------------------------------------------//

// Note: Because of the range constructor in C++23 std::string_view that involves a check for contiguous_range concept,
//       any non-template function call that requires a check whether the source argument (which may be fs::path)
//       is convertible to std::string_view must be made after fs::path::iterator is defined. This includes overload
//       resolution and SFINAE checks. Otherwise, the concept check result formally changes between fs::path::iterator
//       is not defined and defined, which causes compilation errors with gcc 11 and later.
//       https://gcc.gnu.org/bugzilla/show_bug.cgi?id=106808

inline path& path::operator=(path const& p)
{
    return assign(p);
}

inline path& path::operator+=(path const& p)
{
    return concat(p);
}

BOOST_FORCEINLINE path& path::operator/=(path const& p)
{
    return append(p);
}

#if !defined(BOOST_WINDOWS_API)
inline path path::generic_path() const
{
    return path(*this);
}
#endif

inline path path::lexically_proximate(path const& base) const
{
    path tmp(lexically_relative(base));
    return tmp.empty() ? *this : tmp;
}

inline path::reverse_iterator path::rbegin() const
{
    return reverse_iterator(end());
}

inline path::reverse_iterator path::rend() const
{
    return reverse_iterator(begin());
}

inline bool path::filename_is_dot() const
{
    // implicit dot is tricky, so actually call filename(); see path::filename() example
    // in reference.html
    path p(filename());
    return p.size() == 1 && *p.c_str() == dot;
}

inline bool path::filename_is_dot_dot() const
{
    return size() >= 2 && m_pathname[size() - 1] == dot && m_pathname[size() - 2] == dot && (m_pathname.size() == 2 || detail::is_element_separator(m_pathname[size() - 3]));
    // use detail::is_element_separator() rather than detail::is_directory_separator
    // to deal with "c:.." edge case on Windows when ':' acts as a separator
}

#if !defined(BOOST_FILESYSTEM_NO_DEPRECATED)

BOOST_FILESYSTEM_DETAIL_DEPRECATED("Use path::lexically_normal() instead")
inline path& path::normalize()
{
    path tmp(lexically_normal());
    m_pathname.swap(tmp.m_pathname);
    return *this;
}

#endif // !defined(BOOST_FILESYSTEM_NO_DEPRECATED)

//--------------------------------------------------------------------------------------//
//                     class path member template specializations                       //
//--------------------------------------------------------------------------------------//

template< >
inline std::string path::string< std::string >() const
{
    return string();
}

template< >
inline std::wstring path::string< std::wstring >() const
{
    return wstring();
}

template< >
inline std::string path::string< std::string >(codecvt_type const& cvt) const
{
    return string(cvt);
}

template< >
inline std::wstring path::string< std::wstring >(codecvt_type const& cvt) const
{
    return wstring(cvt);
}

template< >
inline std::string path::generic_string< std::string >() const
{
    return generic_string();
}

template< >
inline std::wstring path::generic_string< std::wstring >() const
{
    return generic_wstring();
}

template< >
inline std::string path::generic_string< std::string >(codecvt_type const& cvt) const
{
    return generic_string(cvt);
}

template< >
inline std::wstring path::generic_string< std::wstring >(codecvt_type const& cvt) const
{
    return generic_wstring(cvt);
}

} // namespace filesystem
} // namespace boost

//----------------------------------------------------------------------------//

#include <boost/filesystem/detail/footer.hpp>

#endif // BOOST_FILESYSTEM_PATH_HPP
