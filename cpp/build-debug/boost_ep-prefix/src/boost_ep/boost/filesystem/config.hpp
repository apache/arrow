//  boost/filesystem/v3/config.hpp  ----------------------------------------------------//

//  Copyright Beman Dawes 2003
//  Copyright Andrey Semashev 2021

//  Distributed under the Boost Software License, Version 1.0.
//  See http://www.boost.org/LICENSE_1_0.txt

//  Library home page: http://www.boost.org/libs/filesystem

//--------------------------------------------------------------------------------------//

#ifndef BOOST_FILESYSTEM_CONFIG_HPP
#define BOOST_FILESYSTEM_CONFIG_HPP

// This header implements separate compilation features as described in
// http://www.boost.org/more/separate_compilation.html

#include <boost/config.hpp>
#include <boost/system/api_config.hpp> // for BOOST_POSIX_API or BOOST_WINDOWS_API
#include <boost/detail/workaround.hpp>

#if defined(BOOST_FILESYSTEM_VERSION) && BOOST_FILESYSTEM_VERSION != 3 && BOOST_FILESYSTEM_VERSION != 4
#error Compiling Boost.Filesystem file with BOOST_FILESYSTEM_VERSION defined != 3 or 4
#endif

#if defined(BOOST_FILESYSTEM_SOURCE)
#undef BOOST_FILESYSTEM_VERSION
#define BOOST_FILESYSTEM_VERSION 4
#elif !defined(BOOST_FILESYSTEM_VERSION)
#define BOOST_FILESYSTEM_VERSION 3
#endif

#define BOOST_FILESYSTEM_VERSIONED_SYM(sym) BOOST_JOIN(sym, BOOST_JOIN(_v, BOOST_FILESYSTEM_VERSION))

#if BOOST_FILESYSTEM_VERSION == 4
#undef BOOST_FILESYSTEM_DEPRECATED
#if !defined(BOOST_FILESYSTEM_NO_DEPRECATED)
#define BOOST_FILESYSTEM_NO_DEPRECATED
#endif
#endif

#define BOOST_FILESYSTEM_I18N // aid users wishing to compile several versions

//  BOOST_FILESYSTEM_DEPRECATED needed for source compiles -----------------------------//

#ifdef BOOST_FILESYSTEM_SOURCE
#define BOOST_FILESYSTEM_DEPRECATED
#undef BOOST_FILESYSTEM_NO_DEPRECATED // fixes #9454, src bld fails if NO_DEP defined
#endif

#if defined(BOOST_FILESYSTEM_DEPRECATED) && defined(BOOST_FILESYSTEM_NO_DEPRECATED)
#error Both BOOST_FILESYSTEM_DEPRECATED and BOOST_FILESYSTEM_NO_DEPRECATED are defined
#endif

//  throw an exception  ----------------------------------------------------------------//
//
//  Exceptions were originally thrown via boost::throw_exception().
//  As throw_exception() became more complex, it caused user error reporting
//  to be harder to interpret, since the exception reported became much more complex.
//  The immediate fix was to throw directly, wrapped in a macro to make any later change
//  easier.

#define BOOST_FILESYSTEM_THROW(EX) throw EX

#if defined(BOOST_NO_STD_WSTRING)
#error Configuration not supported: Boost.Filesystem V3 and later requires std::wstring support
#endif

// Deprecated symbols markup -----------------------------------------------------------//

#if !defined(BOOST_FILESYSTEM_ALLOW_DEPRECATED)

#if !defined(BOOST_FILESYSTEM_DETAIL_DEPRECATED) && defined(_MSC_VER)
#if (_MSC_VER) >= 1400
#define BOOST_FILESYSTEM_DETAIL_DEPRECATED(msg) __declspec(deprecated(msg))
#else
// MSVC 7.1 only supports the attribute without a message
#define BOOST_FILESYSTEM_DETAIL_DEPRECATED(msg) __declspec(deprecated)
#endif
#endif

#if !defined(BOOST_FILESYSTEM_DETAIL_DEPRECATED) && defined(__has_extension)
#if __has_extension(attribute_deprecated_with_message)
#define BOOST_FILESYSTEM_DETAIL_DEPRECATED(msg) __attribute__((deprecated(msg)))
#endif
#endif

// gcc since 4.5 supports deprecated attribute with a message; older versions support the attribute without a message.
// Oracle Studio 12.4 supports deprecated attribute with a message; this is the first release that supports the attribute.
#if !defined(BOOST_FILESYSTEM_DETAIL_DEPRECATED) && (\
    (defined(__GNUC__) && (__GNUC__ * 100 + __GNUC_MINOR__) >= 405) ||\
    (defined(__SUNPRO_CC) && __SUNPRO_CC >= 0x5130))
#define BOOST_FILESYSTEM_DETAIL_DEPRECATED(msg) __attribute__((deprecated(msg)))
#endif

#if !defined(BOOST_FILESYSTEM_DETAIL_DEPRECATED) && __cplusplus >= 201402
#define BOOST_FILESYSTEM_DETAIL_DEPRECATED(msg) [[deprecated(msg)]]
#endif

#if !defined(BOOST_FILESYSTEM_DETAIL_DEPRECATED) && defined(__GNUC__)
#define BOOST_FILESYSTEM_DETAIL_DEPRECATED(msg) __attribute__((deprecated))
#endif

#if !defined(BOOST_FILESYSTEM_DETAIL_DEPRECATED) && defined(__has_attribute)
#if __has_attribute(deprecated)
#define BOOST_FILESYSTEM_DETAIL_DEPRECATED(msg) __attribute__((deprecated))
#endif
#endif

#endif // !defined(BOOST_FILESYSTEM_ALLOW_DEPRECATED)

#if !defined(BOOST_FILESYSTEM_DETAIL_DEPRECATED)
#define BOOST_FILESYSTEM_DETAIL_DEPRECATED(msg)
#endif


//  This header implements separate compilation features as described in
//  http://www.boost.org/more/separate_compilation.html

//  normalize macros  ------------------------------------------------------------------//

#if !defined(BOOST_FILESYSTEM_DYN_LINK) && !defined(BOOST_FILESYSTEM_STATIC_LINK) && !defined(BOOST_ALL_DYN_LINK) && !defined(BOOST_ALL_STATIC_LINK)
#define BOOST_FILESYSTEM_STATIC_LINK
#endif

#if defined(BOOST_ALL_DYN_LINK) && !defined(BOOST_FILESYSTEM_DYN_LINK)
#define BOOST_FILESYSTEM_DYN_LINK
#elif defined(BOOST_ALL_STATIC_LINK) && !defined(BOOST_FILESYSTEM_STATIC_LINK)
#define BOOST_FILESYSTEM_STATIC_LINK
#endif

#if defined(BOOST_FILESYSTEM_DYN_LINK) && defined(BOOST_FILESYSTEM_STATIC_LINK)
#error Must not define both BOOST_FILESYSTEM_DYN_LINK and BOOST_FILESYSTEM_STATIC_LINK
#endif

#if defined(BOOST_ALL_NO_LIB) && !defined(BOOST_FILESYSTEM_NO_LIB)
#define BOOST_FILESYSTEM_NO_LIB
#endif

//  enable dynamic linking  ------------------------------------------------------------//

#if defined(BOOST_ALL_DYN_LINK) || defined(BOOST_FILESYSTEM_DYN_LINK)
#if defined(BOOST_FILESYSTEM_SOURCE)
#define BOOST_FILESYSTEM_DECL BOOST_SYMBOL_EXPORT
#else
#define BOOST_FILESYSTEM_DECL BOOST_SYMBOL_IMPORT
#endif
#else
#define BOOST_FILESYSTEM_DECL
#endif

//  enable automatic library variant selection  ----------------------------------------//

#if !defined(BOOST_FILESYSTEM_SOURCE) && !defined(BOOST_ALL_NO_LIB) && !defined(BOOST_FILESYSTEM_NO_LIB)
//
// Set the name of our library, this will get undef'ed by auto_link.hpp
// once it's done with it:
//
#define BOOST_LIB_NAME boost_filesystem
//
// If we're importing code from a dll, then tell auto_link.hpp about it:
//
#if defined(BOOST_ALL_DYN_LINK) || defined(BOOST_FILESYSTEM_DYN_LINK)
#define BOOST_DYN_LINK
#endif
//
// And include the header that does the work:
//
#include <boost/config/auto_link.hpp>
#endif // auto-linking disabled

#if !defined(BOOST_NO_CXX17_HDR_STRING_VIEW) && \
    (\
        (defined(BOOST_DINKUMWARE_STDLIB) && defined(_HAS_CXX23) && (_HAS_CXX23 != 0) && defined(_MSVC_STL_UPDATE) && (_MSVC_STL_UPDATE < 202208L)) || \
        (defined(BOOST_LIBSTDCXX_VERSION) && (BOOST_LIBSTDCXX_VERSION < 110400 || (BOOST_LIBSTDCXX_VERSION >= 120000 && BOOST_LIBSTDCXX_VERSION < 120200)) && (BOOST_CXX_VERSION > 202002L))\
    )
// Indicates that std::string_view has implicit constructor from ranges that was present in an early C++23 draft (N4892).
// This was later rectified by marking the constructor explicit (https://wg21.link/p2499). Unfortunately, some compilers
// were released with the constructor being implicit.
#define BOOST_FILESYSTEM_CXX23_STRING_VIEW_HAS_IMPLICIT_RANGE_CTOR
#endif

#endif // BOOST_FILESYSTEM_CONFIG_HPP
