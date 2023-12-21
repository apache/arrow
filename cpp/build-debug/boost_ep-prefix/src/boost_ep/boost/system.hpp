#ifndef BOOST_SYSTEM_HPP_INCLUDED
#define BOOST_SYSTEM_HPP_INCLUDED

// Copyright 2021 Peter Dimov
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt)
//
// See library home page at http://www.boost.org/libs/system

#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>
#include <boost/config.hpp>

#if (__cplusplus >= 201103L) && !(defined(BOOST_GCC) && BOOST_GCC < 40800) || (defined(_MSC_VER) && _MSC_VER >= 1900)
# include <boost/system/result.hpp>
#endif

#endif // #ifndef BOOST_SYSTEM_HPP_INCLUDED
