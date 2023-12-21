//  Copyright 2022 Andrey Semashev.
//
//  Distributed under the Boost Software License, Version 1.0.
//
//  See accompanying file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt

#include <boost/config.hpp>

BOOST_DEPRECATED("Use bar() instead.")
void foo();

template< typename T >
class BOOST_DEPRECATED("Use std::unique_ptr instead.") my_auto_ptr
{
};

BOOST_DEPRECATED("Use std::numeric_limits<int>::max() instead.")
const int max_int = 0x7fffffff;
