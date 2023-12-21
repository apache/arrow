
// Copyright 2017-2020 Peter Dimov.
//
// Distributed under the Boost Software License, Version 1.0.
//
// See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt

#include <boost/random/random_device.hpp>

#if defined(_MSC_VER)
__declspec(dllexport)
#endif
unsigned get_random_number()
{
    boost::random::random_device dev;
    return dev();
}
