/*
Copyright Rene Rivera 2011-2017
Distributed under the Boost Software License, Version 1.0.
(See accompanying file LICENSE_1_0.txt or copy at
http://www.boost.org/LICENSE_1_0.txt)
*/
#include <boost/predef/version_number.h>
#define BOOST_STRICT_CONFIG
#include <boost/predef/other/workaround.h>

int main()
{
#if BOOST_PREDEF_WORKAROUND(BOOST_VERSION_NUMBER_AVAILABLE,==,0,0,1)
    fail();
#endif
    return 0;
}
