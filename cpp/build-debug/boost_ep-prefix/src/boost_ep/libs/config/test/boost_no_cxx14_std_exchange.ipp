//  (C) Copyright Oliver Kowalke 2016. 
//  Use, modification and distribution are subject to the 
//  Boost Software License, Version 1.0. (See accompanying file 
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for most recent version.

//  MACRO:         BOOST_NO_CXX14_STD_EXCHANGE
//  TITLE:         apply
//  DESCRIPTION:   The compiler supports the std::exchange() function.

#include <utility>

namespace boost_no_cxx14_std_exchange {

int test() {
    int * i = new int( 1);
    int * j = std::exchange( i, nullptr);
    delete j;
    return 0;
}

}
