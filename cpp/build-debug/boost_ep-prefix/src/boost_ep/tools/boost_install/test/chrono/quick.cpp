
// Copyright 2018 Peter Dimov.
//
// Distributed under the Boost Software License, Version 1.0.
//
// See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt

#include <boost/chrono.hpp>
#include <iostream>

int main()
{
    std::cout << boost::chrono::system_clock::now() << std::endl;
    std::cout << boost::chrono::steady_clock::now() << std::endl;
}
