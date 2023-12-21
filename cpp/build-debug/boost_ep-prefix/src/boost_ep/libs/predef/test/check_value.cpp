/*
Copyright Rene Rivera 2015
Distributed under the Boost Software License, Version 1.0.
(See accompanying file LICENSE_1_0.txt or copy at
http://www.boost.org/LICENSE_1_0.txt)
*/

/*
 * Simple program that just prints out the externally
 * defined CHECK_VALUE def. It's used to test the check
 * program and the related BB support.
 */

#include <boost/predef.h>
#include <iostream>
#include <string>

#ifndef CHECK_VALUE
#define CHECK_VALUE "undefined"
#endif

int main()
{
	std::cout << "CHECK_VALUE == " << CHECK_VALUE << "\n" ;
	return 0;
}
