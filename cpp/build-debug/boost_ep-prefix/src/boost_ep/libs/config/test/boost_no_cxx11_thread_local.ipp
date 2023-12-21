//  (C) Copyright John Maddock 2012. 
//  Use, modification and distribution are subject to the 
//  Boost Software License, Version 1.0. (See accompanying file 
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for most recent version.

//  MACRO:         BOOST_NO_CXX11_THREAD_LOCAL
//  TITLE:         thread_local
//  DESCRIPTION:   The compiler supports the thread_local storage specifier.

#include <string>


namespace boost_no_cxx11_thread_local{

template <class T>
int check_local(int n)
{
   static thread_local T s(n, ' ');
   static thread_local int size = s.size();
   if(size != n)
   {
      s = T(n, ' ');
      size = n;
   }
   return size;
}

int test()
{
   return check_local<std::string>(5) == 5 ? 0 : 1;
}

}

