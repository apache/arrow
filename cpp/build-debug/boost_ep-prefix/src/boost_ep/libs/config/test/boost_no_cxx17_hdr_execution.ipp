//  (C) Copyright John Maddock 2020

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX17_HDR_EXECUTION
//  TITLE:         C++17 header <execution> unavailable
//  DESCRIPTION:   The standard library does not supply C++17 header <execution>

#include <execution>

namespace boost_no_cxx17_hdr_execution {

int test()
{
  using std::is_execution_policy;
  using std::execution::sequenced_policy;
  using std::execution::parallel_policy;
  using std::execution::parallel_unsequenced_policy;
  using std::execution::seq;
  using std::execution::par;
  using std::execution::par_unseq;
  return 0;
}

}
