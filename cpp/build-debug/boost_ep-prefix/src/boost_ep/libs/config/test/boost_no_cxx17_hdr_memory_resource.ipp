//  (C) Copyright John Maddock 2020

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX17_HDR_MEMORY_RESOURCE
//  TITLE:         C++17 header <memory_resource> unavailable
//  DESCRIPTION:   The standard library does not supply C++17 header <memory_resource>

#include <memory_resource>

namespace boost_no_cxx17_hdr_memory_resource {

int test()
{
  using std::pmr::polymorphic_allocator;
  using std::pmr::memory_resource;
  using std::pmr::pool_options;
  using std::pmr::synchronized_pool_resource;
  using std::pmr::unsynchronized_pool_resource;
  using std::pmr::monotonic_buffer_resource;
  using std::pmr::new_delete_resource;
  using std::pmr::null_memory_resource;
  using std::pmr::get_default_resource;
  using std::pmr::set_default_resource;
  return 0;
}

}
