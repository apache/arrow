//  (C) Copyright John Maddock 2021

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX20_HDR_COMPARE
//  TITLE:         C++20 <compare> header is either not present or too broken to be used
//  DESCRIPTION:   The compiler does not support the C++20 header <compare>

#include <compare>

namespace boost_no_cxx20_hdr_compare {

   using std::three_way_comparable;
   using std::three_way_comparable_with;
   using std::partial_ordering;
   using std::weak_ordering;
   using std::strong_ordering;
   using std::common_comparison_category;
   using std::compare_three_way_result;
   using std::compare_three_way;
   // Customization points aren't actual functions:
   //using std::strong_order;
   //using std::weak_order;
   //using std::partial_order;
   //using std::compare_strong_order_fallback;
   //using std::compare_weak_order_fallback;
   //using std::compare_partial_order_fallback;
   using std::is_eq;
   using std::is_neq;
   using std::is_lt;
   using std::is_lteq;
   using std::is_gt;
   using std::is_gteq;

   int test()
   {
      return 0;
   }

}
