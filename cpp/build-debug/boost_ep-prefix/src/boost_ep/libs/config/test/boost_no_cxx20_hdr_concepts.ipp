//  (C) Copyright John Maddock 2021

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX20_HDR_CONCEPTS
//  TITLE:         C++20 <concepts> header is either not present or too broken to be used
//  DESCRIPTION:   The compiler does not support the C++20 header <concepts>

#include <concepts>

namespace boost_no_cxx20_hdr_concepts {

   using std::same_as;
   using std::derived_from;
   using std::convertible_to;
   using std::common_reference_with;
   using std::common_with;
   using std::integral;
   using std::signed_integral;
   using std::unsigned_integral;
   using std::floating_point;
   using std::assignable_from;
   using std::swappable;
   using std::swappable_with;
   using std::destructible;
   using std::constructible_from;
   using std::default_initializable;
   using std::move_constructible;
   using std::copy_constructible;
   using std::equality_comparable;
   using std::equality_comparable_with;
   using std::totally_ordered;
   using std::totally_ordered_with;
   using std::movable;
   using std::copyable;
   using std::semiregular;
   using std::regular;
   using std::invocable;
   using std::regular_invocable;
   using std::predicate;
   using std::relation;
   using std::equivalence_relation;
   using std::strict_weak_order;
   using std::swap;

   int test()
   {
      return 0;
   }

}
