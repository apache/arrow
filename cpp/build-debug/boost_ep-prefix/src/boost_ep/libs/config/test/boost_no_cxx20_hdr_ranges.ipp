//  (C) Copyright John Maddock 2021

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX20_HDR_RANGES
//  TITLE:         C++20 <ranges> header is either not present or too broken to be used
//  DESCRIPTION:   The compiler does not support the C++20 header <ranges>

#include <ranges>

namespace boost_no_cxx20_hdr_ranges {

   using std::ranges::range;
   using std::ranges::borrowed_range;
   using std::ranges::sized_range;
   using std::ranges::view;
   using std::ranges::input_range;
   using std::ranges::output_range;
   using std::ranges::forward_range;
   using std::ranges::bidirectional_range;
   using std::ranges::random_access_range;
   using std::ranges::contiguous_range;
   using std::ranges::common_range;
   using std::ranges::viewable_range;

   using std::ranges::iterator_t;
   using std::ranges::sentinel_t;
   using std::ranges::range_difference_t;
   using std::ranges::range_size_t;
   using std::ranges::range_value_t;
   using std::ranges::range_reference_t;
   using std::ranges::range_rvalue_reference_t;
   using std::ranges::view_interface;
   using std::ranges::subrange;
   using std::ranges::dangling;
   using std::ranges::borrowed_iterator_t;
   using std::ranges::borrowed_subrange_t;
   using std::ranges::empty_view;
   using std::views::empty;
   using std::ranges::single_view;
   using std::views::single;
   using std::ranges::iota_view;
   using std::views::iota;
   using std::ranges::basic_istream_view;
   using std::ranges::istream_view;
   using std::views::all_t;
   using std::views::all;
   using std::ranges::ref_view;
   using std::ranges::filter_view;
   using std::views::filter;
   using std::ranges::transform_view;
   using std::views::transform;
   using std::ranges::take_view;
   using std::views::take;
   using std::ranges::take_while_view;
   using std::views::take_while;
   using std::ranges::drop_view;
   using std::views::drop;
   using std::ranges::drop_while_view;
   using std::views::drop_while;
   using std::ranges::join_view;
   using std::views::join;
   using std::ranges::split_view;
   using std::views::split;
   using std::views::counted;
   using std::ranges::common_view;
   using std::views::common;
   using std::ranges::reverse_view;
   using std::views::reverse;
   using std::ranges::elements_view;
   using std::views::elements;
   using std::ranges::keys_view;
   using std::views::keys;
   using std::ranges::values_view;
   using std::views::values;
   using std::ranges::begin;
   using std::ranges::end;
   using std::ranges::cbegin;
   using std::ranges::cend;
   using std::ranges::rbegin;
   using std::ranges::rend;
   using std::ranges::crbegin;
   using std::ranges::crend;
   using std::ranges::size;
   using std::ranges::ssize;
   constexpr auto dummy = std::ranges::empty; // Would conflict with std::views::empty
   using std::ranges::data;
   using std::ranges::cdata;
   using std::ranges::subrange_kind;


   int test()
   {
      return 0;
   }

}
