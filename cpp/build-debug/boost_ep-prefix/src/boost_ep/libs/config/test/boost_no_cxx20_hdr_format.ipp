//  (C) Copyright John Maddock 2021

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX20_HDR_FORMAT
//  TITLE:         C++20 <format> header is either not present or too broken to be used
//  DESCRIPTION:   The compiler does not support the C++20 header <format>

#include <format>

namespace boost_no_cxx20_hdr_format {

   using std::formatter;
   using std::basic_format_parse_context;
   using std::format_parse_context;
   using std::wformat_parse_context;
   using std::basic_format_context;
   using std::format_context;
   using std::wformat_context;
   using std::basic_format_arg;
   using std::basic_format_args;
   using std::format_args;
   using std::wformat_args;
   using std::format_error;
   using std::format;
   using std::format_to;
   using std::format_to_n;
   using std::formatted_size;
   using std::vformat;
   using std::vformat_to;
   using std::visit_format_arg;
   using std::make_format_args;
   using std::make_wformat_args;

   int test()
   {
      return 0;
   }

}
