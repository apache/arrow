//  Copyright (c) Andrey Semashev 2017.
//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for most recent version.

//  MACRO:         BOOST_NO_CXX17_ITERATOR_TRAITS
//  TITLE:         C++17 std::iterator_traits
//  DESCRIPTION:   The compiler does not support SFINAE-friendly std::iterator_traits defined in C++17.

#include <iterator>

namespace boost_no_cxx17_iterator_traits {

struct iterator
{
   typedef std::random_access_iterator_tag iterator_category;
   typedef char value_type;
   typedef std::ptrdiff_t difference_type;
   typedef char* pointer;
   typedef char& reference;

   reference operator*()const;
   iterator operator++();
};

struct non_iterator {};

template< typename T >
struct void_type { typedef void type; };

template< typename Traits, typename Void = void >
struct has_iterator_category
{
    enum { value = false };
};

template< typename Traits >
struct has_iterator_category< Traits, typename void_type< typename Traits::iterator_category >::type >
{
    enum { value = true };
};

int test()
{
    static_assert(has_iterator_category< std::iterator_traits< boost_no_cxx17_iterator_traits::iterator > >::value, "has_iterator_category failed");

    static_assert(!has_iterator_category< std::iterator_traits< boost_no_cxx17_iterator_traits::non_iterator > >::value, "has_iterator_category negative check failed");

    return 0;
}

}
