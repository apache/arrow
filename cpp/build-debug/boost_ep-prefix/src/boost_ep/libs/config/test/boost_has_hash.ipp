//  (C) Copyright John Maddock 2001.
//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for most recent version.

//  MACRO:         BOOST_HAS_HASH
//  TITLE:         <hashset> and <hashmap>
//  DESCRIPTION:   The C++ implementation provides the (SGI) hash_set
//                 or hash_map classes.

#include BOOST_HASH_SET_HEADER
#include BOOST_HASH_MAP_HEADER

#ifndef BOOST_STD_EXTENSION_NAMESPACE
#define BOOST_STD_EXTENSION_NAMESPACE std
#endif

namespace boost_has_hash{

#ifndef DISABLE_BOOST_HAS_HASH_TEST

template <class Key, class Eq, class Hash, class Alloc>
void foo(const BOOST_STD_EXTENSION_NAMESPACE::hash_set<Key,Eq,Hash,Alloc>& )
{
}

template <class Key, class Eq, class Hash, class Alloc>
void foo(const BOOST_STD_EXTENSION_NAMESPACE::hash_multiset<Key,Eq,Hash,Alloc>& )
{
}

template <class Key, class T, class Eq, class Hash, class Alloc>
void foo(const BOOST_STD_EXTENSION_NAMESPACE::hash_map<Key,T,Eq,Hash,Alloc>& )
{
}

#endif

int test()
{
#ifndef DISABLE_BOOST_HAS_HASH_TEST
   BOOST_STD_EXTENSION_NAMESPACE::hash_set<int> hs;
   hs.insert(2);
   foo(hs);
   BOOST_STD_EXTENSION_NAMESPACE::hash_map<int, long> hm;
   hm[3] = 2;
   foo(hm);
   BOOST_STD_EXTENSION_NAMESPACE::hash_multiset<int> hs2;
   hs2.insert(2);
   hs2.insert(2);
   foo(hs2);
#endif
   return 0;
}

}






