//  (C) Copyright John Maddock 2001. 
//  Use, modification and distribution are subject to the 
//  Boost Software License, Version 1.0. (See accompanying file 
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for most recent version.

//  MACRO:         BOOST_HAS_PARTIAL_STD_ALLOCATOR
//  TITLE:         limited std::allocator support
//  DESCRIPTION:   The std lib has at least some kind of stanfard allocator
//                 with allocate/deallocate members and probably not much more.

#include <memory>

#if defined(__GNUC__) && ((__GNUC__ > 4) || ((__GNUC__ == 4) && (__GNUC_MINOR__ >= 7)))
#  define BOOST_UNUSED_ATTRIBUTE __attribute__((unused))
#else
#  define BOOST_UNUSED_ATTRIBUTE
#endif

namespace boost_has_partial_std_allocator{

//
// test everything except rebind template members:
//

template <class T>
int test_allocator(const T& i)
{
   typedef std::allocator<int> alloc1_t;
#if !((__cplusplus > 201700) || (defined(_MSVC_LANG) && (_MSVC_LANG > 201700)))
   typedef typename alloc1_t::size_type           size_type;
   typedef typename alloc1_t::difference_type     difference_type BOOST_UNUSED_ATTRIBUTE;
   typedef typename alloc1_t::pointer             pointer;
   typedef typename alloc1_t::const_pointer       const_pointer;
   typedef typename alloc1_t::reference           reference;
   typedef typename alloc1_t::const_reference     const_reference;
   typedef typename alloc1_t::value_type          value_type BOOST_UNUSED_ATTRIBUTE;
#endif
   alloc1_t a1;
   (void)i;
#if !((__cplusplus > 201700) || (defined(_MSVC_LANG) && (_MSVC_LANG > 201700)))
   pointer p = a1.allocate(1);
   const_pointer cp = p;
   a1.construct(p,i);
   size_type s = a1.max_size();
   (void)s;
   reference r = *p;
   const_reference cr = *cp;
   if(p != a1.address(r)) return -1;
   if(cp != a1.address(cr)) return -1;
   a1.destroy(p);
   a1.deallocate(p,1);
#endif
   return 0;
}


int test()
{
   return test_allocator(0);
}

}

#undef BOOST_UNUSED_ATTRIBUTE

