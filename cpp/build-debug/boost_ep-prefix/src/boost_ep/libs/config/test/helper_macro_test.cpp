//  (C) Copyright John Maddock 2014-9. 
//  (C) Copyright Andrey Semashev 2017. 
//  Use, modification and distribution are subject to the  
//  Boost Software License, Version 1.0. (See accompanying file  
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt) 

#include <boost/config.hpp>

int test_fallthrough(int n) 
{ 
   switch (n) 
   { 
   case 0: 
      n++; 
      BOOST_FALLTHROUGH; 
   case 1: 
      n++; 
      break; 
   } 
   return n; 
}

int test_unreachable(int i)
{
   if(BOOST_LIKELY(i)) return i;

   throw i;
   BOOST_UNREACHABLE_RETURN(0)  // NOTE: no semicolon afterwards!!
}

BOOST_FORCEINLINE int always_inline(int i){ return ++i; }
BOOST_NOINLINE int never_inline(int i){ return ++i; }

BOOST_NORETURN void always_throw()
{
   throw 0;
}

struct BOOST_MAY_ALIAS aliasing_struct {};
typedef unsigned int BOOST_MAY_ALIAS aliasing_uint;

struct BOOST_ATTRIBUTE_NODISCARD nodiscard_struct {};

BOOST_ATTRIBUTE_NODISCARD int nodiscard_proc(int i)
{
   return i * i;
}


#define test_fallthrough(x) foobar(x)

struct empty {};
struct no_unique
{
   int a;
   BOOST_ATTRIBUTE_NO_UNIQUE_ADDRESS empty b;
};

template <bool b>
struct trait
{
   enum { value = b };
};

void* test_nullptr()
{
   return BOOST_NULLPTR;
}

int main()
{
   typedef int unused_type BOOST_ATTRIBUTE_UNUSED;
   try
   {
      int result = test_fallthrough BOOST_PREVENT_MACRO_SUBSTITUTION(0);
      BOOST_STATIC_CONSTANT(bool, value = 0);
      result += test_unreachable(1);
      result += always_inline(2);
      result += never_inline(3);
      if(BOOST_UNLIKELY(!result))
         always_throw();
      nodiscard_struct s;
      no_unique no_un;

      BOOST_IF_CONSTEXPR(trait<true>::value)
      {
         result += 2;
      }

      test_nullptr();
   }
   catch(int)
   {
      return 1;
   }
   return 0;
}

