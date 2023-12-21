//  (C) Copyright John Maddock 2013

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX11_HDR_ATOMIC
//  TITLE:         C++11 <atomic> header is either not present or too broken to be used
//  DESCRIPTION:   The compiler does not support the C++11 header <atomic>

#include <atomic>

#if !defined(ATOMIC_BOOL_LOCK_FREE) || !defined(ATOMIC_CHAR_LOCK_FREE) || !defined(ATOMIC_CHAR16_T_LOCK_FREE) \
     || !defined(ATOMIC_CHAR32_T_LOCK_FREE) || !defined(ATOMIC_WCHAR_T_LOCK_FREE) || !defined(ATOMIC_SHORT_LOCK_FREE)\
     || !defined(ATOMIC_INT_LOCK_FREE) || !defined(ATOMIC_LONG_LOCK_FREE) || !defined(ATOMIC_LLONG_LOCK_FREE)\
     || !defined(ATOMIC_POINTER_LOCK_FREE)
#  error "required macros not defined"
#endif

namespace boost_no_cxx11_hdr_atomic {

   void consume(std::memory_order)
   {}

   int test()
   {
      consume(std::memory_order_relaxed);
      consume(std::memory_order_consume);
      consume(std::memory_order_acquire);
      consume(std::memory_order_release);
      consume(std::memory_order_acq_rel);
      consume(std::memory_order_seq_cst);

      std::atomic<int> a1;
      std::atomic<unsigned> a2;
      std::atomic<int*>    a3;
      a1.is_lock_free();
      a1.store(1);
      a1.load();
      a1.exchange(2);
      int v;
      a1.compare_exchange_weak(v, 2, std::memory_order_relaxed, std::memory_order_relaxed);
      a1.compare_exchange_strong(v, 2, std::memory_order_relaxed, std::memory_order_relaxed);
      a1.fetch_add(2);
      a1.fetch_sub(3);
      a1.fetch_and(3);
      a1.fetch_or(1);
      a1.fetch_xor(1);
      a1++;
      ++a1;
      a1--;
      --a1;
      a1 += 2;
      a1 -= 2;
      a1 &= 1;
      a1 |= 2;
      a1 ^= 3;

      a2 = 0u;

      a3.store(&v);
      a3.fetch_add(1);
      a3.fetch_sub(1);
      ++a3;
      --a3;
      a3++;
      a3--;
      a3 += 1;
      a3 -= 1;

      std::atomic_is_lock_free(&a1);
      // This produces linker errors on Mingw32 for some reason, probably not required anyway for most uses??
      //std::atomic_init(&a1, 2);
      std::atomic_store(&a1, 3);
      std::atomic_store_explicit(&a1, 3, std::memory_order_relaxed);
      std::atomic_load(&a1);
      std::atomic_load_explicit(&a1, std::memory_order_relaxed);
      std::atomic_exchange(&a1, 3);
      std::atomic_compare_exchange_weak(&a1, &v, 2);
      std::atomic_compare_exchange_strong(&a1, &v, 2);
      std::atomic_compare_exchange_weak_explicit(&a1, &v, 2, std::memory_order_relaxed, std::memory_order_relaxed);
      std::atomic_compare_exchange_strong_explicit(&a1, &v, 2, std::memory_order_relaxed, std::memory_order_relaxed);

      std::atomic_flag f = ATOMIC_FLAG_INIT;
      f.test_and_set(std::memory_order_relaxed);
      f.test_and_set();
      f.clear(std::memory_order_relaxed);
      f.clear();

      std::atomic_thread_fence(std::memory_order_relaxed);
      std::atomic_signal_fence(std::memory_order_relaxed);

      return 0;
   }

}
