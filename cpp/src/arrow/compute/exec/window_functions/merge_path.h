// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <algorithm>
#include <cstdint>

namespace arrow {
namespace compute {

class MergeUtil {
 public:
  // Merge Path
  // https://birk.net.technion.ac.il/files/2021/01/OdehGreenMwassiShmueliBirk-MergePath-ParallelMergingMadeSimple-IPDPS_Workshops2012.pdf
  //
  // Given the desired size N of prefix of merged sequence,
  // Find the sizes A and B of prefixes of two sorted input sequences that
  // will generate these N elements. Return the size of the first sequence
  // A.
  //
  template <class T_IS_LESS>
  static int64_t MergePath(int64_t n, int64_t cardA, int64_t cardB,
                           /* takes as an input int64_t index of left element
                              and index of right element */
                           T_IS_LESS is_less) {
    // Binary search for A
    // in [AMIN = max(0, N - cardB); AMAX = min(cardA - 1, N - 1)] range for
    // the largest value such that:
    //    inA[A] < inB[N - 1 - A]
    // If inA[AMIN] >= inB[N - 1 - AMIN] then return AMIN - 1.
    // A will be the largest index to use from A.
    //
    int64_t l = std::max(static_cast<int64_t>(0), n - cardB) - 1;
    int64_t r = std::min(cardA, n);
    while (l + 1 < r) {
      int64_t m = (l + r) / 2;
      if (is_less(m, n - 1 - m)) {
        l = m;
      } else {
        r = m;
      }
    }
    return l + 1;
  }

  template <class T_IS_LESS>
  static bool IsNextFromA(int64_t offsetA, int64_t offsetB, int64_t cardA, int64_t cardB,
                          /* takes as an input int64_t index of left element
                             and index of right element */
                          T_IS_LESS is_less) {
    if (offsetA == cardA) {
      ARROW_DCHECK(offsetB < cardB);
      return false;
    } else if (offsetB == cardB) {
      ARROW_DCHECK(offsetA < cardA);
      return true;
    } else {
      if (is_less(offsetA, offsetB)) {
        return true;
      } else {
        return false;
      }
    }
  }

  template <class T_IS_LESS>
  static void NthElement(int64_t n, int64_t cardA, int64_t cardB, bool* fromA,
                         int64_t* offset,
                         /* takes as an input int64_t index of left element
                            and index of right element */
                         T_IS_LESS is_less) {
    int64_t offsetA = MergePath(n, cardA, cardB, is_less);
    int64_t offsetB = n - offsetA;
    *fromA = IsNextFromA(offsetA, offsetB, cardA, cardB, is_less);
    *offset = *fromA ? offsetA : offsetB;
  }

  template <class T_IS_LESS>
  static void NthPair(int64_t n, int64_t cardA, int64_t cardB, bool* first_fromA,
                      int64_t* first_offset, bool* second_fromA, int64_t* second_offset,
                      /* takes as an input int64_t index of left element
                         and index of right element */
                      T_IS_LESS is_less) {
    int64_t offsetA = MergePath(n, cardA, cardB, is_less);
    int64_t offsetB = n - offsetA;
    *first_fromA = IsNextFromA(offsetA, offsetB, cardA, cardB, is_less);
    *first_offset = *first_fromA ? offsetA : offsetB;
    if (*first_fromA) {
      ++offsetA;
    } else {
      ++offsetB;
    }
    *second_fromA = IsNextFromA(offsetA, offsetB, cardA, cardB, is_less);
    *second_offset = *second_fromA ? offsetA : offsetB;
  }
};

}  // namespace compute
}  // namespace arrow
