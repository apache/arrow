/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef _QUANTILES_SKETCH_HPP_
#define _QUANTILES_SKETCH_HPP_

#include <functional>
#include <memory>
#include <vector>

#include "quantiles_sorted_view.hpp"
#include "common_defs.hpp"
#include "serde.hpp"

namespace datasketches {

/**
 * This is a stochastic streaming sketch that enables near-real time analysis of the
 * approximate distribution from a very large stream in a single pass.
 * The analysis is obtained using get_rank() and get_quantile() functions,
 * the Probability Mass Function from get_PMF() and the Cumulative Distribution Function from get_CDF().
 *
 * <p>Consider a large stream of one million values such as packet sizes coming into a network node.
 * The natural rank of any specific size value is its index in the hypothetical sorted
 * array of values.
 * The normalized rank is the natural rank divided by the stream size,
 * in this case one million.
 * The value corresponding to the normalized rank of 0.5 represents the 50th percentile or median
 * value of the distribution, or get_quantile(0.5). Similarly, the 95th percentile is obtained from
 * get_quantile(0.95).</p>
 *
 * <p>From the min and max values, for example, 1 and 1000 bytes,
 * you can obtain the PMF from get_PMF(100, 500, 900) that will result in an array of
 * 4 fractional values such as {.4, .3, .2, .1}, which means that
 * <ul>
 * <li>40% of the values were &lt; 100,</li>
 * <li>30% of the values were &ge; 100 and &lt; 500,</li>
 * <li>20% of the values were &ge; 500 and &lt; 900, and</li>
 * <li>10% of the values were &ge; 900.</li>
 * </ul>
 * A frequency histogram can be obtained by multiplying these fractions by get_n(),
 * which is the total count of values received.
 * The get_CDF() works similarly, but produces the cumulative distribution instead.
 *
 * <p>As of November 2021, this implementation produces serialized sketches which are binary-compatible
 * with the equivalent Java implementation only when template parameter T = double
 * (64-bit double precision values).
 * 
 * <p>The accuracy of this sketch is a function of the configured value <i>k</i>, which also affects
 * the overall size of the sketch. Accuracy of this quantile sketch is always with respect to
 * the normalized rank. A <i>k</i> of 128 produces a normalized, rank error of about 1.7%.
 * For example, the median item returned from getQuantile(0.5) will be between the actual items
 * from the hypothetically sorted array of input items at normalized ranks of 0.483 and 0.517, with
 * a confidence of about 99%.</p>
 *
 * <pre>
Table Guide for DoublesSketch Size in Bytes and Approximate Error:
          K =&gt; |      16      32      64     128     256     512   1,024
    ~ Error =&gt; | 12.145%  6.359%  3.317%  1.725%  0.894%  0.463%  0.239%
             N | Size in Bytes -&gt;
------------------------------------------------------------------------
             0 |       8       8       8       8       8       8       8
             1 |      72      72      72      72      72      72      72
             3 |      72      72      72      72      72      72      72
             7 |     104     104     104     104     104     104     104
            15 |     168     168     168     168     168     168     168
            31 |     296     296     296     296     296     296     296
            63 |     424     552     552     552     552     552     552
           127 |     552     808   1,064   1,064   1,064   1,064   1,064
           255 |     680   1,064   1,576   2,088   2,088   2,088   2,088
           511 |     808   1,320   2,088   3,112   4,136   4,136   4,136
         1,023 |     936   1,576   2,600   4,136   6,184   8,232   8,232
         2,047 |   1,064   1,832   3,112   5,160   8,232  12,328  16,424
         4,095 |   1,192   2,088   3,624   6,184  10,280  16,424  24,616
         8,191 |   1,320   2,344   4,136   7,208  12,328  20,520  32,808
        16,383 |   1,448   2,600   4,648   8,232  14,376  24,616  41,000
        32,767 |   1,576   2,856   5,160   9,256  16,424  28,712  49,192
        65,535 |   1,704   3,112   5,672  10,280  18,472  32,808  57,384
       131,071 |   1,832   3,368   6,184  11,304  20,520  36,904  65,576
       262,143 |   1,960   3,624   6,696  12,328  22,568  41,000  73,768
       524,287 |   2,088   3,880   7,208  13,352  24,616  45,096  81,960
     1,048,575 |   2,216   4,136   7,720  14,376  26,664  49,192  90,152
     2,097,151 |   2,344   4,392   8,232  15,400  28,712  53,288  98,344
     4,194,303 |   2,472   4,648   8,744  16,424  30,760  57,384 106,536
     8,388,607 |   2,600   4,904   9,256  17,448  32,808  61,480 114,728
    16,777,215 |   2,728   5,160   9,768  18,472  34,856  65,576 122,920
    33,554,431 |   2,856   5,416  10,280  19,496  36,904  69,672 131,112
    67,108,863 |   2,984   5,672  10,792  20,520  38,952  73,768 139,304
   134,217,727 |   3,112   5,928  11,304  21,544  41,000  77,864 147,496
   268,435,455 |   3,240   6,184  11,816  22,568  43,048  81,960 155,688
   536,870,911 |   3,368   6,440  12,328  23,592  45,096  86,056 163,880
 1,073,741,823 |   3,496   6,696  12,840  24,616  47,144  90,152 172,072
 2,147,483,647 |   3,624   6,952  13,352  25,640  49,192  94,248 180,264
 4,294,967,295 |   3,752   7,208  13,864  26,664  51,240  98,344 188,456

 * </pre>

 * <p>There is more documentation available on
 * <a href="https://datasketches.apache.org">datasketches.apache.org</a>.</p>
 *
 * <p>This is an implementation of the Low Discrepancy Mergeable Quantiles Sketch
 * described in section 3.2 of the journal version of the paper "Mergeable Summaries"
 * by Agarwal, Cormode, Huang, Phillips, Wei, and Yi.
 * <a href="http://dblp.org/rec/html/journals/tods/AgarwalCHPWY13"></a></p>
 *
 * <p>This algorithm is independent of the distribution of items and
 * requires only that the items be comparable.</p>
 *
 * <p>This algorithm intentionally inserts randomness into the sampling process for items that
 * ultimately get retained in the sketch. The results produced by this algorithm are not
 * deterministic. For example, if the same stream is inserted into two different instances of this
 * sketch, the answers obtained from the two sketches may not be identical.</p>
 *
 * <p>Similarly, there may be directional inconsistencies. For example, the result
 * obtained from get_quantile(rank) input into the reverse directional query
 * get_rank(item) may not result in the original item.</p>
 *
 * @author Kevin Lang
 * @author Lee Rhodes
 * @author Alexander Saydakov
 * @author Jon Malkin
 */

namespace quantiles_constants {
  const uint16_t DEFAULT_K = 128;
  const uint16_t MIN_K = 2;
  const uint16_t MAX_K = 1 << 15;
}

template <typename T,
          typename Comparator = std::less<T>, // strict weak ordering function (see C++ named requirements: Compare)
          typename Allocator = std::allocator<T>>
class quantiles_sketch {
public:
  using value_type = T;
  using allocator_type = Allocator;
  using comparator = Comparator;

  explicit quantiles_sketch(uint16_t k = quantiles_constants::DEFAULT_K,
      const Comparator& comparator = Comparator(), const Allocator& allocator = Allocator());
  quantiles_sketch(const quantiles_sketch& other);
  quantiles_sketch(quantiles_sketch&& other) noexcept;
  ~quantiles_sketch();
  quantiles_sketch& operator=(const quantiles_sketch& other);
  quantiles_sketch& operator=(quantiles_sketch&& other) noexcept;

  /**
   * @brief Type converting constructor
   * @param other quantiles sketch of a different type
   * @param comparator instance of a Comparator
   * @param allocator instance of an Allocator
   */
  template<typename From, typename FC, typename FA>
  explicit quantiles_sketch(const quantiles_sketch<From, FC, FA>& other,
      const Comparator& comparator = Comparator(), const Allocator& allocator = Allocator());

  /**
   * Updates this sketch with the given data item.
   * @param item from a stream of items
   */
  template<typename FwdT>
  void update(FwdT&& item);

  /**
   * Merges another sketch into this one.
   * @param other sketch to merge into this one
   */
  template<typename FwdSk>
  void merge(FwdSk&& other);

  /**
   * Returns true if this sketch is empty.
   * @return empty flag
   */
  bool is_empty() const;

  /**
   * Returns configured parameter k
   * @return parameter k
   */
  uint16_t get_k() const;

  /**
   * Returns the length of the input stream.
   * @return stream length
   */
  uint64_t get_n() const;

  /**
   * Returns the number of retained items (samples) in the sketch.
   * @return the number of retained items
   */
  uint32_t get_num_retained() const;

  /**
   * Returns true if this sketch is in estimation mode.
   * @return estimation mode flag
   */
  bool is_estimation_mode() const;

  /**
   * Returns the min item of the stream.
   * If the sketch is empty this throws std::runtime_error.
   * @return the min item of the stream
   */
  const T& get_min_item() const;

  /**
   * Returns the max item of the stream.
   * If the sketch is empty this throws std::runtime_error.
   * @return the max item of the stream
   */
  const T& get_max_item() const;

  /**
   * Returns an instance of the comparator for this sketch.
   * @return comparator
   */
  Comparator get_comparator() const;

  /**
   * Returns the allocator for this sketch.
   * @return allocator
   */
  allocator_type get_allocator() const;

  /**
   * Returns an approximation to the data item associated with the given rank
   * of a hypothetical sorted version of the input stream so far.
   * <p>
   * If the sketch is empty this throws std::runtime_error.
   *
   * @param rank the specified normalized rank in the hypothetical sorted stream.
   *
   * @return the approximation to the item at the given rank
   */
  using quantile_return_type = typename quantiles_sorted_view<T, Comparator, Allocator>::quantile_return_type;
  quantile_return_type get_quantile(double rank, bool inclusive = true) const;

  /**
   * This is a multiple-query version of get_quantile().
   * <p>
   * This returns an array that could have been generated by using get_quantile() for each
   * normalized rank separately.
   *
   * <p>If the sketch is empty this throws std::runtime_error.
   *
   * @param ranks given array of normalized ranks in the hypothetical sorted stream.
   * These ranks must be in the interval [0.0, 1.0], inclusive.
   * @param size the number of ranks in the array
   *
   * @return array of approximations to items associated with given ranks in the same order as given ranks
   * in the input array.
   *
   * Deprecated. Will be removed in the next major version. Use get_quantile() instead.
   */
  std::vector<T, Allocator> get_quantiles(const double* ranks, uint32_t size, bool inclusive = true) const;

  /**
   * This is a multiple-query version of get_quantile() that allows the caller to
   * specify the number of evenly-spaced normalized ranks.
   *
   * <p>If the sketch is empty this throws std::runtime_error.
   *
   * @param num an integer that specifies the number of evenly-spaced ranks.
   * This must be an integer greater than 0. A value of 1 is equivalent to get_quantiles([0]).
   * A value of 2 is equivalent to get_quantiles([0, 1]). A value of 3 is equivalent to
   * get_quantiles([0, 0.5, 1]), etc.
   *
   * @return array of approximations to items associated with the given number of evenly-spaced normalized ranks.
   *
   * Deprecated. Will be removed in the next major version. Use get_quantile() instead.
   */
  std::vector<T, Allocator> get_quantiles(uint32_t num, bool inclusive = true) const;

  /**
   * Returns an approximation to the normalized rank of the given item from 0 to 1, inclusive.
   *
   * <p>The resulting approximation has a probabilistic guarantee that can be obtained from the
   * get_normalized_rank_error(false) function.
   *
   * <p>If the sketch is empty this throws std::runtime_error.
   *
   * @param item to be ranked
   * @param inclusive if true the weight of the given item is included into the rank.
   * Otherwise the rank equals the sum of the weights of all items that are less than the given item
   * according to the comparator C.
   * @return an approximate normalized rank of the given item
   */
  double get_rank(const T& item, bool inclusive = true) const;

  /**
   * Returns an approximation to the Probability Mass Function (PMF) of the input stream
   * given a set of split points (items).
   *
   * <p>The resulting approximations have a probabilistic guarantee that can be obtained from the
   * get_normalized_rank_error(true) function.
   *
   * <p>If the sketch is empty this throws std::runtime_error.
   *
   * @param split_points an array of <i>m</i> unique, monotonically increasing items
   * that divide the input domain into <i>m+1</i> consecutive disjoint intervals (bins).
   *
   * @param size of the array of split points.
   *
   * @param inclusive if true the rank of an item includes its own weight, and therefore
   * if the sketch contains items equal to a slit point, then in PMF such items are
   * included into the interval to the left of split point. Otherwise they are included into the interval
   * to the right of split point.
   *
   * @return an array of m+1 doubles each of which is an approximation
   * to the fraction of the input stream items (the mass) that fall into one of those intervals.
   */
  using vector_double = typename quantiles_sorted_view<T, Comparator, Allocator>::vector_double;
  vector_double get_PMF(const T* split_points, uint32_t size, bool inclusive = true) const;

  /**
   * Returns an approximation to the Cumulative Distribution Function (CDF), which is the
   * cumulative analog of the PMF, of the input stream given a set of split points (items).
   *
   * <p>The resulting approximations have a probabilistic guarantee that can be obtained from the
   * get_normalized_rank_error(false) function.
   *
   * <p>If the sketch is empty this throws std::runtime_error.
   *
   * @param split_points an array of <i>m</i> unique, monotonically increasing items
   * that divide the input domain into <i>m+1</i> consecutive disjoint intervals.
   *
   * @param size of the array of split points.
   *
   * @param inclusive if true the rank of an item includes its own weight, and therefore
   * if the sketch contains items equal to a slit point, then in CDF such items are
   * included into the interval to the left of split point. Otherwise they are included into
   * the interval to the right of split point.
   *
   * @return an array of m+1 double values, which are a consecutive approximation to the CDF
   * of the input stream given the split_points. The value at array position j of the returned
   * CDF array is the sum of the returned values in positions 0 through j of the returned PMF
   * array. This can be viewed as array of ranks of the given split points plus one more value
   * that is always 1.
   */
  vector_double get_CDF(const T* split_points, uint32_t size, bool inclusive = true) const;

  /**
   * Computes size needed to serialize the current state of the sketch.
   * This version is for fixed-size arithmetic types (integral and floating point).
   * @param sd instance of a SerDe
   * @return size in bytes needed to serialize this sketch
   */
  template<typename SerDe = serde<T>, typename TT = T, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
  size_t get_serialized_size_bytes(const SerDe& sd = SerDe()) const;

  /**
   * Computes size needed to serialize the current state of the sketch.
   * This version is for all other types and can be expensive since every item needs to be looked at.
   * @param sd instance of a SerDe
   * @return size in bytes needed to serialize this sketch
   */
  template<typename SerDe = serde<T>, typename TT = T, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
  size_t get_serialized_size_bytes(const SerDe& sd = SerDe()) const;

  /**
   * This method serializes the sketch into a given stream in a binary form
   * @param os output stream
   * @param sd instance of a SerDe
   */
  template<typename SerDe = serde<T>>
  void serialize(std::ostream& os, const SerDe& sd = SerDe()) const;

  // This is a convenience alias for users
  // The type returned by the following serialize method
  using vector_bytes = std::vector<uint8_t, typename std::allocator_traits<Allocator>::template rebind_alloc<uint8_t>>;

  /**
   * This method serializes the sketch as a vector of bytes.
   * An optional header can be reserved in front of the sketch.
   * It is a blank space of a given size.
   * This header is used in Datasketches PostgreSQL extension.
   * @param header_size_bytes space to reserve in front of the sketch
   * @param sd instance of a SerDe
   * @return serialized sketch as a vector of bytes
   */
  template<typename SerDe = serde<T>>
  vector_bytes serialize(unsigned header_size_bytes = 0, const SerDe& sd = SerDe()) const;

  /**
   * This method deserializes a sketch from a given stream.
   * @param is input stream
   * @param sd instance of a SerDe
   * @param comparator instance of a Comparator
   * @param allocator instance of an Allocator
   * @return an instance of a sketch
   */
  template<typename SerDe = serde<T>>
  static quantiles_sketch deserialize(std::istream& is, const SerDe& sd = SerDe(),
      const Comparator& comparator = Comparator(), const Allocator& allocator = Allocator());

  /**
   * This method deserializes a sketch from a given array of bytes.
   * @param bytes pointer to the array of bytes
   * @param size the size of the array
   * @param sd instance of a SerDe
   * @param comparator instance of a Comparator
   * @param allocator instance of an Allocator
   * @return an instance of a sketch
   */
  template<typename SerDe = serde<T>>
  static quantiles_sketch deserialize(const void* bytes, size_t size, const SerDe& sd = SerDe(),
      const Comparator& comparator = Comparator(), const Allocator& allocator = Allocator());

  /**
   * Gets the normalized rank error for this sketch. Constants were derived as the best fit to 99 percentile
   * empirically measured max error in thousands of trials.
   * @param is_pmf if true, returns the "double-sided" normalized rank error for the get_PMF() function.
   *               Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @return the normalized rank error for the sketch
   */
  double get_normalized_rank_error(bool is_pmf) const;

  /**
   * Gets the normalized rank error given k and pmf. Constants were derived as the best fit to 99 percentile
   * empirically measured max error in thousands of trials.
   * @param k  the configuration parameter
   * @param is_pmf if true, returns the "double-sided" normalized rank error for the get_PMF() function.
   *               Otherwise, it is the "single-sided" normalized rank error for all the other queries.
   * @return the normalized rank error for the given parameters
   */
  static double get_normalized_rank_error(uint16_t k, bool is_pmf);

  /**
   * Prints a summary of the sketch.
   * @param print_levels if true include information about levels
   * @param print_items if true include sketch data
   */
  string<Allocator> to_string(bool print_levels = false, bool print_items = false) const;

  class const_iterator;
  const_iterator begin() const;
  const_iterator end() const;

  quantiles_sorted_view<T, Comparator, Allocator> get_sorted_view() const;

private:
  using Level = std::vector<T, Allocator>;
  using VectorLevels = std::vector<Level, typename std::allocator_traits<Allocator>::template rebind_alloc<Level>>;

  /* Serialized sketch layout:
   * Long || Start Byte Addr:
   * Addr:
   *      ||       0        |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
   *  0   || Preamble_Longs | SerVer | FamID  |  Flags |----- K ---------|---- unused -----|
   *
   *      ||       8        |    9   |   10   |   11   |   12   |   13   |   14   |   15   |
   *  1   ||---------------------------Items Seen Count (N)--------------------------------|
   *
   * Long 3 is the start of data, beginning with serialized min and max item, followed by
   * the sketch data buffers.
   */

  static const size_t EMPTY_SIZE_BYTES = 8;
  static const uint8_t SERIAL_VERSION_1 = 1;
  static const uint8_t SERIAL_VERSION_2 = 2;
  static const uint8_t SERIAL_VERSION = 3;
  static const uint8_t FAMILY = 8;

  enum flags { RESERVED0, RESERVED1, IS_EMPTY, IS_COMPACT, IS_SORTED };

  static const uint8_t PREAMBLE_LONGS_SHORT = 1; // for empty
  static const uint8_t PREAMBLE_LONGS_FULL = 2;
  static const size_t DATA_START = 16;

  Comparator comparator_;
  Allocator allocator_;
  bool is_base_buffer_sorted_;
  uint16_t k_;
  uint64_t n_;
  uint64_t bit_pattern_;
  Level base_buffer_;
  VectorLevels levels_;
  T* min_item_;
  T* max_item_;
  mutable quantiles_sorted_view<T, Comparator, Allocator>* sorted_view_;

  void setup_sorted_view() const; // modifies mutable state
  void reset_sorted_view();

  // for deserialization
  class item_deleter;
  class items_deleter;
  quantiles_sketch(uint16_t k, uint64_t n, uint64_t bit_pattern,
      Level&& base_buffer, VectorLevels&& levels,
      std::unique_ptr<T, item_deleter> min_item, std::unique_ptr<T, item_deleter> max_item,
      bool is_sorted, const Comparator& comparator = Comparator(), const Allocator& allocator = Allocator());

  void grow_base_buffer();
  void process_full_base_buffer();

  // returns true if size adjusted, else false
  bool grow_levels_if_needed();

  // buffers should be pre-sized to target capacity as appropriate
  template<typename FwdV>
  static void in_place_propagate_carry(uint8_t starting_level, FwdV&& buf_size_k,
                                       Level& buf_size_2k, bool apply_as_update,
                                       quantiles_sketch& sketch);
  static void zip_buffer(Level& buf_in, Level& buf_out);
  static void merge_two_size_k_buffers(Level& arr_in_1, Level& arr_in_2, Level& arr_out, const Comparator& comparator);

  template<typename SerDe>
  static Level deserialize_array(std::istream& is, uint32_t num_items, uint32_t capcacity, const SerDe& serde, const Allocator& allocator);
  
  template<typename SerDe>
  static std::pair<Level, size_t> deserialize_array(const void* bytes, size_t size, uint32_t num_items, uint32_t capcacity, const SerDe& serde, const Allocator& allocator);

  static void check_k(uint16_t k);
  static void check_serial_version(uint8_t serial_version);
  static void check_header_validity(uint8_t preamble_longs, uint8_t flags_byte, uint8_t serial_version);
  static void check_family_id(uint8_t family_id);

  static uint32_t compute_retained_items(uint16_t k, uint64_t n);
  static uint32_t compute_base_buffer_items(uint16_t k, uint64_t n);
  static uint64_t compute_bit_pattern(uint16_t k, uint64_t n);
  static uint32_t count_valid_levels(uint64_t bit_pattern);
  static uint8_t compute_levels_needed(uint16_t k, uint64_t n);

 /**
  * Merges the src sketch into the tgt sketch with equal values of K.
  * src is modified only if elements can be moved out of it.
  */
  template<typename FwdSk>
  static void standard_merge(quantiles_sketch& tgt, FwdSk&& src);

 /**
  * Merges the src sketch into the tgt sketch with a smaller value of K.
  * However, it is required that the ratio of the two K values be a power of 2.
  * I.e., other.get_k() = this.get_k() * 2^(nonnegative integer).
  * src is modified only if elements can be moved out of it.
  */
  template<typename FwdSk>
  static void downsampling_merge(quantiles_sketch& tgt, FwdSk&& src);

  template<typename FwdV>
  static void zip_buffer_with_stride(FwdV&& buf_in, Level& buf_out, uint16_t stride);

  /**
   * Returns the zero-based bit position of the lowest zero bit of <i>bits</i> starting at
   * <i>startingBit</i>. If input is all ones, this returns 64.
   * @param bits the input bits as a long
   * @param starting_bit the zero-based starting bit position. Only the low 6 bits are used.
   * @return the zero-based bit position of the lowest zero bit starting at <i>startingBit</i>.
   */
  static uint8_t lowest_zero_bit_starting_at(uint64_t bits, uint8_t starting_bit);

  template<typename TT = T, typename std::enable_if<std::is_floating_point<TT>::value, int>::type = 0>
  static inline bool check_update_item(TT item) {
    return !std::isnan(item);
  }

  template<typename TT = T, typename std::enable_if<!std::is_floating_point<TT>::value, int>::type = 0>
  static inline bool check_update_item(TT) {
    return true;
  }
};


template<typename T, typename C, typename A>
class quantiles_sketch<T, C, A>::const_iterator: public std::iterator<std::input_iterator_tag, T> {
public:
  using value_type = std::pair<const T&, const uint64_t>;
  const_iterator& operator++();
  const_iterator& operator++(int);
  bool operator==(const const_iterator& other) const;
  bool operator!=(const const_iterator& other) const;
  const value_type operator*() const;
  const return_value_holder<value_type> operator->() const;
private:
  friend class quantiles_sketch<T, C, A>;
  using Level = std::vector<T, A>;
  using AllocLevel = typename std::allocator_traits<A>::template rebind_alloc<Level>;
  Level base_buffer_;
  std::vector<Level, AllocLevel> levels_;
  int level_;
  uint32_t index_;
  uint32_t bb_count_;
  uint64_t bit_pattern_;
  uint64_t weight_;
  uint16_t k_;
  const_iterator(const Level& base_buffer, const std::vector<Level, AllocLevel>& levels, uint16_t k, uint64_t n, bool is_end);
};

} /* namespace datasketches */

#include "quantiles_sketch_impl.hpp"

#endif // _QUANTILES_SKETCH_HPP_
