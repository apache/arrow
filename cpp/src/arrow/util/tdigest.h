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

// approximate quantiles from arbitrary length dataset with O(1) space
// based on 'Computing Extremely Accurate Quantiles Using t-Digests' from Dunning & Ertl
// - https://arxiv.org/abs/1902.04023
// - https://github.com/tdunning/t-digest

#pragma once

#include <algorithm>
#include <cmath>
#include <queue>
#include <vector>

#include "arrow/util/logging.h"

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

namespace arrow {
namespace internal {

namespace detail {

// a numerically stable lerp is unbelievably complex
// but we are *approximating* the quantile, so let's keep it simple
double Lerp(double a, double b, double t) { return a + t * (b - a); }

// histogram bin
struct Centroid {
  double mean;
  double weight;  // # data points in this bin

  // merge with another centroid
  void Merge(const Centroid& centroid) {
    weight += centroid.weight;
    mean += (centroid.mean - mean) * centroid.weight / weight;
  }
};

// scale function K0: linear function, as baseline
struct ScalerK0 {
  explicit ScalerK0(uint32_t delta) : delta_norm(delta / 2.0) {}

  double K(double q) const { return delta_norm * q; }
  double Q(double k) const { return k / delta_norm; }

  const double delta_norm;
};

// scale function K1
struct ScalerK1 {
  explicit ScalerK1(uint32_t delta) : delta_norm(delta / (2.0 * M_PI)) {}

  double K(double q) const { return delta_norm * std::asin(2 * q - 1); }
  double Q(double k) const { return (std::sin(k / delta_norm) + 1) / 2; }

  const double delta_norm;
};

// implements t-digest merging algorithm
template <class T = ScalerK1>
class TDigestMerger : private T {
 public:
  explicit TDigestMerger(uint32_t delta) : T(delta) {}

  void Reset(double total_weight, std::vector<Centroid>* tdigest) {
    total_weight_ = total_weight;
    tdigest_ = tdigest;
    tdigest_->resize(0);
    weight_so_far_ = 0;
    weight_limit_ = -1;  // trigger first centroid merge
  }

  // merge one centroid from a sorted centroid stream
  void Add(const Centroid& centroid) {
    auto& td = *tdigest_;
    const double weight = weight_so_far_ + centroid.weight;
    if (weight <= weight_limit_) {
      td[td.size() - 1].Merge(centroid);
    } else {
      const double quantile = weight_so_far_ / total_weight_;
      const double next_weight_limit = total_weight_ * this->Q(this->K(quantile) + 1);
      // weight limit should be strictly increasing, until the last centroid
      if (next_weight_limit <= weight_limit_) {
        weight_limit_ = total_weight_;
      } else {
        weight_limit_ = next_weight_limit;
      }
      td.push_back(centroid);  // should never exceed capacity and trigger reallocation
    }
    weight_so_far_ = weight;
  }

  // verify k-size of a tdigest
  bool Verify(const std::vector<Centroid>* tdigest, double total_weight) const {
    const auto& td = *tdigest;
    double q_prev = 0, k_prev = this->K(0);
    for (size_t i = 0; i < td.size(); ++i) {
      const double q = q_prev + td[i].weight / total_weight;
      const double k = this->K(q);
      if (td[i].weight != 1 && (k - k_prev) > 1.001) {
        ARROW_LOG(ERROR) << "oversized centroid, k2 - k1 = " << (k - k_prev);
        return false;
      }
      k_prev = k;
      q_prev = q;
    }
    return true;
  }

 private:
  double total_weight_;   // total weight of this tdigest
  double weight_so_far_;  // accumulated weight till current bin
  double weight_limit_;   // max accumulated weight to move to next bin
  std::vector<Centroid>* tdigest_;
};

}  // namespace detail

class TDigest {
 public:
  explicit TDigest(uint32_t delta = kDelta, uint32_t buffer_size = kBufferSize)
      : delta_(delta), merger_(delta) {
    if (delta < 10) {
      ARROW_LOG(WARNING) << "delta is too small, increased to 10";
      delta = 10;
    }
    if (buffer_size < 50 || buffer_size < delta) {
      ARROW_LOG(INFO) << "increase buffer size may improve performance";
    }

    // pre-allocate input and tdigest buffers, no dynamic memory allocation at runtime
    input_.reserve(buffer_size);
    tdigest_buffers_[0].reserve(delta);
    tdigest_buffers_[1].reserve(delta);
    tdigest_ = &tdigest_buffers_[0];
    tdigest_next_ = &tdigest_buffers_[1];

    Reset();
  }

  // no copy/move/assign due to some dirty pointer tricks
  TDigest(const TDigest&) = delete;
  TDigest(TDigest&&) = delete;
  TDigest& operator=(const TDigest&) = delete;
  TDigest& operator=(TDigest&&) = delete;

  // reset and re-use this tdigest
  void Reset() {
    input_.resize(0);
    tdigest_->resize(0);
    tdigest_next_->resize(0);
    total_weight_ = 0;
    min_ = std::numeric_limits<double>::max();
    max_ = std::numeric_limits<double>::lowest();
  }

  // verify data integrity, only for test
  bool Verify() {
    MergeInput();
    // check weight, centroid order
    double total_weight = 0, prev_mean = std::numeric_limits<double>::lowest();
    for (const auto& centroid : *tdigest_) {
      if (std::isnan(centroid.mean) || std::isnan(centroid.weight)) {
        ARROW_LOG(ERROR) << "NAN found in tdigest";
        return false;
      }
      if (centroid.mean < prev_mean) {
        ARROW_LOG(ERROR) << "centroid mean decreases";
        return false;
      }
      if (centroid.weight < 1) {
        ARROW_LOG(ERROR) << "invalid centroid weight";
        return false;
      }
      prev_mean = centroid.mean;
      total_weight += centroid.weight;
    }
    if (total_weight != total_weight_) {
      ARROW_LOG(ERROR) << "tdigest total weight mismatch";
      return false;
    }
    // check if buffer expanded
    if (tdigest_->capacity() > delta_) {
      ARROW_LOG(ERROR) << "oversized tdigest buffer";
      return false;
    }
    // check k-size
    return merger_.Verify(tdigest_, total_weight_);
  }

  // dump internal data, only for debug
  void Dump() {
    MergeInput();
    const auto& td = *tdigest_;
    for (size_t i = 0; i < td.size(); ++i) {
      std::cout << i << ": mean = " << td[i].mean << ", weight = " << td[i].weight
                << std::endl;
    }
    std::cout << "min = " << min_ << ", max = " << max_ << std::endl;
    const size_t total_memory =
        2 * td.capacity() * sizeof(detail::Centroid) + input_.capacity() * sizeof(double);
    std::cout << "total memory = " << total_memory << " bytes" << std::endl;
  }

  // buffer a single data point, consume internal buffer if full
  // this function is intensively called and performance critical
  void Add(double value) {
    DCHECK(!std::isnan(value)) << "cannot add NAN";
    if (ARROW_PREDICT_FALSE(input_.size() == input_.capacity())) {
      MergeInput();
    }
    input_.push_back(value);
  }

  // merge with other t-digests, called infrequently
  void Merge(std::vector<TDigest*>& tdigests) {
    // current and end iterator
    using CentroidIter = std::vector<detail::Centroid>::const_iterator;
    using CentroidIterPair = std::pair<CentroidIter, CentroidIter>;
    // use a min-heap to find next minimal centroid from all tdigests
    auto centroid_gt = [](const CentroidIterPair& lhs, const CentroidIterPair& rhs) {
      return lhs.first->mean > rhs.first->mean;
    };
    using CentroidQueue =
        std::priority_queue<CentroidIterPair, std::vector<CentroidIterPair>,
                            decltype(centroid_gt)>;

    // trivial dynamic memory allocated at runtime
    std::vector<CentroidIterPair> queue_buffer;
    queue_buffer.reserve(tdigests.size() + 1);
    CentroidQueue queue(std::move(centroid_gt), std::move(queue_buffer));

    MergeInput();
    if (tdigest_->size() > 0) {
      queue.emplace(tdigest_->cbegin(), tdigest_->cend());
    }
    for (TDigest* td : tdigests) {
      td->MergeInput();
      if (td->tdigest_->size() > 0) {
        queue.emplace(td->tdigest_->cbegin(), td->tdigest_->cend());
        total_weight_ += td->total_weight_;
        min_ = std::min(min_, td->min_);
        max_ = std::max(max_, td->max_);
      }
    }

    merger_.Reset(total_weight_, tdigest_next_);

    CentroidIter current_iter, end_iter;
    // do k-way merge till one buffer left
    while (queue.size() > 1) {
      std::tie(current_iter, end_iter) = queue.top();
      merger_.Add(*current_iter);
      queue.pop();
      if (++current_iter != end_iter) {
        queue.emplace(current_iter, end_iter);
      }
    }
    // merge last buffer
    if (!queue.empty()) {
      std::tie(current_iter, end_iter) = queue.top();
      while (current_iter != end_iter) {
        merger_.Add(*current_iter++);
      }
    }

    std::swap(tdigest_, tdigest_next_);
  }

  // an ugly helper
  void Merge(std::vector<std::unique_ptr<TDigest>>& ptrs) {
    std::vector<TDigest*> tdigests;
    tdigests.reserve(ptrs.size());
    for (auto& ptr : ptrs) {
      tdigests.push_back(ptr.get());
    }
    Merge(tdigests);
  }

  // calculate quantile
  double Quantile(double q) {
    MergeInput();

    const auto& td = *tdigest_;

    if (q < 0 || q > 1) {
      ARROW_LOG(ERROR) << "quantile must be between 0 and 1";
      return NAN;
    }
    if (td.size() == 0) {
      ARROW_LOG(WARNING) << "empty tdigest";
      return NAN;
    }

    const double index = q * total_weight_;
    if (index <= 1) {
      return min_;
    } else if (index >= total_weight_ - 1) {
      return max_;
    }

    // find centroid contains the index
    uint32_t ci = 0;
    double weight_sum = 0;
    for (; ci < td.size(); ++ci) {
      weight_sum += td[ci].weight;
      if (index <= weight_sum) {
        break;
      }
    }
    DCHECK_LT(ci, td.size());

    // deviation of index from the centroid center
    double diff = index + td[ci].weight / 2 - weight_sum;

    // index happen to be in a unit weight centroid
    if (td[ci].weight == 1 && std::abs(diff) < 0.5) {
      return td[ci].mean;
    }

    // find adjacent centroids for interpolation
    uint32_t ci_left = ci, ci_right = ci;
    if (diff > 0) {
      if (ci_right == td.size() - 1) {
        // index larger than center of last bin
        DCHECK_EQ(weight_sum, total_weight_);
        const detail::Centroid* c = &td[ci_right];
        DCHECK_GE(c->weight, 2);
        return detail::Lerp(c->mean, max_, diff / (c->weight / 2));
      }
      ++ci_right;
    } else {
      if (ci_left == 0) {
        // index smaller than center of first bin
        const detail::Centroid* c = &td[0];
        DCHECK_GE(c->weight, 2);
        return detail::Lerp(min_, c->mean, index / (c->weight / 2));
      }
      --ci_left;
      diff += td[ci_left].weight / 2 + td[ci_right].weight / 2;
    }

    // interpolate from adjacent centroids
    diff /= (td[ci_left].weight / 2 + td[ci_right].weight / 2);
    return detail::Lerp(td[ci_left].mean, td[ci_right].mean, diff);
  }

 private:
  // merge input data with current tdigest
  void MergeInput() {
    if (input_.size() == 0) {
      return;
    }

    total_weight_ += input_.size();
    merger_.Reset(total_weight_, tdigest_next_);

    std::sort(input_.begin(), input_.end());
    min_ = std::min(min_, input_.front());
    max_ = std::max(max_, input_.back());

    // pick next minimal centroid from input and tdigest, feed to merger
    const auto& td = *tdigest_;
    uint32_t tdigest_index = 0, input_index = 0;
    while (tdigest_index < td.size() && input_index < input_.size()) {
      if (td[tdigest_index].mean < input_[input_index]) {
        merger_.Add(td[tdigest_index++]);
      } else {
        merger_.Add(detail::Centroid{input_[input_index++], 1});
      }
    }
    while (tdigest_index < td.size()) {
      merger_.Add(td[tdigest_index++]);
    }
    while (input_index < input_.size()) {
      merger_.Add(detail::Centroid{input_[input_index++], 1});
    }

    input_.resize(0);
    std::swap(tdigest_, tdigest_next_);
  }

  const uint32_t delta_;

  detail::TDigestMerger<> merger_;
  double total_weight_;
  double min_, max_;

  // input buffer, size = buffer_size * sizeof(double)
  std::vector<double> input_;
  // ping-pong buffer holds two tdigests, size = 2 * delta * sizeof(Centroid)
  std::vector<detail::Centroid> tdigest_buffers_[2];
  std::vector<detail::Centroid>*tdigest_, *tdigest_next_;

  // default parameters
  static constexpr uint32_t kDelta = 100;       // compression
  static constexpr uint32_t kBufferSize = 500;  // input buffer
};

}  // namespace internal
}  // namespace arrow
