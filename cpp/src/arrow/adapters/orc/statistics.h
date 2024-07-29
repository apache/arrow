#pragma once

#include <fstream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include "arrow/type.h"
#include "arrow/util/io_util.h"

namespace arrow::adapters::orc {

class NoValueError : public std::runtime_error{
  public:
    explicit NoValueError(const std::string& what) : std::runtime_error(what) {};
};

template<typename T>
class InternalTypedStats {
  private:
    T min_;
    T max_;
    T sum_;

  public:
    T GetMin() { return min_; }
    T GetMax() { return max_; }
    T GetSum() { return sum_; }

    InternalTypedStats(T min, T max, T sum): min_(min), max_(max), sum_(sum) {};
};

using InternalIntStats = InternalTypedStats<int64_t>;
using InternalDoubleStats = InternalTypedStats<double>;

class ColumnStatistics {
  protected:
    bool hasNull_{};
    bool hasMin_{};
    bool hasMax_{};
    bool hasSum_{};
    bool hasTotalLength_{};
    uint64_t totalLength_{};
    uint64_t valueCount_{};
    Type::type kind_{Type::NA};

  public:
    // getters
    bool HasNull() const { return hasNull_; }
    bool HasMin() const { return hasMin_; }
    bool HasMax() const { return hasMax_; }
    bool HasSum() const { return hasSum_; }
    bool HasTotalLength() const { return hasTotalLength_; }
    uint64_t GetTotalLength() const { return totalLength_; }
    uint64_t GetValueCount() const { return valueCount_; }
    Type::type GetType() const {return kind_; }

    //setters
    void SetHasNull(bool value) { hasNull_ = value; }
    void SetHasMin(bool value) { hasMin_ = value; }
    void SetHasMax(bool value) { hasMax_ = value; }
    void SetHasSum(bool value) { hasSum_ = value; }
    void SetHasTotalLength(bool value) { hasTotalLength_ = value; }
    void SetTotalLength(uint64_t value) { totalLength_ = value; }
    void SetValueCount(uint64_t value) { valueCount_ = value; }
    void SetType(Type::type colType) { kind_ = colType; }

    virtual std::string ToString() const;

    ColumnStatistics() = default;
    virtual ~ColumnStatistics() = default;
};

class DoubleColumnStatistics : public ColumnStatistics {
  private:
    std::unique_ptr<InternalDoubleStats> stats_;

  public:
    void SetInternalStatistics(InternalDoubleStats stats) {
      stats_ = std::make_unique<InternalDoubleStats>(stats);
    }

    double GetMin() const {
      if (HasMin()) { return stats_->GetMin(); }
      throw NoValueError("Min is not set.");
    }

    double GetMax() const {
      if (HasMax()) { return stats_->GetMax(); }
      throw NoValueError("Max is not set.");
    }

    double GetSum() const {
      if (HasSum()) { return stats_->GetSum(); }
      throw NoValueError("Sum is not set.");
    }

    std::string ToString() const override;
};

class IntegerColumnStatistics : public ColumnStatistics {
  private:
    std::unique_ptr<InternalIntStats> stats_;

  public:
    void SetInternalStatistics(InternalIntStats stats) {
        stats_ = std::make_unique<InternalIntStats>(stats);
    }

    int64_t GetMin() const {
      if (HasMin()) { return stats_->GetMin(); }
      throw NoValueError("Min is not set.");
    }

    int64_t GetMax() const {
      if (HasMax()) { return stats_->GetMax(); }
      throw NoValueError("Max is not set.");
    }

    int64_t GetSum() const {
      if (HasSum()) { return stats_->GetSum(); }
      throw NoValueError("Sum is not set.");
    }

    std::string ToString() const override;
};

class Statistics {
  private:
    std::vector<ColumnStatistics*> colStats_;
  public:
    virtual ~Statistics() = default;
    Statistics() = default;

    const ColumnStatistics* GetColumnStatistics(uint32_t columnId) {
      return colStats_[columnId];
    }

    uint32_t GetColumnCount() {
      return static_cast<uint32_t>(colStats_.size());
    }

    virtual void SetColumnStatistics(std::vector<ColumnStatistics*>& colStats) {
      colStats_ = colStats;
    }
};

class StripeStatistics: public Statistics {
  private:
    std::unique_ptr<Statistics> stripeStats_;
    std::vector<std::vector<ColumnStatistics*>> rowIndexStats_;
  public:
    ~StripeStatistics() override = default;
    StripeStatistics() = default;

    const ColumnStatistics* GetColumnStatistics(uint32_t columnId) {
      return stripeStats_->GetColumnStatistics(columnId);
    }

    uint32_t GetColumnCount() {
      return stripeStats_->GetColumnCount();
    }

    void SetColumnStatistics(std::vector<ColumnStatistics*>& colStats) override {
      stripeStats_ = std::make_unique<Statistics>();
      stripeStats_->SetColumnStatistics(colStats);
    }

    const ColumnStatistics* GetRowIndexStatistics(uint32_t columnIndex,
                                                  uint32_t rowIndex) {
      return rowIndexStats_[columnIndex][rowIndex];
    }

    uint32_t GetRowIndexStatCount(uint32_t columnIndex) {
      return static_cast<uint32_t>(rowIndexStats_[columnIndex].size());
    }

    void SetRowIndexStats(
        std::vector<std::vector<ColumnStatistics*>> riStats) {
      rowIndexStats_ = riStats;
    }
};

} // namespace arrow::adapters::orc
