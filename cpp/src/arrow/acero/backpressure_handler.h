#pragma once
#include "arrow/acero/options.h"

#include <memory>

namespace arrow::acero {

class BackpressureHandler {
 private:
  BackpressureHandler(size_t low_threshold, size_t high_threshold,
                      std::unique_ptr<BackpressureControl> backpressure_control)
      : low_threshold_(low_threshold),
        high_threshold_(high_threshold),
        backpressure_control_(std::move(backpressure_control)) {}

 public:
  static Result<BackpressureHandler> Make(
      size_t low_threshold, size_t high_threshold,
      std::unique_ptr<BackpressureControl> backpressure_control) {
    if (low_threshold >= high_threshold) {
      return Status::Invalid("low threshold (", low_threshold,
                             ") must be less than high threshold (", high_threshold, ")");
    }
    if (backpressure_control == NULLPTR) {
      return Status::Invalid("null backpressure control parameter");
    }
    BackpressureHandler backpressure_handler(low_threshold, high_threshold,
                                             std::move(backpressure_control));
    return std::move(backpressure_handler);
  }

  void Handle(size_t start_level, size_t end_level) {
    if (start_level < high_threshold_ && end_level >= high_threshold_) {
      backpressure_control_->Pause();
    } else if (start_level > low_threshold_ && end_level <= low_threshold_) {
      backpressure_control_->Resume();
    }
  }

 private:
  size_t low_threshold_;
  size_t high_threshold_;
  std::unique_ptr<BackpressureControl> backpressure_control_;
};

}  // namespace arrow::acero
