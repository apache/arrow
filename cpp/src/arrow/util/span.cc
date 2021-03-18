#include "arrow/util/span.h"

#include <chrono>
#include <cstdint>
#include <sstream>
#include <thread>
#include <unordered_map>

#include "arrow/util/logging.h"

namespace arrow {

int64_t GetNowNs() {
  std::chrono::time_point<std::chrono::steady_clock, std::chrono::nanoseconds> now(
      std::chrono::steady_clock::now());
  return now.time_since_epoch().count();
}

class Span::Impl {
 public:
  explicit Impl(const std::string& name)
      : name_(name), thread_id_(std::this_thread::get_id()), start_ns_(GetNowNs()) {}

  void AddAttribute(const std::string& key, const std::string& value) {
    attributes_.insert({key, value});
  }

  void End() {
    const auto end_ns = GetNowNs();
    ARROW_LOG(DEBUG) << "span"
                     << " name=" << name_ << ";thread_id=" << thread_id_
                     << ";start=" << start_ns_ << ";end=" << end_ns
                     << AttributesKeyValueString();
  }

  std::string AttributesKeyValueString() const {
    if (attributes_.empty()) {
      return "";
    }
    std::stringstream result;
    for (const auto& pair : attributes_) {
      result << ';' << pair.first << '=' << pair.second;
    }
    return result.str();
  }

 private:
  std::string name_;
  std::thread::id thread_id_;
  int64_t start_ns_;
  std::unordered_map<std::string, std::string> attributes_;
};

Span::Span(const std::string& name) : impl_(new Impl(name)) {}
Span::~Span() { End(); }

void Span::AddAttribute(const std::string& key, const std::string& value) {
  impl_->AddAttribute(key, value);
}

void Span::End() { impl_->End(); }

}  // namespace arrow
