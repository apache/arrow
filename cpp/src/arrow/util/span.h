
#pragma once

#include <memory>
#include <string>

namespace arrow {

class Span final {
 public:
  explicit Span(const std::string& name);
  ~Span();

  void AddAttribute(const std::string& key, const std::string& value);
  void End();

 private:
  class Impl;

  std::shared_ptr<Impl> impl_;
};

}  // namespace arrow