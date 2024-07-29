#include "arrow/adapters/orc/statistics.h"


namespace arrow::adapters::orc {

  std::string ColumnStatistics::ToString() const {
    std::ostringstream buf;
    buf << "Column contains " << GetValueCount()
        << " values. Has null value: " << (HasNull()? "yes": "no") << std::endl;
    return buf.str();
  }

  std::string DoubleColumnStatistics::ToString() const {
    std::ostringstream buf;
    buf << "Column contains " << GetValueCount()
        << " values. Has null value: " << (HasNull()? "yes": "no") << std::endl;

    if (HasMin()) { buf << "Min: " << GetMin() << std::endl; }
    if (HasMax()) { buf << "Max: " << GetMax() << std::endl; }
    if (HasSum()) { buf << "Sum: " << GetSum() << std::endl; }
    return buf.str();
  }

  std::string IntegerColumnStatistics::ToString() const {
    std::ostringstream buf;
    buf << "Column contains " << GetValueCount()
        << " values. Has null value: " << (HasNull()? "yes": "no") << std::endl;

    if (HasMin()) { buf << "Min: " << GetMin() << std::endl; }
    if (HasMax()) { buf << "Max: " << GetMax() << std::endl; }
    if (HasSum()) { buf << "Sum: " << GetSum() << std::endl; }
    return buf.str();
  }

} // namespace arrow::adapters::orc
