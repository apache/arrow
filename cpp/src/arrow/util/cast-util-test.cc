#include "arrow/util/cast-util.h"

#include <cstdint>
#include <iostream>

#include "gtest/gtest.h"
#include "arrow/test-util.h"
#include "arrow/status.h"

namespace arrow{
namespace internal{
TEST(CastUtilTest,CastTest){
    const int * a = new int();
    const int * b ;
    ConvertPhysicalType<int, int>(a , sizeof(int) , &b);
    EXPECT_EQ(*a == *b , true);
    
    const int * a_ = new int();
    const double * b_ ;
    ConvertPhysicalType<int, double>(a_ , sizeof(int) , &b_);
    EXPECT_EQ(a == b , true);
}
}
}
