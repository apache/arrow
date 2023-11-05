

## std::decay
用于获取类型的衰减类型（decay type）。衰减类型是指将数组类型转换为指针类型、将函数类型转换
为函数指针类型，并移除引用修饰符和顶层 const 修饰符的结果类型
template <class T>
struct decay {
    using type = typename std::remove_reference<T>::type;
};

## std::declval
主要用于在编译时获取一个类型的临时值，即使该类型没有默认构造函数或不可复制。它不
会创建对象，只是返回一个 T 类型的右值引用。由于 std::declval 是一个右值引用，因此
它适用于需要右值引用的上下文，例如在模板中推断返回类型和进行类型转换

## 模板特化:
1. 类模板的部分特化：
// 通用的类模板定义
template <typename T, typename U>
struct MyTemplate {
    void print() {
        std::cout << "General template" << std::endl;
    }
};

// 针对特定类型的部分特化
template <typename T>
struct MyTemplate<T, int> {
    void print() {
        std::cout << "Specialized template for int" << std::endl;
    }
};

2. 函数模板的重载特化：
// 通用的函数模板定义
template <typename T>
void myFunction(T t) {
    std::cout << "General template" << std::endl;
}

// 针对特定类型的重载特化
template <>
void myFunction<int>(int t) {
    std::cout << "Specialized template for int" << std::endl;
}

## 折叠表达式（Fold Expressions）
// C++11 通过递归展开模板参数
template<typename... Args>
void printValues(Args... args) {
    (std::cout << ... << args) << std::endl;
}

// C++17 使用折叠表达式展开模板参数
template<typename... Args>
void printValues(Args... args) {
    ((std::cout << args), ...);
    std::cout << std::endl;
}

## std::is_void
接受一个类型 T 作为模板参数，并提供了一个静太成员常量 value，该常量表示类型 T 是否为
void。如果 value 为 true，则说明 T 是 void 类型；如果 value 为 false，则说明 T 不是
void 类型

template <class T>
struct is_void : std::false_type {};

template <>
struct is_void<void> : std::true_type {};


## std::enable_if
用于在编译时根据条件来启用或禁用函数重载
template <bool B, class T = void> // 通用版本的模板
struct enable_if {};

template <class T> // 特化版本的模板
struct enable_if<true, T> {
    using type = T;
};
std::enable_if 通过模板特化实现了条件分支。当 B 的值为 true 时，
std::enable_if<B, T>::type 将是类型 T；
当 B 的值为 false 时，std::enable_if<B, T> 不提供 type 成员

## std::is_same
用于在编译时检查两个类型是否相同。它的定义如下：

template <class T, class U>
struct is_same : std::false_type {};

template <class T>
struct is_same<T, T> : std::true_type {};

std::is_same 接受两个类型 T 和 U 作为模板参数，并提供了一个静态成员常量 value，该常量表示这
两个类型是否相同。如果 value 为 true，则说明 T 和 U 是相同的类型；如果 value 为 false，则
说明 T 和 U 是不同的类型。