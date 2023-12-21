// Serialize example
// This example shows writing JSON string with writer directly.

#include "rapidjson/prettywriter.h" // for stringify JSON
#include <cstdio>
#include <string>
#include <vector>

using namespace rapidjson;

class Person {
public:
    Person(const std::string& name, unsigned age) : name_(name), age_(age) {}
    Person(const Person& rhs) : name_(rhs.name_), age_(rhs.age_) {}
    virtual ~Person();

    Person& operator=(const Person& rhs) {
        name_ = rhs.name_;
        age_ = rhs.age_;
        return *this;
    }

protected:
    template <typename Writer>
    void Serialize(Writer& writer) const {
        // This base class just write out name-value pairs, without wrapping within an object.
        writer.String("name");
#if RAPIDJSON_HAS_STDSTRING
        writer.String(name_);
#else
        writer.String(name_.c_str(), static_cast<SizeType>(name_.length())); // Supplying length of string is faster.
#endif
        writer.String("age");
        writer.Uint(age_);
    }

private:
    std::string name_;
    unsigned age_;
};

Person::~Person() {
}

class Education {
public:
    Education(const std::string& school, double GPA) : school_(school), GPA_(GPA) {}
    Education(const Education& rhs) : school_(rhs.school_), GPA_(rhs.GPA_) {}

    template <typename Writer>
    void Serialize(Writer& writer) const {
        writer.StartObject();
        
        writer.String("school");
#if RAPIDJSON_HAS_STDSTRING
        writer.String(school_);
#else
        writer.String(school_.c_str(), static_cast<SizeType>(school_.length()));
#endif

        writer.String("GPA");
        writer.Double(GPA_);

        writer.EndObject();
    }

private:
    std::string school_;
    double GPA_;
};

class Dependent : public Person {
public:
    Dependent(const std::string& name, unsigned age, Education* education = 0) : Person(name, age), education_(education) {}
    Dependent(const Dependent& rhs) : Person(rhs), education_(0) { education_ = (rhs.education_ == 0) ? 0 : new Education(*rhs.education_); }
    virtual ~Dependent();

    Dependent& operator=(const Dependent& rhs) {
        if (this == &rhs)
            return *this;
        delete education_;
        education_ = (rhs.education_ == 0) ? 0 : new Education(*rhs.education_);
        return *this;
    }

    template <typename Writer>
    void Serialize(Writer& writer) const {
        writer.StartObject();

        Person::Serialize(writer);

        writer.String("education");
        if (education_)
            education_->Serialize(writer);
        else
            writer.Null();

        writer.EndObject();
    }

private:

    Education *education_;
};

Dependent::~Dependent() {
    delete education_; 
}

class Employee : public Person {
public:
    Employee(const std::string& name, unsigned age, bool married) : Person(name, age), dependents_(), married_(married) {}
    Employee(const Employee& rhs) : Person(rhs), dependents_(rhs.dependents_), married_(rhs.married_) {}
    virtual ~Employee();

    Employee& operator=(const Employee& rhs) {
        static_cast<Person&>(*this) = rhs;
        dependents_ = rhs.dependents_;
        married_ = rhs.married_;
        return *this;
    }

    void AddDependent(const Dependent& dependent) {
        dependents_.push_back(dependent);
    }

    template <typename Writer>
    void Serialize(Writer& writer) const {
        writer.StartObject();

        Person::Serialize(writer);

        writer.String("married");
        writer.Bool(married_);

        writer.String(("dependents"));
        writer.StartArray();
        for (std::vector<Dependent>::const_iterator dependentItr = dependents_.begin(); dependentItr != dependents_.end(); ++dependentItr)
            dependentItr->Serialize(writer);
        writer.EndArray();

        writer.EndObject();
    }

private:
    std::vector<Dependent> dependents_;
    bool married_;
};

Employee::~Employee() {
}

int main(int, char*[]) {
    std::vector<Employee> employees;

    employees.push_back(Employee("Milo YIP", 34, true));
    employees.back().AddDependent(Dependent("Lua YIP", 3, new Education("Happy Kindergarten", 3.5)));
    employees.back().AddDependent(Dependent("Mio YIP", 1));

    employees.push_back(Employee("Percy TSE", 30, false));

    StringBuffer sb;
    PrettyWriter<StringBuffer> writer(sb);

    writer.StartArray();
    for (std::vector<Employee>::const_iterator employeeItr = employees.begin(); employeeItr != employees.end(); ++employeeItr)
        employeeItr->Serialize(writer);
    writer.EndArray();

    puts(sb.GetString());

    return 0;
}
