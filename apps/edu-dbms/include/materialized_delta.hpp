#pragma once
#include "edudb.hpp"
#include <array>

namespace edudb::mv {
// An exact signed sum of finite INTEGER/IEEE754 binary64 values, represented in
// units of 2^-1074. The fixed 2304-bit magnitude covers DBL_MAX * INT64_MAX.
// It prevents AVG's finite result from inheriting a double-SUM overflow.
class ExactNumeric {
    static constexpr std::size_t words=72;
    std::array<std::uint32_t,words> magnitude_{};
    bool negative_=false;
    static int compare(const std::array<std::uint32_t,words>&,const std::array<std::uint32_t,words>&);
    static void subtract(std::array<std::uint32_t,words>&,const std::array<std::uint32_t,words>&);
    static int highest(const std::array<std::uint32_t,words>&);
    static bool bit(const std::array<std::uint32_t,words>&,int);
public:
    void add(const json& value,int direction=1);
    std::int64_t integer()const;
    double real(std::uint64_t denominator=1)const;
    json state()const;
    static ExactNumeric restore(const json&);
};
struct AggregateSpec {std::string kind,type;};
class AggregateGroup {
    struct Cell{std::uint64_t count=0;ExactNumeric sum;};
    std::vector<AggregateSpec> specs_;
    std::vector<Cell> cells_;
    std::uint64_t rows_=0;
public:
    explicit AggregateGroup(std::vector<AggregateSpec>);
    // Each argument is the already-bound aggregate argument for one source row.
    // COUNT(*) uses a non-null constant. Changes are atomic even on bad input.
    void change(const json& arguments,int direction);
    std::uint64_t rows()const{return rows_;}
    json values()const;
    json state()const;
    static AggregateGroup restore(const json&,const std::vector<AggregateSpec>&);
};
} // namespace edudb::mv
