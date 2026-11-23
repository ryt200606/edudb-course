#include "materialized_delta.hpp"
#include <cmath>
#include <cstring>
#include <limits>

namespace edudb::mv {
static_assert(sizeof(double)==8&&std::numeric_limits<double>::is_iec559,"Exact numeric requires IEEE754 binary64 double");
int ExactNumeric::compare(const std::array<std::uint32_t,words>& a,const std::array<std::uint32_t,words>& b){for(std::size_t n=words;n-->0;)if(a[n]!=b[n])return a[n]<b[n]?-1:1;return 0;}
void ExactNumeric::subtract(std::array<std::uint32_t,words>& a,const std::array<std::uint32_t,words>& b){std::uint64_t borrow=0;for(std::size_t n=0;n<words;++n){const auto sub=std::uint64_t(b[n])+borrow;const auto before=std::uint64_t(a[n]);a[n]=std::uint32_t(before-sub);borrow=before<sub;}if(borrow)throw DbError("MATERIALIZED_STATE","Exact accumulator subtraction underflow");}
int ExactNumeric::highest(const std::array<std::uint32_t,words>& a){for(std::size_t n=words;n-->0;)if(a[n]){for(int b=31;b>=0;--b)if(a[n]&(std::uint32_t(1)<<b))return int(n*32)+b;}return -1;}
bool ExactNumeric::bit(const std::array<std::uint32_t,words>& a,int n){return n>=0&&std::size_t(n/32)<words&&(a[std::size_t(n/32)]&(std::uint32_t(1)<<(n%32)));}
void ExactNumeric::add(const json& value,int direction){
    if(direction!=1&&direction!=-1)throw DbError("MATERIALIZED_STATE","Delta direction must be +1 or -1");
    if(value.is_null())return;
    std::uint64_t significand=0;int shift=0;bool negative=false;
    if(value.is_number_integer()){
        if(value.is_number_unsigned()&&value.get<std::uint64_t>()>std::uint64_t(std::numeric_limits<std::int64_t>::max()))throw DbError("NUMERIC_OVERFLOW","Unsigned input exceeds SQL INTEGER");
        auto integer=value.get<std::int64_t>();negative=integer<0;significand=negative?std::uint64_t(0)-std::uint64_t(integer):std::uint64_t(integer);shift=1074;
    }else if(value.is_number_float()){
        const double number=value.get<double>();if(!std::isfinite(number))throw DbError("NUMERIC_OVERFLOW","Exact accumulator accepts finite REAL values only");std::uint64_t bits=0;std::memcpy(&bits,&number,sizeof(bits));negative=bool(bits>>63);const auto exponent=unsigned((bits>>52)&2047);significand=bits&((std::uint64_t(1)<<52)-1);if(exponent){significand|=std::uint64_t(1)<<52;shift=int(exponent)-1;}
    }else throw DbError("TYPE_ERROR","SUM/AVG delta input requires numeric values");
    if(!significand)return;
    if(direction<0)negative=!negative;
    std::array<std::uint32_t,words> term{};for(int n=0;n<64;++n)if(significand&(std::uint64_t(1)<<n)){const auto position=shift+n;if(position<0||std::size_t(position/32)>=words)throw DbError("NUMERIC_OVERFLOW","Exact accumulator range exceeded");term[std::size_t(position/32)]|=std::uint32_t(1)<<(position%32);}
    if(negative_==negative){std::uint64_t carry=0;for(std::size_t n=0;n<words;++n){const auto total=std::uint64_t(magnitude_[n])+term[n]+carry;magnitude_[n]=std::uint32_t(total);carry=total>>32;}if(carry)throw DbError("NUMERIC_OVERFLOW","Exact accumulator magnitude exceeded");}
    else{const auto order=compare(magnitude_,term);if(order>=0)subtract(magnitude_,term);else{subtract(term,magnitude_);magnitude_=term;negative_=negative;}}
    if(highest(magnitude_)<0)negative_=false;
}
std::int64_t ExactNumeric::integer()const{
    const int top=highest(magnitude_);if(top<0)return 0;
    for(int n=0;n<1074;++n)if(bit(magnitude_,n))throw DbError("MATERIALIZED_STATE","INTEGER aggregate contains a fractional term");
    if(top>1137)throw DbError("NUMERIC_OVERFLOW","SUM(INTEGER) exceeds int64");
    std::uint64_t value=0;for(int n=0;n<64;++n)if(bit(magnitude_,1074+n))value|=std::uint64_t(1)<<n;
    const auto minimum=std::uint64_t(1)<<63;
    if(negative_){if(value>minimum)throw DbError("NUMERIC_OVERFLOW","SUM(INTEGER) exceeds int64");if(value==minimum)return std::numeric_limits<std::int64_t>::min();return -std::int64_t(value);}
    if(value>=minimum){throw DbError("NUMERIC_OVERFLOW","SUM(INTEGER) exceeds int64");}
    return std::int64_t(value);
}
double ExactNumeric::real(std::uint64_t denominator)const{
    if(!denominator)throw DbError("MATERIALIZED_STATE","Zero numeric denominator");
    auto quotient=magnitude_;std::uint64_t remainder=0;
    if(denominator<=std::numeric_limits<std::uint32_t>::max()){
        for(std::size_t n=words;n-->0;){const auto current=(remainder<<32)|quotient[n];quotient[n]=std::uint32_t(current/denominator);remainder=current%denominator;}
    }else{
        quotient.fill(0);
        for(int n=int(words*32)-1;n>=0;--n){const bool carry=bool(remainder>>63);remainder=(remainder<<1)|(bit(magnitude_,n)?1u:0u);if(carry||remainder>=denominator){remainder-=denominator;quotient[std::size_t(n/32)]|=std::uint32_t(1)<<(n%32);}}
    }
    const bool above_half=remainder>denominator/2,tie=denominator%2==0&&remainder==denominator/2;
    int top=highest(quotient);std::uint64_t raw=0;
    if(top<52){for(int n=0;n<52;++n)if(bit(quotient,n))raw|=std::uint64_t(1)<<n;if(above_half||(tie&&(raw&1)))++raw;}
    else{
        const int shift=top-52;std::uint64_t significand=0;for(int n=0;n<53;++n)if(bit(quotient,shift+n))significand|=std::uint64_t(1)<<n;
        bool round_up=false;
        if(shift==0){round_up=above_half||(tie&&(significand&1));}
        else if(bit(quotient,shift-1)){bool tail=remainder!=0;for(int n=0;n<shift-1&&!tail;++n)tail=bit(quotient,n);round_up=tail||(significand&1);}
        if(round_up&&++significand==(std::uint64_t(1)<<53)){significand>>=1;++top;}
        const int exponent=top-51;if(exponent>2046)throw DbError("NUMERIC_OVERFLOW","REAL aggregate exceeds finite binary64");
        raw=(std::uint64_t(exponent)<<52)|(significand&((std::uint64_t(1)<<52)-1));
    }
    if(negative_)raw|=std::uint64_t(1)<<63;
    double result=0;std::memcpy(&result,&raw,sizeof(result));return result;
}
json ExactNumeric::state()const{
    static const char* digits="0123456789ABCDEF";std::string hex;hex.reserve(words*8);for(std::size_t n=words;n-->0;)for(int shift=28;shift>=0;shift-=4)hex+=digits[(magnitude_[n]>>shift)&15];return {{"format",1},{"negative",negative_},{"magnitude_hex",hex}};
}
ExactNumeric ExactNumeric::restore(const json& state){
    if(!state.is_object()||(!state.contains("format")||state["format"]!=1)||!state.contains("negative")||!state["negative"].is_boolean()||!state.contains("magnitude_hex")||!state["magnitude_hex"].is_string())throw DbError("MATERIALIZED_STATE","Invalid exact accumulator state");
    const auto& text=state["magnitude_hex"].get_ref<const std::string&>();if(text.size()!=words*8)throw DbError("MATERIALIZED_STATE","Invalid exact accumulator width");ExactNumeric result;result.negative_=state["negative"].get<bool>();for(std::size_t n=0;n<text.size();++n){const auto c=text[n];const int value=c>='0'&&c<='9'?c-'0':c>='A'&&c<='F'?c-'A'+10:-1;if(value<0)throw DbError("MATERIALIZED_STATE","Invalid exact accumulator hex");const auto word=words-1-n/8;result.magnitude_[word]=(result.magnitude_[word]<<4)|std::uint32_t(value);}if(highest(result.magnitude_)<0&&result.negative_)throw DbError("MATERIALIZED_STATE","Negative zero accumulator is not canonical");return result;
}
AggregateGroup::AggregateGroup(std::vector<AggregateSpec> specs):specs_(std::move(specs)),cells_(specs_.size()){
    if(specs_.size()>128)throw DbError("MATERIALIZED_RESOURCE","At most128 incremental aggregates per view");
    for(const auto& spec:specs_)if((spec.kind!="COUNT"&&spec.kind!="SUM"&&spec.kind!="AVG")||((spec.kind=="SUM"||spec.kind=="AVG")&&spec.type!="INTEGER"&&spec.type!="REAL"))throw DbError("UNSUPPORTED_SQL","Incremental aggregates support COUNT, SUM and AVG of numeric input");
}
void AggregateGroup::change(const json& arguments,int direction){
    if((direction!=1&&direction!=-1)||!arguments.is_array()||arguments.size()!=cells_.size())throw DbError("MATERIALIZED_STATE","Aggregate delta argument shape/direction mismatch");
    auto next=*this;if(direction<0&&next.rows_==0)throw DbError("MATERIALIZED_STATE","Aggregate group row count underflow");if(direction>0&&next.rows_==std::uint64_t(std::numeric_limits<std::int64_t>::max()))throw DbError("NUMERIC_OVERFLOW","Aggregate group row count exceeds INTEGER");next.rows_=direction>0?next.rows_+1:next.rows_-1;
    for(std::size_t n=0;n<cells_.size();++n){const auto& value=arguments[n];if(value.is_null())continue;auto& cell=next.cells_[n];if(direction<0&&cell.count==0)throw DbError("MATERIALIZED_STATE","Aggregate non-null count underflow");cell.count=direction>0?cell.count+1:cell.count-1;if(cell.count>next.rows_)throw DbError("MATERIALIZED_STATE","Aggregate non-null count exceeds group rows");if(specs_[n].kind!="COUNT"){if(specs_[n].type=="INTEGER"&&!value.is_number_integer())throw DbError("TYPE_ERROR","INTEGER aggregate delta requires INTEGER input");cell.sum.add(value,direction);}}
    for(const auto& cell:next.cells_)if(cell.count>next.rows_)throw DbError("MATERIALIZED_STATE","Aggregate non-null count exceeds remaining group rows");
    *this=std::move(next);
}
json AggregateGroup::values()const{
    json output=json::array();for(std::size_t n=0;n<cells_.size();++n){const auto& cell=cells_[n];const auto& spec=specs_[n];if(spec.kind=="COUNT")output.push_back(cell.count);else if(!cell.count)output.push_back(nullptr);else if(spec.kind=="SUM")output.push_back(spec.type=="INTEGER"?json(cell.sum.integer()):json(cell.sum.real()));else output.push_back(cell.sum.real(cell.count));}return output;
}
json AggregateGroup::state()const{
    json cells=json::array();for(std::size_t n=0;n<cells_.size();++n)cells.push_back({{"kind",specs_[n].kind},{"type",specs_[n].type},{"count",cells_[n].count},{"sum",cells_[n].sum.state()}});return {{"format",1},{"rows",rows_},{"cells",cells}};
}
AggregateGroup AggregateGroup::restore(const json& state,const std::vector<AggregateSpec>& specs){
    AggregateGroup result(specs);if(!state.is_object()||(!state.contains("format")||state["format"]!=1)||!state.contains("rows")||!state["rows"].is_number_unsigned()||!state.contains("cells")||!state["cells"].is_array()||state["cells"].size()!=specs.size())throw DbError("MATERIALIZED_STATE","Invalid incremental group state");result.rows_=state["rows"].get<std::uint64_t>();if(result.rows_>std::uint64_t(std::numeric_limits<std::int64_t>::max()))throw DbError("MATERIALIZED_STATE","Invalid group row count");for(std::size_t n=0;n<specs.size();++n){const auto& cell=state["cells"][n];if(cell.value("kind",std::string())!=specs[n].kind||cell.value("type",std::string())!=specs[n].type||!cell.contains("count")||!cell["count"].is_number_unsigned())throw DbError("MATERIALIZED_STATE","Aggregate state definition mismatch");result.cells_[n].count=cell["count"].get<std::uint64_t>();if(result.cells_[n].count>result.rows_)throw DbError("MATERIALIZED_STATE","Invalid non-null aggregate count");result.cells_[n].sum=ExactNumeric::restore(cell.at("sum"));}return result;
}
} // namespace edudb::mv
