#include <asmjit/core.h>
#include <asmjit/x86.h>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <vector>

// The same CodeHolder / Compiler / JitRuntime ownership model used by QuestDB's
// filter JIT. This opt-in bridge has its own runtime, leaving the filter ABI intact.
using namespace asmjit;
namespace {
struct Header { uint32_t magic,version,registers,labels,count,reserved; };
struct Instruction { int32_t op,dst,a,b; int64_t immediate; };
static_assert(sizeof(Header)==24 && sizeof(Instruction)==24);
enum Opcode { Const=1,Move,Add,Sub,Mul,And,Xor,Ushr,Eq,Ne,Lt,Ge,Load,Store,Bind,Jump,BranchFalse,Allocate,Free,Poll,Return,CompareBranch,BranchTrue,Ctz,Prefetch,FAdd,FSub,FMul,FDiv,ToDouble,SDiv,AtomicLoad,CompareExchange,AtomicStore,AtomicAdd,ByteEqMask,Reallocate };
JitRuntime runtime;
std::atomic<int64_t> live_functions{0};
struct Errors : ErrorHandler {
    Error error=Error::kOk;
    void handle_error(Error e,const char*,BaseEmitter*) override { if(error==Error::kOk)error=e; }
};
void require(bool condition,const char* message) { if(!condition)throw std::invalid_argument(message); }
void validate(const Header& h,const Instruction* code) {
    require(h.magic==0x51444952 && h.version==8 && h.reserved==0,"incompatible query IR ABI");
    require(h.registers>=3 && h.registers<=1000000 && h.labels<=1000000 && h.count>0 && h.count<=1000000,"invalid IR dimensions");
    std::vector<bool> bound(h.labels),referenced(h.labels);
    auto reg=[&](int id){require(id>=0 && uint32_t(id)<h.registers,"invalid register");};
    auto label=[&](int id){require(id>=0 && uint32_t(id)<h.labels,"invalid label");};
    for(uint32_t n=0;n<h.count;n++) {
        const auto& i=code[n];
        switch(i.op) {
            case ByteEqMask:
                reg(i.dst);reg(i.a);
                require(i.b==8||i.b==16||i.b==32||i.b==64,"invalid byte mask lanes");
                require(i.immediate>=0&&i.immediate<=255,"invalid byte mask constant");break;
            case Const:reg(i.dst);break;
            case AtomicLoad:case Move:case Load:case Allocate:case Ctz:case ToDouble:reg(i.dst);reg(i.a);break;
            case AtomicAdd:case SDiv:case FAdd:case FSub:case FMul:case FDiv:case Add:case Sub:case Mul:case And:case Xor:case Ushr:case Eq:case Ne:case Lt:case Ge:reg(i.dst);reg(i.a);reg(i.b);break;
            case CompareExchange:reg(i.dst);reg(i.a);reg(i.b);require(i.immediate>=0&&uint64_t(i.immediate)<h.registers,"invalid CAS value");break;
            case Reallocate:reg(i.dst);reg(i.a);reg(i.b);require(i.immediate>=0&&uint64_t(i.immediate)<h.registers,"invalid reallocation size register");break;
            case AtomicStore:case Store:case Free:reg(i.a);reg(i.b);break;
            case Bind:label(i.dst);require(!bound[i.dst],"duplicate label");bound[i.dst]=true;break;
            case CompareBranch:reg(i.b);require(i.immediate>=Eq&&i.immediate<=Ge,"invalid branch comparison");[[fallthrough]];
            case BranchTrue:case BranchFalse:reg(i.a);[[fallthrough]];
            case Jump:label(i.dst);referenced[i.dst]=true;break;
            case Poll:break;
            case Prefetch:reg(i.a);require(i.immediate==0||i.immediate==1,"invalid prefetch hint");break;
            case Return:reg(i.a);break;
            default:throw std::invalid_argument("unknown IR instruction");
        }
        if(i.op==Load||i.op==Store)require(i.immediate==1||i.immediate==2||i.immediate==4||i.immediate==8,"invalid memory width");
    }
    for(uint32_t n=0;n<h.labels;n++)require(!referenced[n]||bound[n],"unbound label");
    require(code[h.count-1].op==Return,"IR must end in return");
}
void* compile(const Header& h,const Instruction* instructions) {
    CodeHolder code;code.init(runtime.environment());
    Errors errors;code.set_error_handler(&errors);
    x86::Compiler c(&code);
    auto* fn=c.add_func(FuncSignature::build<int64_t,int64_t,int64_t,int64_t,int64_t,int64_t,int64_t,int64_t>());
    require(fn!=nullptr,"could not create generated function");
    std::vector<x86::Gp> r; r.reserve(h.registers);
    for(uint32_t n=0;n<h.registers;n++)r.push_back(c.new_gp64());
    for(uint32_t n=0;n<3;n++)fn->set_arg(n,r[n]);
    auto alloc=c.new_gp64(),release=c.new_gp64(),poll=c.new_gp64(),realloc=c.new_gp64();
    fn->set_arg(3,alloc);fn->set_arg(4,release);fn->set_arg(5,poll);fn->set_arg(6,realloc);
    std::vector<Label> labels;labels.reserve(h.labels);
    for(uint32_t n=0;n<h.labels;n++)labels.push_back(c.new_label());
    // Constants with one definition can be rematerialized at their use. This also
    // enables immediate instructions without a separate move/register per literal.
    std::vector<uint32_t> definitions(h.registers);
    std::vector<bool> constant(h.registers);
    std::vector<int64_t> value(h.registers);
    definitions[0]=definitions[1]=definitions[2]=1;
    for(uint32_t n=0;n<h.count;n++) {
        const auto& i=instructions[n];
        if((i.op>=Const&&i.op<=Load)||i.op==Allocate||i.op==Reallocate||i.op==Ctz||i.op==AtomicAdd||i.op==ByteEqMask||(i.op>=FAdd&&i.op<=CompareExchange)) {
            definitions[i.dst]++;if(i.op==Const){constant[i.dst]=true;value[i.dst]=i.immediate;}
        }
    }
    for(uint32_t n=0;n<h.registers;n++)constant[n]=constant[n]&&definitions[n]==1;
    auto source=[&](int id){if(constant[id])c.mov(r[id],value[id]);return r[id];};
    auto move=[&](const x86::Gp& dst,int id){if(constant[id])c.mov(dst,value[id]);else c.mov(dst,r[id]);};
    auto immediate=[&](int id){return constant[id]&&value[id]==int64_t(int32_t(value[id]));};
    auto compare=[&](int a,int b){auto left=source(a);if(immediate(b))c.cmp(left,Imm(value[b]));else c.cmp(left,source(b));};
    for(uint32_t n=0;n<h.count;n++) {
        const auto& i=instructions[n];
        switch(i.op) {
            case Const:if(!constant[i.dst])c.mov(r[i.dst],i.immediate);break;
            case Move:move(r[i.dst],i.a);break;
            case SDiv: {
                auto high=c.new_gp64();
                move(r[i.dst],i.a);auto divisor=source(i.b);
                c.cqo(high,r[i.dst]);c.idiv(high,r[i.dst],divisor);break;
            }
            case ToDouble: {
                auto value=c.new_xmm();
                c.cvtsi2sd(value,source(i.a));c.movq(r[i.dst],value);break;
            }
            case FAdd:case FSub:case FMul:case FDiv: {
                auto left=c.new_xmm(),right=c.new_xmm();
                c.movq(left,source(i.a));c.movq(right,source(i.b));
                switch(i.op) {
                    case FAdd:c.addsd(left,right);break;case FSub:c.subsd(left,right);break;
                    case FMul:c.mulsd(left,right);break;case FDiv:c.divsd(left,right);break;
                }
                c.movq(r[i.dst],left);break;
            }
            case Add:case Sub:case Mul:case And:case Xor:case Ushr:
                move(r[i.dst],i.a);
                if(i.op==Ushr) {
                    if(constant[i.b])c.shr(r[i.dst],Imm(uint64_t(value[i.b])&63));
                    else c.shr(r[i.dst],source(i.b).r8());
                }else if(immediate(i.b)) {
                    Imm right(value[i.b]);
                    switch(i.op) {
                        case Add:c.add(r[i.dst],right);break;case Sub:c.sub(r[i.dst],right);break;
                        case Mul:c.imul(r[i.dst],r[i.dst],right);break;case And:c.and_(r[i.dst],right);break;case Xor:c.xor_(r[i.dst],right);break;
                    }
                }else {
                    auto right=source(i.b);
                    switch(i.op) {
                        case Add:c.add(r[i.dst],right);break;case Sub:c.sub(r[i.dst],right);break;
                        case Mul:c.imul(r[i.dst],right);break;case And:c.and_(r[i.dst],right);break;case Xor:c.xor_(r[i.dst],right);break;
                    }
                }break;
            case Eq:case Ne:case Lt:case Ge:
                compare(i.a,i.b);
                switch(i.op) {
                    case Eq:c.sete(r[i.dst].r8());break;
                    case Ne:c.setne(r[i.dst].r8());break;
                    case Lt:c.setl(r[i.dst].r8());break;
                    case Ge:c.setge(r[i.dst].r8());break;
                }
                c.movzx(r[i.dst],r[i.dst].r8());break;
            case ByteEqMask: {
                // SSE2 is baseline on x86-64. Eight lanes must not read sixteen bytes.
                auto base=source(i.a), repeated=c.new_gp64();
                auto needle=c.new_xmm();
                c.mov(repeated,uint64_t(i.immediate)*0x0101010101010101ULL);
                c.movq(needle,repeated);c.punpcklqdq(needle,needle);
                for(int offset=0;offset<i.b;offset+=16) {
                    auto bytes=c.new_xmm();
                    if(i.b==8)c.movq(bytes,x86::qword_ptr(base));
                    else c.movdqu(bytes,x86::ptr(base,offset));
                    c.pcmpeqb(bytes,needle);
                    auto bits=offset==0?r[i.dst]:c.new_gp64();
                    c.pmovmskb(bits.r32(),bytes);
                    if(i.b==8)c.and_(bits,255);
                    if(offset!=0){c.shl(bits,offset);c.or_(r[i.dst],bits);}
                }
                break;
            }
            case Ctz: {
                auto input=c.new_gp64();move(input,i.a);
                auto zero=c.new_gp64();c.mov(zero,64);
                c.bsf(r[i.dst],input);c.cmovz(r[i.dst],zero);break;
            }
            case AtomicLoad:c.mov(r[i.dst],x86::qword_ptr(source(i.a)));break;
            case AtomicStore:c.mov(x86::qword_ptr(source(i.a)),source(i.b));break;
            case AtomicAdd: {
                auto address=source(i.a),value=c.new_gp64();move(value,i.b);
                c.lock().xadd(x86::qword_ptr(address),value);c.mov(r[i.dst],value);break;
            }
            case CompareExchange: {
                auto address=source(i.a),value=source(int(i.immediate)),observed=c.new_gp64();
                move(observed,i.b);
                c.lock().cmpxchg(x86::qword_ptr(address),value,observed);
                c.mov(r[i.dst],observed);break;
            }
            case Load: {
                auto base=source(i.a);
                switch(i.immediate) {
                    case 1:c.movsx(r[i.dst],x86::byte_ptr(base,i.b));break;
                    case 2:c.movsx(r[i.dst],x86::word_ptr(base,i.b));break;
                    case 4:c.movsxd(r[i.dst],x86::dword_ptr(base,i.b));break;
                    case 8:c.mov(r[i.dst],x86::qword_ptr(base,i.b));break;
                }break;
            }
            case Store: {
                auto base=source(i.a),stored=source(i.b);
                switch(i.immediate) {
                    case 1:c.mov(x86::byte_ptr(base,i.dst),stored.r8());break;
                    case 2:c.mov(x86::word_ptr(base,i.dst),stored.r16());break;
                    case 4:c.mov(x86::dword_ptr(base,i.dst),stored.r32());break;
                    case 8:c.mov(x86::qword_ptr(base,i.dst),stored);break;
                }break;
            }
            case Prefetch: {
                auto address=x86::ptr(source(i.a),i.dst);
                if(i.immediate)c.prefetchw(address);else c.prefetcht0(address);
                break;
            }
            case Bind:c.bind(labels[i.dst]);break;
            case Jump:c.jmp(labels[i.dst]);break;
            case BranchFalse:case BranchTrue: {
                auto condition=source(i.a);c.test(condition,condition);
                if(i.op==BranchFalse)c.jz(labels[i.dst]);else c.jnz(labels[i.dst]);break;
            }
            case CompareBranch:
                compare(i.a,i.b);
                switch(i.immediate) {
                    case Eq:c.je(labels[i.dst]);break;case Ne:c.jne(labels[i.dst]);break;
                    case Lt:c.jl(labels[i.dst]);break;case Ge:c.jge(labels[i.dst]);break;
                }break;
            case Allocate: {
                auto bytes=source(i.a);
                InvokeNode* call=nullptr;
                require(c.invoke(Out(call),alloc,FuncSignature::build<int64_t,int64_t>())==Error::kOk && call,"could not emit allocation call");
                call->set_arg(0,bytes);call->set_ret(0,r[i.dst]);break;
            }
            case Reallocate: {
                auto address=source(i.a),oldBytes=source(i.b),newBytes=source(int(i.immediate));
                InvokeNode* call=nullptr;
                require(c.invoke(Out(call),realloc,FuncSignature::build<int64_t,int64_t,int64_t,int64_t>())==Error::kOk && call,"could not emit reallocation call");
                call->set_arg(0,address);call->set_arg(1,oldBytes);call->set_arg(2,newBytes);call->set_ret(0,r[i.dst]);break;
            }
            case Free: {
                auto address=source(i.a),bytes=source(i.b);
                InvokeNode* call=nullptr;
                require(c.invoke(Out(call),release,FuncSignature::build<void,int64_t,int64_t>())==Error::kOk && call,"could not emit release call");
                call->set_arg(0,address);call->set_arg(1,bytes);break;
            }
            case Poll: {
                InvokeNode* call=nullptr;auto result=c.new_gp64();auto ok=c.new_label();
                require(c.invoke(Out(call),poll,FuncSignature::build<int64_t>())==Error::kOk && call,"could not emit cancellation call");call->set_ret(0,result);
                c.test(result,result);c.jz(ok);c.mov(result,4);c.ret(result);c.bind(ok);break;
            }
            case Return:c.ret(source(i.a));break;
        }
    }
    c.end_func();
    Error error=errors.error;
    if(error==Error::kOk)error=c.finalize();
    void* result=nullptr;
    if(error==Error::kOk)error=runtime.add(&result,&code);
    if(error!=Error::kOk)throw std::runtime_error(DebugUtils::error_as_string(error));
    live_functions.fetch_add(1);return result;
}
}
#define EXPORT extern "C" __attribute__((visibility("default")))
EXPORT void* query_compile(const void* bytes,uint64_t size,char* error,uint64_t error_size) noexcept {
    try {
        require(bytes && size>=sizeof(Header),"truncated query IR");
        Header header;std::memcpy(&header,bytes,sizeof(header));
        require(size==sizeof(Header)+uint64_t(header.count)*sizeof(Instruction),"incorrect query IR length");
        require(header.count<=1000000 && header.registers<=1000000 && header.labels<=1000000,"invalid IR dimensions");
        // FFM supplies an 8-byte aligned buffer. Copy keeps this ABI tolerant of unaligned input.
        std::vector<Instruction> instructions(header.count);
        std::memcpy(instructions.data(),static_cast<const char*>(bytes)+sizeof(Header),instructions.size()*sizeof(Instruction));
        validate(header,instructions.data());
        return compile(header,instructions.data());
    }catch(const std::exception& e){if(error&&error_size)std::snprintf(error,error_size,"%s",e.what());return nullptr;}
    catch(...){if(error&&error_size)std::snprintf(error,error_size,"native query compilation failed");return nullptr;}
}
EXPORT void query_release(void* fn) noexcept {
    if(fn&&runtime.release(fn)==Error::kOk)live_functions.fetch_sub(1);
}
EXPORT int64_t query_live_functions() noexcept {return live_functions.load();}
