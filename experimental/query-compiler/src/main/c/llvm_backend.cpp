/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

#include <llvm/ExecutionEngine/Orc/LLJIT.h>
#include <llvm/ExecutionEngine/Orc/ObjectTransformLayer.h>
#include <llvm/Object/SymbolSize.h>
#include <llvm/ExecutionEngine/Orc/ExecutionUtils.h>
#include <llvm/IR/Verifier.h>
#include <llvm/IRReader/IRReader.h>
#include <llvm/Passes/PassBuilder.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Target/TargetMachine.h>
#include <array>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>

namespace {
using namespace llvm;
using namespace llvm::orc;
std::mutex mutex;
std::once_flag initialized;
std::atomic<int64_t> live{0};
uint64_t sequence = 0;

void check(Error error) { if (error) throw std::runtime_error(toString(std::move(error))); }
template<class T> T unwrap(Expected<T> result) {
    if (!result) throw std::runtime_error(toString(result.takeError()));
    return std::move(*result);
}
void message(char* buffer, uint64_t capacity, const char* text) noexcept {
    if (buffer && capacity) std::snprintf(buffer, static_cast<size_t>(capacity), "%s", text);
}
struct Engine {
    std::unique_ptr<TargetMachine> target;
    std::unique_ptr<LLJIT> jit;
    std::unordered_map<std::string, uint64_t> profileSizes;
    std::string profileDirectory;
    std::string profilePath(const std::string& symbol, const char* suffix) const {
        return profileDirectory + "/" + std::to_string(getpid()) + "-" + symbol + suffix;
    }
    explicit Engine(int level) {
        std::call_once(initialized, [] {
            if (InitializeNativeTarget() || InitializeNativeTargetAsmPrinter() || InitializeNativeTargetAsmParser())
                throw std::runtime_error("Could not initialize LLVM native target");
        });
        auto builder = unwrap(JITTargetMachineBuilder::detectHost());
        builder.setCodeGenOptLevel(static_cast<CodeGenOptLevel>(level));
        target = unwrap(builder.createTargetMachine());
        jit = unwrap(LLJITBuilder().setJITTargetMachineBuilder(std::move(builder)).create());
        if (const char* path = std::getenv("QUERY_CODEGEN_PROFILE_DIR")) {
            profileDirectory = path;
            jit->getObjTransformLayer().setTransform([this](std::unique_ptr<MemoryBuffer> object)
                    -> Expected<std::unique_ptr<MemoryBuffer>> {
                auto parsed = object::ObjectFile::createObjectFile(object->getMemBufferRef());
                if (!parsed) return parsed.takeError();
                for (const auto& [symbol, size] : object::computeSymbolSizes(**parsed)) {
                    auto name = symbol.getName();
                    if (!name) return name.takeError();
                    if (!name->starts_with("questdb_kernel_")) continue;
                    profileSizes[name->str()] = size;
                    std::error_code error;
                    raw_fd_ostream out(profilePath(name->str(), ".o"), error);
                    if (error) return errorCodeToError(error);
                    out.write(object->getBufferStart(), object->getBufferSize());
                }
                return object;
            });
        }
        if (jit->getDataLayout().getPointerSizeInBits() != 64) throw std::runtime_error("LLVM kernel ABI requires 64-bit pointers");
        jit->getMainJITDylib().addGenerator(unwrap(DynamicLibrarySearchGenerator::GetForCurrentProcess(jit->getDataLayout().getGlobalPrefix())));
    }
};
std::array<std::unique_ptr<Engine>, 4> engines;
struct KernelHandle {
    ResourceTrackerSP tracker;
    bool counted = false;
    ~KernelHandle() {
        if (tracker) consumeError(tracker->remove());
        if (counted) --live;
    }
};
void verify(Module& module) {
    std::string diagnostic; raw_string_ostream out(diagnostic);
    if (verifyModule(module, &out)) throw std::runtime_error("Invalid LLVM module: " + diagnostic);
}
void optimize(Module& module, TargetMachine* target, int level) {
    LoopAnalysisManager loops; FunctionAnalysisManager functions;
    CGSCCAnalysisManager calls; ModuleAnalysisManager modules;
    PassBuilder builder(target);
    builder.registerLoopAnalyses(loops); builder.registerFunctionAnalyses(functions);
    builder.registerCGSCCAnalyses(calls); builder.registerModuleAnalyses(modules);
    builder.crossRegisterProxies(loops, functions, calls, modules);
    const OptimizationLevel levels[] = {OptimizationLevel::O0, OptimizationLevel::O1, OptimizationLevel::O2, OptimizationLevel::O3};
    auto passes = level == 0 ? builder.buildO0DefaultPipeline(levels[level]) : builder.buildPerModuleDefaultPipeline(levels[level]);
    passes.run(module, modules);
}
}

#define EXPORTED extern "C" __attribute__((visibility("default")))
EXPORTED void* query_llvm_compile(const char* ir, uint64_t length, int32_t level, uint64_t* entry,
                                  const char* optimized_path, char* error, uint64_t capacity) noexcept {
    if (entry) *entry = 0;
    try {
        std::lock_guard<std::mutex> lock(mutex);
        if (!ir || !entry || level < 0 || level > 3) throw std::invalid_argument("Invalid LLVM compile arguments");
        auto context = std::make_unique<LLVMContext>(); SMDiagnostic diagnostic;
        // LLVM's lexer requires a trailing NUL even though MemoryBufferRef carries a length.
        auto buffer = MemoryBuffer::getMemBufferCopy(StringRef(ir, static_cast<size_t>(length)), "query.ll");
        auto module = parseIR(buffer->getMemBufferRef(), diagnostic, *context);
        if (!module) {
            std::string text; raw_string_ostream out(text); diagnostic.print("query LLVM", out);
            throw std::runtime_error(text);
        }
        verify(*module);
        Function* query = module->getFunction("query");
        if (!query || query->isDeclaration() || !query->hasExternalLinkage() || query->isVarArg()
                || query->arg_size() != 6 || !query->getReturnType()->isIntegerTy(64)
                || query->getCallingConv() != CallingConv::C)
            throw std::invalid_argument("Expected external i64 query(i64, i64, i64, i64, i64, i64)");
        for (const auto& arg : query->args()) if (!arg.getType()->isIntegerTy(64)) throw std::invalid_argument("Invalid query parameter type");
        if (!engines[level]) engines[level] = std::make_unique<Engine>(level);
        auto& engine = *engines[level];
        module->setDataLayout(engine.jit->getDataLayout()); module->setTargetTriple(engine.jit->getTargetTriple());
        std::string symbol = "questdb_kernel_" + std::to_string(sequence++); query->setName(symbol);
        optimize(*module, engine.target.get(), level); verify(*module);
        if (optimized_path) {
            std::error_code failure; raw_fd_ostream output(optimized_path, failure);
            if (failure) throw std::runtime_error("Cannot dump optimized LLVM IR: " + failure.message());
            module->print(output, nullptr); output.flush();
            if (output.has_error()) { auto error = output.error(); output.clear_error(); throw std::runtime_error("Cannot write optimized LLVM IR: " + error.message()); }
        }
        if (!engine.profileDirectory.empty()) {
            std::error_code failure;
            raw_fd_ostream output(engine.profilePath(symbol, ".ll"), failure);
            if (failure) throw std::runtime_error("Cannot write profiling IR: " + failure.message());
            module->print(output, nullptr);
        }
        auto handle = std::make_unique<KernelHandle>();
        handle->tracker = engine.jit->getMainJITDylib().createResourceTracker();
        check(engine.jit->addIRModule(handle->tracker, ThreadSafeModule(std::move(module), std::move(context))));
        auto address = unwrap(engine.jit->lookup(symbol));
        *entry = address.getValue(); ++live; handle->counted = true;
        if (!engine.profileDirectory.empty()) {
            auto size = engine.profileSizes.at(symbol);
            auto name = engine.profilePath("native", ".map");
            FILE* map = std::fopen(name.c_str(), "a");
            if (!map) throw std::runtime_error("Cannot write profiling symbol map");
            std::fprintf(map, "%llx %llx %s\n", static_cast<unsigned long long>(*entry),
                         static_cast<unsigned long long>(size), symbol.c_str());
            std::fclose(map);
        }
        return handle.release();
    } catch (const std::exception& failure) { message(error, capacity, failure.what()); }
      catch (...) { message(error, capacity, "Unknown LLVM compilation failure"); }
    return nullptr;
}
EXPORTED int32_t query_llvm_release(void* pointer, char* error, uint64_t capacity) noexcept {
    try {
        std::lock_guard<std::mutex> lock(mutex);
        std::unique_ptr<KernelHandle> handle(static_cast<KernelHandle*>(pointer));
        if (!handle) return 0;
        auto failure = handle->tracker->remove(); handle->tracker.reset(); check(std::move(failure));
        return 0;
    } catch (const std::exception& failure) { message(error, capacity, failure.what()); }
      catch (...) { message(error, capacity, "Unknown LLVM release failure"); }
    return 1;
}
EXPORTED int64_t query_llvm_live_functions() noexcept { return live.load(); }
