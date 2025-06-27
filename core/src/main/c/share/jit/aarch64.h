#pragma once

#include <cstddef>

// Forward declarations to keep the header light
namespace asmjit {
    namespace aarch64 {
        class Compiler;
    }
}
struct instruction_t;

namespace questdb::aarch64 {
    void scalar_loop(
        asmjit::aarch64::Compiler& c,
        const instruction_t* istream,
        size_t size,
        bool null_check,
        int unroll_factor = 1);
}
