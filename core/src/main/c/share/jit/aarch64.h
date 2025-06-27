#pragma once

#include <cstddef>
#include <asmjit/asmjit.h>
#include "common.h"

namespace questdb::aarch64 {
    void scalar_loop(
        asmjit::aarch64::Compiler& c,
        const instruction_t* istream,
        size_t size,
        bool null_check,
        int unroll_factor = 1);
}