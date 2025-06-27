#pragma once

#include <cstddef>
#include <asmjit/asmjit.h>
#include <asmjit/a64.h>
#include "common.h"

namespace questdb::aarch64 {
    void scalar_loop(
        asmjit::a64::Compiler& c,
        const instruction_t* istream,
        size_t size,
        bool null_check,
        const asmjit::a64::Gp& data_ptr,
        const asmjit::a64::Gp& data_size,
        const asmjit::a64::Gp& varsize_aux_ptr,
        const asmjit::a64::Gp& vars_ptr,
        const asmjit::a64::Gp& vars_size,
        const asmjit::a64::Gp& rows_ptr,
        const asmjit::a64::Gp& rows_size,
        const asmjit::a64::Gp& rows_id_start_offset,
        int unroll_factor = 1);
}