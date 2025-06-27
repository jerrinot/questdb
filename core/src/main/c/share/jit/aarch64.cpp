#include "aarch64.h"
#include "common.h"
#include "impl/aarch64.h"
#include <utility>
#include <asmjit/core/archcommons.h>

using namespace asmjit;

namespace questdb::aarch64 {

    jit_value_t read_imm(asmjit::a64::Compiler &c, const instruction_t &instr) {
        auto type = static_cast<data_type_t>(instr.options);
        switch (type) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
            case data_type_t::i64: {
                return {asmjit::Imm(instr.ipayload.lo), type, data_kind_t::kConst};
            }
            case data_type_t::i128: {
                return {
                    c.newConst(asmjit::ConstPoolScope::kLocal, &instr.ipayload, 16),
                    type,
                    data_kind_t::kMemory
                };
            }
            case data_type_t::f32:
            case data_type_t::f64: {
                return {asmjit::Imm(instr.dpayload), type, data_kind_t::kConst};
            }
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t
    read_vars_mem(asmjit::a64::Compiler &c, data_type_t type, int32_t idx, const asmjit::a64::Gp &vars_ptr) {
        auto shift = type_shift(type);
        auto type_size = 1 << shift;
        return {asmjit::arm::Mem(vars_ptr, 8 * idx), type, data_kind_t::kMemory};
    }

    jit_value_t read_mem(
            asmjit::a64::Compiler &c, data_type_t type, int32_t column_idx, const asmjit::a64::Gp &data_ptr,
            const asmjit::a64::Gp &varsize_aux_ptr, const asmjit::a64::Gp &input_index
    ) {
        // For now, we don't support variable size columns on ARM
        asmjit::a64::Gp column_address = c.newInt64("column_address");
        c.ldr(column_address, asmjit::a64::ptr(data_ptr, 8 * column_idx));

        auto shift = type_shift(type);
        auto type_size = 1 << shift;
        if (type_size <= 8) {
            return {asmjit::arm::Mem(column_address, input_index), type, data_kind_t::kMemory};
        } else {
            asmjit::a64::Gp offset = c.newInt64("row_offset");
            c.mov(offset, input_index);
            c.lsl(offset, offset, shift);
            return {asmjit::arm::Mem(column_address, offset), type, data_kind_t::kMemory};
        }
    }

    jit_value_t mem2reg(asmjit::a64::Compiler &c, const jit_value_t &v) {
        auto type = v.dtype();
        auto mem = v.op().as<asmjit::arm::Mem>();
        switch (type) {
            case data_type_t::i8: {
                asmjit::a64::Gp row_data = c.newInt32("i8_mem");
                c.ldrsb(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::i16: {
                asmjit::a64::Gp row_data = c.newInt32("i16_mem");
                c.ldrsh(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::i32: {
                asmjit::a64::Gp row_data = c.newInt32("i32_mem");
                c.ldr(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::i64: {
                asmjit::a64::Gp row_data = c.newInt64("i64_mem");
                c.ldr(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::i128: {
                asmjit::a64::Vec row_data = c.newVecQ("i128_mem");
                c.ldr(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::f32: {
                asmjit::a64::Vec row_data = c.newVecS("f32_mem");
                c.ldr(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::f64: {
                asmjit::a64::Vec row_data = c.newVecD("f64_mem");
                c.ldr(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t imm2reg(asmjit::a64::Compiler &c, data_type_t dst_type, const jit_value_t &v) {
        asmjit::Imm k = v.op().as<asmjit::Imm>();
        if (k.isInt()) {
            auto value = k.valueAs<int64_t>();
            switch (dst_type) {
                case data_type_t::f32: {
                    asmjit::a64::Vec reg = c.newVecS("f32_imm");
                    c.fmov(reg, static_cast<float>(value));
                    return {reg, data_type_t::f32, data_kind_t::kConst};
                }
                case data_type_t::f64: {
                    asmjit::a64::Vec reg = c.newVecD("f64_imm");
                    c.fmov(reg, static_cast<double>(value));
                    return {reg, data_type_t::f64, data_kind_t::kConst};
                }
                default: {
                    asmjit::a64::Gp reg = c.newInt64("i64_imm");
                    c.mov(reg, value);
                    return {reg, dst_type, data_kind_t::kConst};
                }
            }
        } else {
            auto value = k.valueAs<double>();
            if (dst_type == data_type_t::f64) {
                asmjit::a64::Vec reg = c.newVecD("f64_imm");
                c.fmov(reg, value);
                return {reg, data_type_t::f64, data_kind_t::kConst};
            } else {
                asmjit::a64::Vec reg = c.newVecS("f32_imm");
                c.fmov(reg, static_cast<float>(value));
                return {reg, data_type_t::f32, data_kind_t::kConst};
            }
        }
    }

    jit_value_t convert_type(asmjit::a64::Compiler &c, const jit_value_t &v, data_type_t dst_type, bool null_check) {
        auto src_type = v.dtype();
        auto src_kind = v.dkind();
        
        // No conversion needed
        if (src_type == dst_type) {
            return v;
        }
        
        // Handle type conversions
        switch (src_type) {
            case data_type_t::i8:
                switch (dst_type) {
                    case data_type_t::i16:
                    case data_type_t::i32: {
                        auto result = questdb::aarch64::int8_to_int32(c, v.gp(), null_check);
                        return {result, dst_type, src_kind};
                    }
                    case data_type_t::i64: {
                        auto i32_result = questdb::aarch64::int8_to_int32(c, v.gp(), null_check);
                        auto result = questdb::aarch64::int32_to_int64(c, i32_result, null_check);
                        return {result, dst_type, src_kind};
                    }
                    case data_type_t::f32: {
                        auto i32_result = questdb::aarch64::int8_to_int32(c, v.gp(), null_check);
                        auto result = questdb::aarch64::int32_to_float(c, i32_result, null_check);
                        return {result, dst_type, src_kind};
                    }
                    case data_type_t::f64: {
                        auto i32_result = questdb::aarch64::int8_to_int32(c, v.gp(), null_check);
                        auto result = questdb::aarch64::int32_to_double(c, i32_result, null_check);
                        return {result, dst_type, src_kind};
                    }
                    default:
                        break;
                }
                break;
            case data_type_t::i16:
                switch (dst_type) {
                    case data_type_t::i32: {
                        auto result = questdb::aarch64::int16_to_int32(c, v.gp(), null_check);
                        return {result, dst_type, src_kind};
                    }
                    case data_type_t::i64: {
                        auto i32_result = questdb::aarch64::int16_to_int32(c, v.gp(), null_check);
                        auto result = questdb::aarch64::int32_to_int64(c, i32_result, null_check);
                        return {result, dst_type, src_kind};
                    }
                    case data_type_t::f32: {
                        auto i32_result = questdb::aarch64::int16_to_int32(c, v.gp(), null_check);
                        auto result = questdb::aarch64::int32_to_float(c, i32_result, null_check);
                        return {result, dst_type, src_kind};
                    }
                    case data_type_t::f64: {
                        auto i32_result = questdb::aarch64::int16_to_int32(c, v.gp(), null_check);
                        auto result = questdb::aarch64::int32_to_double(c, i32_result, null_check);
                        return {result, dst_type, src_kind};
                    }
                    default:
                        break;
                }
                break;
            case data_type_t::i32:
                switch (dst_type) {
                    case data_type_t::i64: {
                        auto result = questdb::aarch64::int32_to_int64(c, v.gp(), null_check);
                        return {result, dst_type, src_kind};
                    }
                    case data_type_t::f32: {
                        auto result = questdb::aarch64::int32_to_float(c, v.gp(), null_check);
                        return {result, dst_type, src_kind};
                    }
                    case data_type_t::f64: {
                        auto result = questdb::aarch64::int32_to_double(c, v.gp(), null_check);
                        return {result, dst_type, src_kind};
                    }
                    default:
                        break;
                }
                break;
            case data_type_t::i64:
                switch (dst_type) {
                    case data_type_t::f32: {
                        auto result = questdb::aarch64::int64_to_float(c, v.gp(), null_check);
                        return {result, dst_type, src_kind};
                    }
                    case data_type_t::f64: {
                        auto result = questdb::aarch64::int64_to_double(c, v.gp(), null_check);
                        return {result, dst_type, src_kind};
                    }
                    default:
                        break;
                }
                break;
            case data_type_t::f32:
                switch (dst_type) {
                    case data_type_t::f64: {
                        auto result = questdb::aarch64::float_to_double(c, v.vec());
                        return {result, dst_type, src_kind};
                    }
                    default:
                        break;
                }
                break;
            default:
                break;
        }
        
        // No conversion available - return original value
        return v;
    }

    jit_value_t load_register(asmjit::a64::Compiler &c, data_type_t dst_type, const jit_value_t &v) {
        jit_value_t loaded;
        if (v.op().isImm()) {
            loaded = imm2reg(c, dst_type, v);
        } else if (v.op().isMem()) {
            loaded = mem2reg(c, v);
        } else {
            loaded = v;
        }
        
        // Apply type conversion if needed
        return convert_type(c, loaded, dst_type, true);
    }

    jit_value_t load_register(asmjit::a64::Compiler &c, const jit_value_t &v) {
        return load_register(c, v.dtype(), v);
    }

    inline jit_value_t get_argument(asmjit::a64::Compiler &c, asmjit::ZoneStack<jit_value_t> &values) {
        auto arg = values.pop();
        return load_register(c, arg);
    }

    jit_value_t neg(asmjit::a64::Compiler &c, const jit_value_t &lhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = lhs.dkind();
        switch (dt) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32: {
                auto result = questdb::aarch64::int32_neg(c, lhs.gp(), null_check);
                return {result, dt, dk};
            }
            case data_type_t::i64: {
                auto result = questdb::aarch64::int64_neg(c, lhs.gp(), null_check);
                return {result, dt, dk};
            }
            case data_type_t::f32: {
                auto result = questdb::aarch64::float_neg(c, lhs.vec());
                return {result, dt, dk};
            }
            case data_type_t::f64: {
                auto result = questdb::aarch64::double_neg(c, lhs.vec());
                return {result, dt, dk};
            }
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t bin_not(asmjit::a64::Compiler &c, const jit_value_t &lhs) {
        auto dt = lhs.dtype();
        auto dk = lhs.dkind();
        auto result = questdb::aarch64::int32_not(c, lhs.gp());
        return {result, dt, dk};
    }

    data_type_t promote_types(data_type_t lhs_type, data_type_t rhs_type) {
        // Type promotion hierarchy: i8 < i16 < i32 < i64 < f32 < f64
        // Return the "higher" type
        
        auto type_rank = [](data_type_t type) -> int {
            switch (type) {
                case data_type_t::i8: return 1;
                case data_type_t::i16: return 2;
                case data_type_t::i32: return 3;
                case data_type_t::i64: return 4;
                case data_type_t::f32: return 5;
                case data_type_t::f64: return 6;
                case data_type_t::i128: return 7; // Special case for 128-bit
                default: return 0;
            }
        };
        
        int lhs_rank = type_rank(lhs_type);
        int rhs_rank = type_rank(rhs_type);
        
        return (lhs_rank > rhs_rank) ? lhs_type : rhs_type;
    }

    inline std::pair<jit_value_t, jit_value_t>
    get_arguments(asmjit::a64::Compiler &c, asmjit::ZoneStack<jit_value_t> &values, bool null_check) {
        auto lhs = values.pop();
        auto rhs = values.pop();
        
        // Load registers first
        auto lhs_loaded = load_register(c, lhs);
        auto rhs_loaded = load_register(c, rhs);
        
        // Determine target type for promotion
        auto target_type = promote_types(lhs_loaded.dtype(), rhs_loaded.dtype());
        
        // Convert both operands to the target type
        auto lhs_converted = convert_type(c, lhs_loaded, target_type, null_check);
        auto rhs_converted = convert_type(c, rhs_loaded, target_type, null_check);
        
        return {lhs_converted, rhs_converted};
    }

    jit_value_t bin_and(asmjit::a64::Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        auto result = questdb::aarch64::int32_and(c, lhs.gp(), rhs.gp());
        return {result, dt, dk};
    }

    jit_value_t bin_or(asmjit::a64::Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        auto result = questdb::aarch64::int32_or(c, lhs.gp(), rhs.gp());
        return {result, dt, dk};
    }

    jit_value_t cmp_eq(asmjit::a64::Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        if (dt == data_type_t::i64) {
            auto result = questdb::aarch64::int64_eq(c, lhs.gp(), rhs.gp());
            return {result, data_type_t::i32, dk};
        } else {
            auto result = questdb::aarch64::int32_eq(c, lhs.gp(), rhs.gp());
            return {result, data_type_t::i32, dk};
        }
    }

    jit_value_t cmp_ne(asmjit::a64::Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        if (dt == data_type_t::i64) {
            auto result = questdb::aarch64::int64_ne(c, lhs.gp(), rhs.gp());
            return {result, data_type_t::i32, dk};
        } else {
            auto result = questdb::aarch64::int32_ne(c, lhs.gp(), rhs.gp());
            return {result, data_type_t::i32, dk};
        }
    }

    jit_value_t cmp_gt(asmjit::a64::Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        if (dt == data_type_t::i64) {
            auto result = questdb::aarch64::int64_gt(c, lhs.gp(), rhs.gp(), null_check);
            return {result, data_type_t::i32, dk};
        } else {
            auto result = questdb::aarch64::int32_gt(c, lhs.gp(), rhs.gp(), null_check);
            return {result, data_type_t::i32, dk};
        }
    }

    jit_value_t cmp_ge(asmjit::a64::Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        if (dt == data_type_t::i64) {
            auto result = questdb::aarch64::int64_ge(c, lhs.gp(), rhs.gp(), null_check);
            return {result, data_type_t::i32, dk};
        } else {
            auto result = questdb::aarch64::int32_ge(c, lhs.gp(), rhs.gp(), null_check);
            return {result, data_type_t::i32, dk};
        }
    }

    jit_value_t cmp_lt(asmjit::a64::Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        if (dt == data_type_t::i64) {
            auto result = questdb::aarch64::int64_lt(c, lhs.gp(), rhs.gp(), null_check);
            return {result, data_type_t::i32, dk};
        } else {
            auto result = questdb::aarch64::int32_lt(c, lhs.gp(), rhs.gp(), null_check);
            return {result, data_type_t::i32, dk};
        }
    }

    jit_value_t cmp_le(asmjit::a64::Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        if (dt == data_type_t::i64) {
            auto result = questdb::aarch64::int64_le(c, lhs.gp(), rhs.gp(), null_check);
            return {result, data_type_t::i32, dk};
        } else {
            auto result = questdb::aarch64::int32_le(c, lhs.gp(), rhs.gp(), null_check);
            return {result, data_type_t::i32, dk};
        }
    }

    jit_value_t add(asmjit::a64::Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        switch (dt) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32: {
                auto result = questdb::aarch64::int32_add(c, lhs.gp(), rhs.gp(), null_check);
                return {result, dt, dk};
            }
            case data_type_t::i64: {
                auto result = questdb::aarch64::int64_add(c, lhs.gp(), rhs.gp(), null_check);
                return {result, dt, dk};
            }
            case data_type_t::f32: {
                auto result = questdb::aarch64::float_add(c, lhs.vec(), rhs.vec());
                return {result, dt, dk};
            }
            case data_type_t::f64: {
                auto result = questdb::aarch64::double_add(c, lhs.vec(), rhs.vec());
                return {result, dt, dk};
            }
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t sub(asmjit::a64::Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        switch (dt) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32: {
                auto result = questdb::aarch64::int32_sub(c, lhs.gp(), rhs.gp(), null_check);
                return {result, dt, dk};
            }
            case data_type_t::i64: {
                auto result = questdb::aarch64::int64_sub(c, lhs.gp(), rhs.gp(), null_check);
                return {result, dt, dk};
            }
            case data_type_t::f32: {
                auto result = questdb::aarch64::float_sub(c, lhs.vec(), rhs.vec());
                return {result, dt, dk};
            }
            case data_type_t::f64: {
                auto result = questdb::aarch64::double_sub(c, lhs.vec(), rhs.vec());
                return {result, dt, dk};
            }
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t mul(asmjit::a64::Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        switch (dt) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32: {
                auto result = questdb::aarch64::int32_mul(c, lhs.gp(), rhs.gp(), null_check);
                return {result, dt, dk};
            }
            case data_type_t::i64: {
                auto result = questdb::aarch64::int64_mul(c, lhs.gp(), rhs.gp(), null_check);
                return {result, dt, dk};
            }
            case data_type_t::f32: {
                auto result = questdb::aarch64::float_mul(c, lhs.vec(), rhs.vec());
                return {result, dt, dk};
            }
            case data_type_t::f64: {
                auto result = questdb::aarch64::double_mul(c, lhs.vec(), rhs.vec());
                return {result, dt, dk};
            }
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t div(asmjit::a64::Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        switch (dt) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32: {
                auto result = questdb::aarch64::int32_div(c, lhs.gp(), rhs.gp(), null_check);
                return {result, dt, dk};
            }
            case data_type_t::i64: {
                auto result = questdb::aarch64::int64_div(c, lhs.gp(), rhs.gp(), null_check);
                return {result, dt, dk};
            }
            case data_type_t::f32: {
                auto result = questdb::aarch64::float_div(c, lhs.vec(), rhs.vec());
                return {result, dt, dk};
            }
            case data_type_t::f64: {
                auto result = questdb::aarch64::double_div(c, lhs.vec(), rhs.vec());
                return {result, dt, dk};
            }
            default:
                __builtin_unreachable();
        }
    }

    void emit_bin_op(asmjit::a64::Compiler &c, const instruction_t &instr, asmjit::ZoneStack<jit_value_t> &values, bool null_check) {
        auto args = get_arguments(c, values, null_check);
        auto lhs = args.first;
        auto rhs = args.second;
        switch (instr.opcode) {
            case opcodes::And:
                values.append(bin_and(c, lhs, rhs));
                break;
            case opcodes::Or:
                values.append(bin_or(c, lhs, rhs));
                break;
            case opcodes::Eq:
                values.append(cmp_eq(c, lhs, rhs));
                break;
            case opcodes::Ne:
                values.append(cmp_ne(c, lhs, rhs));
                break;
            case opcodes::Gt:
                values.append(cmp_gt(c, lhs, rhs, null_check));
                break;
            case opcodes::Ge:
                values.append(cmp_ge(c, lhs, rhs, null_check));
                break;
            case opcodes::Lt:
                values.append(cmp_lt(c, lhs, rhs, null_check));
                break;
            case opcodes::Le:
                values.append(cmp_le(c, lhs, rhs, null_check));
                break;
            case opcodes::Add:
                values.append(add(c, lhs, rhs, null_check));
                break;
            case opcodes::Sub:
                values.append(sub(c, lhs, rhs, null_check));
                break;
            case opcodes::Mul:
                values.append(mul(c, lhs, rhs, null_check));
                break;
            case opcodes::Div:
                values.append(div(c, lhs, rhs, null_check));
                break;
            default:
                __builtin_unreachable();
        }
    }

    void emit_code(asmjit::a64::Compiler &c, const instruction_t *istream, size_t size, asmjit::ZoneStack<jit_value_t> &values,
                   bool null_check,
                   const asmjit::a64::Gp &data_ptr,
                   const asmjit::a64::Gp &varsize_aux_ptr,
                   const asmjit::a64::Gp &vars_ptr,
                   const asmjit::a64::Gp &input_index) {
        for (size_t i = 0; i < size; ++i) {
            auto &instr = istream[i];
            switch (instr.opcode) {
                case opcodes::Ret:
                    return;
                case opcodes::Imm:
                    values.append(read_imm(c, instr));
                    break;
                case opcodes::Var: {
                    auto type = static_cast<data_type_t>(instr.options);
                    auto idx  = static_cast<int32_t>(instr.ipayload.lo);
                    values.append(read_vars_mem(c, type, idx, vars_ptr));
                }
                    break;
                case opcodes::Mem: {
                    auto type = static_cast<data_type_t>(instr.options);
                    auto idx  = static_cast<int32_t>(instr.ipayload.lo);
                    values.append(read_mem(c, type, idx, data_ptr, varsize_aux_ptr, input_index));
                }
                    break;
                case opcodes::Neg:
                    values.append(neg(c, get_argument(c, values), null_check));
                    break;
                case opcodes::Not:
                    values.append(bin_not(c, get_argument(c, values)));
                    break;
                default:
                    emit_bin_op(c, instr, values, null_check);
                    break;
            }
        }
    }

    void scalar_loop(asmjit::a64::Compiler &c, const instruction_t *istream, size_t size, bool null_check,
                     const asmjit::a64::Gp& data_ptr, const asmjit::a64::Gp& data_size, const asmjit::a64::Gp& varsize_aux_ptr,
                     const asmjit::a64::Gp& vars_ptr, const asmjit::a64::Gp& vars_size, const asmjit::a64::Gp& rows_ptr,
                     const asmjit::a64::Gp& rows_size, const asmjit::a64::Gp& rows_id_start_offset, int unroll_factor) {

        asmjit::a64::Gp input_index = c.newInt64("input_index");
        c.mov(input_index, 0);

        asmjit::a64::Gp output_index = c.newInt64("output_index");
        c.mov(output_index, 0);

        asmjit::Label l_loop = c.newLabel();
        asmjit::Label l_exit = c.newLabel();

        c.cmp(input_index, rows_size);
        c.b_ge(l_exit);

        c.bind(l_loop);

        asmjit::Zone zone(4096);
        asmjit::ZoneAllocator allocator(&zone);
        asmjit::ZoneStack<jit_value_t> values;
        values.init(&allocator);

        emit_code(c, istream, size, values, null_check, data_ptr, varsize_aux_ptr, vars_ptr, input_index);

        auto mask = values.pop();

        asmjit::a64::Gp adjusted_id = c.newInt64("adjusted_id");
        c.add(adjusted_id, input_index, rows_id_start_offset);
        c.str(adjusted_id, asmjit::arm::Mem(rows_ptr, output_index));

        c.tst(mask.gp(), 1);
        c.cinc(output_index, output_index, arm::CondCode::kNE);

        c.add(input_index, input_index, 1);

        c.cmp(input_index, rows_size);
        c.b_lt(l_loop);

        c.bind(l_exit);
        c.ret(output_index);
    }
}