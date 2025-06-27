#include "aarch64.h"
#include <utility>

namespace questdb::aarch64 {
    using namespace asmjit::aarch64;

    jit_value_t read_imm(Compiler &c, const instruction_t &instr) {
        auto type = static_cast<data_type_t>(instr.options);
        switch (type) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
            case data_type_t::i64: {
                return {imm(instr.ipayload.lo), type, data_kind_t::kConst};
            }
            case data_type_t::i128: {
                return {
                    c.newConst(ConstPoolScope::kLocal, &instr.ipayload, 16),
                    type,
                    data_kind_t::kMemory
                };
            }
            case data_type_t::f32:
            case data_type_t::f64: {
                return {imm(instr.dpayload), type, data_kind_t::kConst};
            }
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t
    read_vars_mem(Compiler &c, data_type_t type, int32_t idx, const Gp &vars_ptr) {
        auto shift = type_shift(type);
        auto type_size = 1 << shift;
        return {Mem(vars_ptr, 8 * idx, type_size), type, data_kind_t::kMemory};
    }

    jit_value_t read_mem(
            Compiler &c, data_type_t type, int32_t column_idx, const Gp &data_ptr,
            const Gp &varsize_aux_ptr, const Gp &input_index
    ) {
        // For now, we don't support variable size columns on ARM
        Gp column_address = c.newInt64("column_address");
        c.mov(column_address, ptr(data_ptr, 8 * column_idx, 8));

        auto shift = type_shift(type);
        auto type_size = 1 << shift;
        if (type_size <= 8) {
            return {Mem(column_address, input_index, shift, 0, type_size), type, data_kind_t::kMemory};
        } else {
            Gp offset = c.newInt64("row_offset");
            c.mov(offset, input_index);
            c.sal(offset, shift);
            return {Mem(column_address, offset, 0, 0, type_size), type, data_kind_t::kMemory};
        }
    }

    jit_value_t mem2reg(Compiler &c, const jit_value_t &v) {
        auto type = v.dtype();
        auto mem = v.op().as<Mem>();
        switch (type) {
            case data_type_t::i8: {
                Gp row_data = c.newGpd("i8_mem");
                c.ldrsb(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::i16: {
                Gp row_data = c.newGpd("i16_mem");
                c.ldrsh(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::i32: {
                Gp row_data = c.newGpd("i32_mem");
                c.ldr(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::i64: {
                Gp row_data = c.newGpq("i64_mem");
                c.ldr(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::i128: {
                Vec row_data = c.newVecQ("i128_mem");
                c.ldr(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::f32: {
                Vec row_data = c.newVecS("f32_mem");
                c.ldr(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            case data_type_t::f64: {
                Vec row_data = c.newVecD("f64_mem");
                c.ldr(row_data, mem);
                return {row_data, type, data_kind_t::kMemory};
            }
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t imm2reg(Compiler &c, data_type_t dst_type, const jit_value_t &v) {
        Imm k = v.op().as<Imm>();
        if (k.isInt()) {
            auto value = k.valueAs<int64_t>();
            switch (dst_type) {
                case data_type_t::f32: {
                    Vec reg = c.newVecS("f32_imm");
                    c.fmov(reg, static_cast<float>(value));
                    return {reg, data_type_t::f32, data_kind_t::kConst};
                }
                case data_type_t::f64: {
                    Vec reg = c.newVecD("f64_imm");
                    c.fmov(reg, static_cast<double>(value));
                    return {reg, data_type_t::f64, data_kind_t::kConst};
                }
                default: {
                    Gp reg = c.newGpq("i64_imm");
                    c.mov(reg, value);
                    return {reg, dst_type, data_kind_t::kConst};
                }
            }
        } else {
            auto value = k.valueAs<double>();
            if (dst_type == data_type_t::f64) {
                Vec reg = c.newVecD("f64_imm");
                c.fmov(reg, value);
                return {reg, data_type_t::f64, data_kind_t::kConst};
            } else {
                Vec reg = c.newVecS("f32_imm");
                c.fmov(reg, static_cast<float>(value));
                return {reg, data_type_t::f32, data_kind_t::kConst};
            }
        }
    }

    jit_value_t load_register(Compiler &c, data_type_t dst_type, const jit_value_t &v) {
        if (v.op().isImm()) {
            return imm2reg(c, dst_type, v);
        } else if (v.op().isMem()) {
            return mem2reg(c, v);
        } else {
            return v;
        }
    }

    jit_value_t load_register(Compiler &c, const jit_value_t &v) {
        return load_register(c, v.dtype(), v);
    }

    inline jit_value_t get_argument(Compiler &c, ZoneStack<jit_value_t> &values) {
        auto arg = values.pop();
        return load_register(c, arg);
    }

    jit_value_t neg(Compiler &c, const jit_value_t &lhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = lhs.dkind();
        switch (dt) {
            case data_type_t::i8:
            case data_type_t::i16:
            case data_type_t::i32:
            case data_type_t::i64:
                c.neg(lhs.gp(), lhs.gp());
                return {lhs.gp(), dt, dk};
            case data_type_t::f32:
            case data_type_t::f64:
                c.fneg(lhs.dp(), lhs.dp());
                return {lhs.dp(), dt, dk};
            default:
                __builtin_unreachable();
        }
    }

    jit_value_t bin_not(Compiler &c, const jit_value_t &lhs) {
        auto dt = lhs.dtype();
        auto dk = lhs.dkind();
        c.mvn(lhs.gp(), lhs.gp());
        return {lhs.gp(), dt, dk};
    }

    inline std::pair<jit_value_t, jit_value_t>
    get_arguments(Compiler &c, ZoneStack<jit_value_t> &values, bool null_check) {
        auto lhs = values.pop();
        auto rhs = values.pop();
        // We don't support type conversion for now
        return {load_register(c, lhs), load_register(c, rhs)};
    }

    jit_value_t bin_and(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        c.and_(lhs.gp(), lhs.gp(), rhs.gp());
        return {lhs.gp(), dt, dk};
    }

    jit_value_t bin_or(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        c.orr(lhs.gp(), lhs.gp(), rhs.gp());
        return {lhs.gp(), dt, dk};
    }

    jit_value_t cmp_eq(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        Gp result = c.newGpd("result");
        c.cmp(lhs.gp(), rhs.gp());
        c.cset(result, Cond::kEQ);
        return {result, data_type_t::i32, dk};
    }

    jit_value_t cmp_ne(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        Gp result = c.newGpd("result");
        c.cmp(lhs.gp(), rhs.gp());
        c.cset(result, Cond::kNE);
        return {result, data_type_t::i32, dk};
    }

    jit_value_t cmp_gt(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        Gp result = c.newGpd("result");
        c.cmp(lhs.gp(), rhs.gp());
        c.cset(result, Cond::kGT);
        return {result, data_type_t::i32, dk};
    }

    jit_value_t cmp_ge(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        Gp result = c.newGpd("result");
        c.cmp(lhs.gp(), rhs.gp());
        c.cset(result, Cond::kGE);
        return {result, data_type_t::i32, dk};
    }

    jit_value_t cmp_lt(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        Gp result = c.newGpd("result");
        c.cmp(lhs.gp(), rhs.gp());
        c.cset(result, Cond::kLT);
        return {result, data_type_t::i32, dk};
    }

    jit_value_t cmp_le(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        Gp result = c.newGpd("result");
        c.cmp(lhs.gp(), rhs.gp());
        c.cset(result, Cond::kLE);
        return {result, data_type_t::i32, dk};
    }

    jit_value_t add(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        c.add(lhs.gp(), lhs.gp(), rhs.gp());
        return {lhs.gp(), dt, dk};
    }

    jit_value_t sub(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        c.sub(lhs.gp(), lhs.gp(), rhs.gp());
        return {lhs.gp(), dt, dk};
    }

    jit_value_t mul(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        c.mul(lhs.gp(), lhs.gp(), rhs.gp());
        return {lhs.gp(), dt, dk};
    }

    jit_value_t div(Compiler &c, const jit_value_t &lhs, const jit_value_t &rhs, bool null_check) {
        auto dt = lhs.dtype();
        auto dk = dst_kind(lhs, rhs);
        c.sdiv(lhs.gp(), lhs.gp(), rhs.gp());
        return {lhs.gp(), dt, dk};
    }

    void emit_bin_op(Compiler &c, const instruction_t &instr, ZoneStack<jit_value_t> &values, bool null_check) {
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

    void emit_code(Compiler &c, const instruction_t *istream, size_t size, ZoneStack<jit_value_t> &values,
                   bool null_check,
                   const Gp &data_ptr,
                   const Gp &varsize_aux_ptr,
                   const Gp &vars_ptr,
                   const Gp &input_index) {
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

    void scalar_loop(Compiler &c, const instruction_t *istream, size_t size, bool null_check, int unroll_factor) {
        // Get the arguments from the function signature
        auto data_ptr = c.arg(0).as<Gp>();
        auto data_size = c.arg(1).as<Gp>();
        auto varsize_aux_ptr = c.arg(2).as<Gp>();
        auto vars_ptr = c.arg(3).as<Gp>();
        auto vars_size = c.arg(4).as<Gp>();
        auto rows_ptr = c.arg(5).as<Gp>();
        auto rows_size = c.arg(6).as<Gp>();
        auto rows_id_start_offset = c.arg(7).as<Gp>();

        Gp input_index = c.newGpq("input_index");
        c.mov(input_index, 0);

        Gp output_index = c.newGpq("output_index");
        c.mov(output_index, 0);

        Label l_loop = c.newLabel();
        Label l_exit = c.newLabel();

        c.cmp(input_index, rows_size);
        c.b_ge(l_exit);

        c.bind(l_loop);

        Zone zone(4096);
        ZoneAllocator allocator(&zone);
        ZoneStack<jit_value_t> values;
        values.init(&allocator);

        emit_code(c, istream, size, values, null_check, data_ptr, varsize_aux_ptr, vars_ptr, input_index);

        auto mask = values.pop();

        Gp adjusted_id = c.newGpq("adjusted_id");
        c.add(adjusted_id, input_index, rows_id_start_offset);
        c.str(adjusted_id, Mem(rows_ptr, output_index, 3));

        c.tst(mask.gp(), 1);
        c.cinc(output_index, output_index, Cond::kNE);

        c.add(input_index, input_index, 1);

        c.cmp(input_index, rows_size);
        c.b_lt(l_loop);

        c.bind(l_exit);
        c.ret(output_index);
    }
}