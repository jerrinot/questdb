/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __\__ \ |_| |_| | |_) |
 *    \___\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

#ifndef QUESTDB_JIT_IMPL_AARCH64_H
#define QUESTDB_JIT_IMPL_AARCH64_H

#include "consts.h"

namespace questdb::aarch64 {
    using namespace asmjit;
    using namespace asmjit::a64;

    inline Gp int32_not(Compiler &c, const Gp &b) {
        c.mvn(b.r32(), b.r32());
        return b;
    }

    inline Gp int32_and(Compiler &c, const Gp &b1, const Gp &b2) {
        c.and_(b1.r32(), b1.r32(), b2.r32());
        return b1;
    }

    inline Gp int32_or(Compiler &c, const Gp &b1, const Gp &b2) {
        c.comment("int32_or_start");
        c.orr(b1.r32(), b1.r32(), b2.r32());
        c.comment("int32_or_stop");
        return b1;
    }

    inline void check_int32_null(Compiler &c, const Gp &dst, const Gp &lhs, const Gp &rhs) {
        c.comment("check_int32_null");
        Gp null_val = c.newInt32();
        c.mov(null_val, INT_NULL);
        c.cmp(lhs.r32(), null_val.r32());
        c.csel(dst.r32(), lhs.r32(), dst.r32(), arm::CondCode::kEQ);
        c.cmp(rhs.r32(), null_val.r32());
        c.csel(dst.r32(), rhs.r32(), dst.r32(), arm::CondCode::kEQ);
    }

    inline Gp int32_neg(Compiler &c, const Gp &rhs, bool check_null) {
        c.comment("int32_neg");
        
        Gp r = c.newInt32();
        c.mov(r.r32(), rhs.r32());
        c.neg(r.r32(), r.r32());
        if (check_null) {
            Gp null_val = c.newInt32();
            c.mov(null_val, INT_NULL);
            c.cmp(rhs.r32(), null_val.r32());
            c.csel(r.r32(), null_val.r32(), r.r32(), arm::CondCode::kEQ);
        }
        return r;
    }

    inline Gp int64_neg(Compiler &c, const Gp &rhs, bool check_null) {
        c.comment("int64_neg");
        
        Gp r = c.newInt64();
        c.mov(r.r64(), rhs.r64());
        c.neg(r.r64(), r.r64());
        if (check_null) {
            Gp null_val = c.newInt64();
            c.mov(null_val, LONG_NULL);
            c.cmp(rhs.r64(), null_val.r64());
            c.csel(r.r64(), rhs.r64(), r.r64(), arm::CondCode::kEQ);
        }
        return r;
    }

    inline Gp int32_add(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        c.comment("int32_add");
        
        Gp r = c.newInt32();
        c.add(r.r32(), lhs.r32(), rhs.r32());
        if (check_null) check_int32_null(c, r, lhs, rhs);
        return r;
    }

    inline Gp int32_sub(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        c.comment("int32_sub");
        
        Gp r = c.newInt32();
        c.sub(r.r32(), lhs.r32(), rhs.r32());
        if (check_null) check_int32_null(c, r, lhs, rhs);
        return r;
    }

    inline Gp int32_mul(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        c.comment("int32_mul");
        
        Gp r = c.newInt32();
        c.mul(r.r32(), lhs.r32(), rhs.r32());
        if (check_null) check_int32_null(c, r, lhs, rhs);
        return r;
    }

    inline Gp int32_div(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        c.comment("int32_div");
        
        Label l_null = c.newLabel();
        Label l_exit = c.newLabel();
        
        Gp r = c.newInt32();
        
        if (!check_null) {
            c.cbz(rhs.r32(), l_null);
            c.sdiv(r.r32(), lhs.r32(), rhs.r32());
            c.b(l_exit);
            c.bind(l_null);
            c.mov(r, INT_NULL);
            c.bind(l_exit);
            return r;
        }
        
        Gp null_val = c.newInt32();
        c.mov(null_val, INT_NULL);
        c.mov(r.r32(), null_val.r32());
        
        // Check for division by zero
        c.cbz(rhs.r32(), l_null);
        
        // Check for null inputs
        c.cmp(lhs.r32(), null_val.r32());
        c.b_eq(l_null);
        
        c.sdiv(r.r32(), lhs.r32(), rhs.r32());
        c.bind(l_null);
        return r;
    }

    inline void check_int64_null(Compiler &c, const Gp &dst, const Gp &lhs, const Gp &rhs) {
        c.comment("check_int64_null");
        Gp null_val = c.newInt64();
        c.mov(null_val, LONG_NULL);
        c.cmp(lhs.r64(), null_val.r64());
        c.csel(dst.r64(), lhs.r64(), dst.r64(), arm::CondCode::kEQ);
        c.cmp(rhs.r64(), null_val.r64());
        c.csel(dst.r64(), rhs.r64(), dst.r64(), arm::CondCode::kEQ);
    }

    inline Gp int64_add(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        c.comment("int64_add");
        
        Gp r = c.newInt64();
        c.add(r.r64(), lhs.r64(), rhs.r64());
        if (check_null) check_int64_null(c, r, lhs, rhs);
        return r;
    }

    inline Gp int64_sub(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        c.comment("int64_sub");
        
        Gp r = c.newInt64();
        c.sub(r.r64(), lhs.r64(), rhs.r64());
        if (check_null) check_int64_null(c, r, lhs, rhs);
        return r;
    }

    inline Gp int64_mul(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        c.comment("int64_mul");
        
        Gp r = c.newInt64();
        c.mul(r.r64(), lhs.r64(), rhs.r64());
        if (check_null) check_int64_null(c, r, lhs, rhs);
        return r;
    }

    inline Gp int64_div(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        c.comment("int64_div");
        
        Label l_null = c.newLabel();
        Label l_exit = c.newLabel();
        
        Gp r = c.newInt64();
        
        if (!check_null) {
            c.cbz(rhs.r64(), l_null);
            c.sdiv(r.r64(), lhs.r64(), rhs.r64());
            c.b(l_exit);
            c.bind(l_null);
            c.mov(r, LONG_NULL);
            c.bind(l_exit);
            return r;
        }
        
        Gp null_val = c.newInt64();
        c.mov(null_val, LONG_NULL);
        c.mov(r.r64(), null_val.r64());
        
        // Check for division by zero
        c.cbz(rhs.r64(), l_null);
        
        // Check for null inputs
        c.cmp(lhs.r64(), null_val.r64());
        c.b_eq(l_null);
        
        c.sdiv(r.r64(), lhs.r64(), rhs.r64());
        c.bind(l_null);
        return r;
    }

    inline Vec float_neg(Compiler &c, const Vec &rhs) {
        c.fneg(rhs, rhs);
        return rhs;
    }

    inline Vec double_neg(Compiler &c, const Vec &rhs) {
        c.fneg(rhs, rhs);
        return rhs;
    }

    inline Vec float_add(Compiler &c, const Vec &lhs, const Vec &rhs) {
        c.fadd(lhs, lhs, rhs);
        return lhs;
    }

    inline Vec float_sub(Compiler &c, const Vec &lhs, const Vec &rhs) {
        c.fsub(lhs, lhs, rhs);
        return lhs;
    }

    inline Vec float_mul(Compiler &c, const Vec &lhs, const Vec &rhs) {
        c.fmul(lhs, lhs, rhs);
        return lhs;
    }

    inline Vec float_div(Compiler &c, const Vec &lhs, const Vec &rhs) {
        c.fdiv(lhs, lhs, rhs);
        return lhs;
    }

    inline Vec double_add(Compiler &c, const Vec &lhs, const Vec &rhs) {
        c.fadd(lhs, lhs, rhs);
        return lhs;
    }

    inline Vec double_sub(Compiler &c, const Vec &lhs, const Vec &rhs) {
        c.fsub(lhs, lhs, rhs);
        return lhs;
    }

    inline Vec double_mul(Compiler &c, const Vec &lhs, const Vec &rhs) {
        c.fmul(lhs, lhs, rhs);
        return lhs;
    }

    inline Vec double_div(Compiler &c, const Vec &lhs, const Vec &rhs) {
        c.fdiv(lhs, lhs, rhs);
        return lhs;
    }

    inline Gp int32_eq(Compiler &c, const Gp &lhs, const Gp &rhs) {
        Gp r = c.newInt32();
        c.cmp(lhs.r32(), rhs.r32());
        c.cset(r.r32(), arm::CondCode::kEQ);
        return r;
    }

    inline Gp int32_ne(Compiler &c, const Gp &lhs, const Gp &rhs) {
        Gp r = c.newInt32();
        c.cmp(lhs.r32(), rhs.r32());
        c.cset(r.r32(), arm::CondCode::kNE);
        return r;
    }

    inline Gp int32_lt_gt(Compiler &c, const Gp &lhs, const Gp &rhs, bool gt, bool check_null) {
        if (!check_null) {
            Gp r = c.newInt32();
            c.cmp(lhs.r32(), rhs.r32());
            if (gt) {
                c.cset(r.r32(), arm::CondCode::kGT);
            } else {
                c.cset(r.r32(), arm::CondCode::kLT);
            }
            return r;
        } else {
            Gp result = c.newInt32();
            Gp null_val = c.newInt32();
            Gp lhs_valid = c.newInt32();
            Gp rhs_valid = c.newInt32();
            
            c.mov(null_val, INT_NULL);
            
            // Check if operands are not null
            c.cmp(lhs.r32(), null_val.r32());
            c.cset(lhs_valid.r32(), arm::CondCode::kNE);
            c.cmp(rhs.r32(), null_val.r32());
            c.cset(rhs_valid.r32(), arm::CondCode::kNE);
            
            // Both must be valid for comparison
            c.and_(result.r32(), lhs_valid.r32(), rhs_valid.r32());
            
            // Perform comparison if both are valid
            c.cmp(lhs.r32(), rhs.r32());
            if (gt) {
                c.cset(lhs_valid.r32(), arm::CondCode::kGT);
            } else {
                c.cset(lhs_valid.r32(), arm::CondCode::kLT);
            }
            c.and_(result.r32(), result.r32(), lhs_valid.r32());
            
            return result;
        }
    }

    inline Gp int32_le_ge(Compiler &c, const Gp &lhs, const Gp &rhs, bool ge, bool check_null) {
        if (!check_null) {
            Gp r = c.newInt32();
            c.cmp(lhs.r32(), rhs.r32());
            if (ge) {
                c.cset(r.r32(), arm::CondCode::kGE);
            } else {
                c.cset(r.r32(), arm::CondCode::kLE);
            }
            return r;
        } else {
            Gp result = c.newInt32();
            Gp null_val = c.newInt32();
            Gp lhs_null = c.newInt32();
            Gp rhs_valid = c.newInt32();
            
            c.mov(null_val, INT_NULL);
            
            // Check null status
            c.cmp(lhs.r32(), null_val.r32());
            c.cset(lhs_null.r32(), arm::CondCode::kEQ);
            c.cmp(rhs.r32(), null_val.r32());
            c.cset(rhs_valid.r32(), arm::CondCode::kNE);
            
            // XOR for special null handling logic
            c.eor(result.r32(), rhs_valid.r32(), lhs_null.r32());
            
            // Perform comparison
            c.cmp(lhs.r32(), rhs.r32());
            if (ge) {
                c.cset(lhs_null.r32(), arm::CondCode::kGE);
            } else {
                c.cset(lhs_null.r32(), arm::CondCode::kLE);
            }
            c.and_(result.r32(), result.r32(), lhs_null.r32());
            
            return result;
        }
    }

    inline Gp int32_lt(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int32_lt_gt(c, lhs, rhs, false, check_null);
    }

    inline Gp int32_le(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int32_le_ge(c, lhs, rhs, false, check_null);
    }

    inline Gp int32_gt(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int32_lt_gt(c, lhs, rhs, true, check_null);
    }

    inline Gp int32_ge(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int32_le_ge(c, lhs, rhs, true, check_null);
    }

    inline Gp int64_eq(Compiler &c, const Gp &lhs, const Gp &rhs) {
        Gp r = c.newInt64();
        c.cmp(lhs.r64(), rhs.r64());
        c.cset(r.r32(), arm::CondCode::kEQ);
        return r;
    }

    inline Gp int64_ne(Compiler &c, const Gp &lhs, const Gp &rhs) {
        Gp r = c.newInt64();
        c.cmp(lhs.r64(), rhs.r64());
        c.cset(r.r32(), arm::CondCode::kNE);
        return r;
    }

    inline Gp int64_lt_gt(Compiler &c, const Gp &lhs, const Gp &rhs, bool gt, bool check_null) {
        if (!check_null) {
            Gp r = c.newInt64();
            c.cmp(lhs.r64(), rhs.r64());
            if (gt) {
                c.cset(r.r32(), arm::CondCode::kGT);
            } else {
                c.cset(r.r32(), arm::CondCode::kLT);
            }
            return r;
        } else {
            Gp result = c.newInt64();
            Gp null_val = c.newInt64();
            Gp lhs_valid = c.newInt64();
            Gp rhs_valid = c.newInt64();
            
            c.mov(null_val, LONG_NULL);
            
            // Check if operands are not null
            c.cmp(lhs.r64(), null_val.r64());
            c.cset(lhs_valid.r32(), arm::CondCode::kNE);
            c.cmp(rhs.r64(), null_val.r64());
            c.cset(rhs_valid.r32(), arm::CondCode::kNE);
            
            // Both must be valid for comparison
            c.and_(result.r32(), lhs_valid.r32(), rhs_valid.r32());
            
            // Perform comparison if both are valid
            c.cmp(lhs.r64(), rhs.r64());
            if (gt) {
                c.cset(lhs_valid.r32(), arm::CondCode::kGT);
            } else {
                c.cset(lhs_valid.r32(), arm::CondCode::kLT);
            }
            c.and_(result.r32(), result.r32(), lhs_valid.r32());
            
            return result;
        }
    }

    inline Gp int64_le_ge(Compiler &c, const Gp &lhs, const Gp &rhs, bool ge, bool check_null) {
        if (!check_null) {
            Gp r = c.newInt64();
            c.cmp(lhs.r64(), rhs.r64());
            if (ge) {
                c.cset(r.r32(), arm::CondCode::kGE);
            } else {
                c.cset(r.r32(), arm::CondCode::kLE);
            }
            return r;
        } else {
            Gp result = c.newInt64();
            Gp null_val = c.newInt64();
            Gp lhs_null = c.newInt64();
            Gp rhs_valid = c.newInt64();
            
            c.mov(null_val, LONG_NULL);
            
            // Check null status
            c.cmp(lhs.r64(), null_val.r64());
            c.cset(lhs_null.r32(), arm::CondCode::kEQ);
            c.cmp(rhs.r64(), null_val.r64());
            c.cset(rhs_valid.r32(), arm::CondCode::kNE);
            
            // XOR for special null handling logic
            c.eor(result.r32(), rhs_valid.r32(), lhs_null.r32());
            
            // Perform comparison
            c.cmp(lhs.r64(), rhs.r64());
            if (ge) {
                c.cset(lhs_null.r32(), arm::CondCode::kGE);
            } else {
                c.cset(lhs_null.r32(), arm::CondCode::kLE);
            }
            c.and_(result.r32(), result.r32(), lhs_null.r32());
            
            return result;
        }
    }

    inline Gp int64_lt(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int64_lt_gt(c, lhs, rhs, false, check_null);
    }

    inline Gp int64_le(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int64_le_ge(c, lhs, rhs, false, check_null);
    }

    inline Gp int64_gt(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int64_lt_gt(c, lhs, rhs, true, check_null);
    }

    inline Gp int64_ge(Compiler &c, const Gp &lhs, const Gp &rhs, bool check_null) {
        return int64_le_ge(c, lhs, rhs, true, check_null);
    }

    inline Gp float_lt(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.newInt32();
        c.fcmp(lhs, rhs);
        c.cset(r, arm::CondCode::kLT);
        return r;
    }

    inline Gp float_le(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.newInt32();
        c.fcmp(lhs, rhs);
        c.cset(r, arm::CondCode::kLE);
        return r;
    }

    inline Gp float_gt(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.newInt32();
        c.fcmp(lhs, rhs);
        c.cset(r, arm::CondCode::kGT);
        return r;
    }

    inline Gp float_ge(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.newInt32();
        c.fcmp(lhs, rhs);
        c.cset(r, arm::CondCode::kGE);
        return r;
    }

    inline Gp double_lt(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.newInt32();
        c.fcmp(lhs, rhs);
        c.cset(r, arm::CondCode::kLT);
        return r;
    }

    inline Gp double_le(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.newInt32();
        c.fcmp(lhs, rhs);
        c.cset(r, arm::CondCode::kLE);
        return r;
    }

    inline Gp double_gt(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.newInt32();
        c.fcmp(lhs, rhs);
        c.cset(r, arm::CondCode::kGT);
        return r;
    }

    inline Gp double_ge(Compiler &c, const Vec &lhs, const Vec &rhs) {
        Gp r = c.newInt32();
        c.fcmp(lhs, rhs);
        c.cset(r, arm::CondCode::kGE);
        return r;
    }

}
#endif //QUESTDB_JIT_IMPL_AARCH64_H