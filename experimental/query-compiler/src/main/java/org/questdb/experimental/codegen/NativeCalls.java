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

package org.questdb.experimental.codegen;

import java.lang.foreign.*;
import java.lang.invoke.*;

/** Shared generated-kernel ABI: three arguments plus allocator, release and cancellation callbacks. */
final class NativeCalls {
    private NativeCalls(){ }
    static Kernel bind(MemorySegment address,Arena arena)throws ReflectiveOperationException {
        MethodHandle function = Linker.nativeLinker().downcallHandle(address, FunctionDescriptor.of(
                ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
        var lookup = MethodHandles.lookup();
        long allocate = Linker.nativeLinker().upcallStub(lookup.findStatic(KernelMemory.class, "malloc",
                MethodType.methodType(long.class, long.class)), FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG), arena).address();
        long free = Linker.nativeLinker().upcallStub(lookup.findStatic(KernelMemory.class, "free",
                MethodType.methodType(void.class, long.class, long.class)),
                FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG), arena).address();
        long poll = Linker.nativeLinker().upcallStub(lookup.findStatic(KernelMemory.class, "poll",
                MethodType.methodType(long.class)), FunctionDescriptor.of(ValueLayout.JAVA_LONG), arena).address();
        MethodHandle bound = MethodHandles.insertArguments(function, 3, allocate, free, poll);
        return (columns, rows, state) -> invoke(bound, columns, rows, state);
    }

    private static long invoke(MethodHandle function, long columns, long rows, long state) {
        try {
            return (long) function.invokeExact(columns, rows, state);
        } catch (Error e) {
            throw e;
        } catch (Throwable e) {
            throw new IllegalStateException("Native kernel invocation failed", e);
        }
    }

}
