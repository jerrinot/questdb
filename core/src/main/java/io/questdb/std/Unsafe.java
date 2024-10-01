/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
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

package io.questdb.std;

// @formatter:off
import io.questdb.cairo.CairoException;
import org.jetbrains.annotations.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import static io.questdb.std.MemoryTag.NATIVE_DEFAULT;

public final class Unsafe {
    public static final long BYTE_OFFSET;
    public static final long BYTE_SCALE;
    public static final long INT_OFFSET;
    public static final long INT_SCALE;
    //#if jdk.version!=8
    public static final Module JAVA_BASE_MODULE = System.class.getModule();
    //#endif
    public static final long LONG_OFFSET;
    public static final long LONG_SCALE;
    private static final LongAdder[] COUNTERS = new LongAdder[MemoryTag.SIZE];
    private static final AtomicLong FREE_COUNT = new AtomicLong(0);
    private static final AtomicLong MALLOC_COUNT = new AtomicLong(0);
    private static final AtomicLong MEM_USED = new AtomicLong(0);
    //#if jdk.version!=8
    private static final long OVERRIDE;
    //#endif
    private static final AtomicLong REALLOC_COUNT = new AtomicLong(0);
    private static final AtomicLong RSS_MEM_USED = new AtomicLong(0);
    private static final sun.misc.Unsafe UNSAFE;
    private static final AnonymousClassDefiner anonymousClassDefiner;
    //#if jdk.version!=8
    private static final Method implAddExports;
    //#endif
    private static long RSS_MEM_LIMIT = 0;

    private Unsafe() {
    }

    //#if jdk.version!=8
    public static void addExports(Module from, Module to, String packageName) {
        try {
            implAddExports.invoke(from, packageName, to);
        } catch (ReflectiveOperationException e) {
            e.printStackTrace();
        }
    }
    //#endif

    public static long arrayGetVolatile(long[] array, int index) {
        assert index > -1 && index < array.length;
        return Unsafe.getUnsafe().getLongVolatile(array, LONG_OFFSET + ((long) index << LONG_SCALE));
    }

    public static int arrayGetVolatile(int[] array, int index) {
        assert index > -1 && index < array.length;
        return Unsafe.getUnsafe().getIntVolatile(array, INT_OFFSET + ((long) index << INT_SCALE));
    }

    /**
     * This call has Atomic*#lazySet / memory_order_release semantics.
     *
     * @param array array to put into
     * @param index index
     * @param value value to put
     */
    public static void arrayPutOrdered(long[] array, int index, long value) {
        assert index > -1 && index < array.length;
        Unsafe.getUnsafe().putOrderedLong(array, LONG_OFFSET + ((long) index << LONG_SCALE), value);
    }

    /**
     * This call has Atomic*#lazySet / memory_order_release semantics.
     *
     * @param array array to put into
     * @param index index
     * @param value value to put
     */
    public static void arrayPutOrdered(int[] array, int index, int value) {
        assert index > -1 && index < array.length;
        Unsafe.getUnsafe().putOrderedInt(array, INT_OFFSET + ((long) index << INT_SCALE), value);
    }

    public static int byteArrayGetInt(byte[] array, int index) {
        assert index > -1 && index < array.length - 3;
        return Unsafe.getUnsafe().getInt(array, BYTE_OFFSET + index);
    }

    public static long byteArrayGetLong(byte[] array, int index) {
        assert index > -1 && index < array.length - 7;
        return Unsafe.getUnsafe().getLong(array, BYTE_OFFSET + index);
    }

    public static long calloc(long size, int memoryTag) {
        long ptr = malloc(size, memoryTag);
        Vect.memset(ptr, size, 0);
        return ptr;
    }

    public static boolean cas(Object o, long offset, long expected, long value) {
        return UNSAFE.compareAndSwapLong(o, offset, expected, value);
    }

    public static boolean cas(Object o, long offset, int expected, int value) {
        return UNSAFE.compareAndSwapInt(o, offset, expected, value);
    }

    public static boolean cas(long[] array, int index, long expected, long value) {
        assert index > -1 && index < array.length;
        return Unsafe.cas(array, Unsafe.LONG_OFFSET + (((long) index) << Unsafe.LONG_SCALE), expected, value);
    }

    /**
     * Defines a class but does not make it known to the class loader or system dictionary.
     * <p>
     * Equivalent to {@code Unsafe#defineAnonymousClass} and {@code Lookup#defineHiddenClass}, except that
     * it does not support constant pool patches.
     *
     * @param hostClass context for linkage, access control, protection domain, and class loader
     * @param data      bytes of a class file
     * @return Java Class for the given bytecode
     */
    @Nullable
    public static Class<?> defineAnonymousClass(Class<?> hostClass, byte[] data) {
        return anonymousClassDefiner.define(hostClass, data);
    }

    public static long free(long ptr, long usefulSize, int memoryTag) {
        if (ptr != 0) {
            long prologSize = prologSize(usefulSize);
            long epilogSize = epilogSize(usefulSize);

            long rawPtr = ptr - prologSize;
            checkMemoryTag(rawPtr, usefulSize);
            Unsafe.getUnsafe().freeMemory(rawPtr);

            long rawSize = prologSize + usefulSize + epilogSize;
            FREE_COUNT.incrementAndGet();
            recordMemAlloc(-rawSize, memoryTag);
        }
        return 0;
    }

    public static boolean getBool(long address) {
        return UNSAFE.getByte(address) == 1;
    }

    public static long getFieldOffset(Class<?> clazz, String name) {
        try {
            return UNSAFE.objectFieldOffset(clazz.getDeclaredField(name));
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static long getFreeCount() {
        return FREE_COUNT.get();
    }

    public static long getMallocCount() {
        return MALLOC_COUNT.get();
    }

    public static long getMemUsed() {
        return MEM_USED.get();
    }

    public static long getMemUsedByTag(int memoryTag) {
        assert memoryTag >= 0 && memoryTag < MemoryTag.SIZE;
        return COUNTERS[memoryTag].sum();
    }

    public static long getReallocCount() {
        return REALLOC_COUNT.get();
    }

    public static long getRssMemUsed() {
        return RSS_MEM_USED.get();
    }

    public static sun.misc.Unsafe getUnsafe() {
        return UNSAFE;
    }

    public static void incrFreeCount() {
        FREE_COUNT.incrementAndGet();
    }

    public static void incrMallocCount() {
        MALLOC_COUNT.incrementAndGet();
    }

    public static void incrReallocCount() {
        REALLOC_COUNT.incrementAndGet();
    }

    //#if jdk.version!=8

    /**
     * Equivalent to {@link AccessibleObject#setAccessible(boolean) AccessibleObject.setAccessible(true)}, except that
     * it does not produce an illegal access error or warning.
     *
     * @param accessibleObject the instance to make accessible
     */
    public static void makeAccessible(AccessibleObject accessibleObject) {
        UNSAFE.putBooleanVolatile(accessibleObject, OVERRIDE, true);
    }
    //#endif

    public static long malloc(long usefulSize, int memoryTag) {
        try {
            assert memoryTag >= MemoryTag.NATIVE_PATH;
            checkAllocLimit(usefulSize, memoryTag);
            long prologSize = prologSize(usefulSize);
            long epilogSize = epilogSize(usefulSize);
            long rawSize = usefulSize + prologSize + epilogSize;
            long rawPtr = Unsafe.getUnsafe().allocateMemory(rawSize);
            tagMemoryAllocation(rawPtr, usefulSize);
            recordMemAlloc(rawSize, memoryTag);
            MALLOC_COUNT.incrementAndGet();
            return rawPtr + prologSize;
        } catch (OutOfMemoryError oom) {
            CairoException e = CairoException.nonCritical().setOutOfMemory(true)
                    .put("sun.misc.Unsafe.allocateMemory() OutOfMemoryError [RSS_MEM_USED=")
                    .put(RSS_MEM_USED.get())
                    .put(", size=")
                    .put(usefulSize)
                    .put(", memoryTag=").put(memoryTag)
                    .put("], original message: ")
                    .put(oom.getMessage());
            System.err.println(e.getFlyweightMessage());
            throw e;
        }
    }

    public static long realloc(long usefulOldAddress, long usefulOldSize, long usefulNewSize, int memoryTag) {
        try {
            if (usefulOldAddress == 0) {
                return malloc(usefulNewSize, memoryTag);
            }
            if (usefulNewSize == 0) {
                return free(usefulOldAddress, usefulOldSize, memoryTag);
            }

            assert memoryTag >= MemoryTag.NATIVE_PATH;
            long oldPrologSize = prologSize(usefulOldSize);
            long oldEpilogSize = epilogSize(usefulOldSize);
            long newPrologSize = prologSize(usefulNewSize);
            long newEpilogSize = epilogSize(usefulNewSize);

            long rawOldSize = oldPrologSize + usefulOldSize + oldEpilogSize;
            long rawOldPtr = usefulOldAddress - oldPrologSize;

            long rawNewSize = newPrologSize + usefulNewSize + newEpilogSize;
            long rawSizeDiff = rawNewSize - rawOldSize;

            if (rawOldPtr != 0) {
                checkMemoryTag(rawOldPtr, usefulOldSize);
            }

            long rawNewPtr = Unsafe.getUnsafe().allocateMemory(rawNewSize);
            tagMemoryAllocation(rawNewPtr, usefulNewSize);

            Vect.memcpy(rawNewPtr + newPrologSize, rawOldPtr + oldPrologSize, Math.min(usefulOldSize, usefulNewSize));

            Unsafe.getUnsafe().freeMemory(rawOldPtr);
            recordMemAlloc(rawSizeDiff, memoryTag);
            REALLOC_COUNT.incrementAndGet();
            return rawNewPtr + newPrologSize;
        } catch (OutOfMemoryError oom) {
            CairoException e = CairoException.nonCritical().setOutOfMemory(true)
                    .put("sun.misc.Unsafe.reallocateMemory() OutOfMemoryError [RSS_MEM_USED=")
                    .put(RSS_MEM_USED.get())
                    .put(", oldSize=")
                    .put(usefulOldSize)
                    .put(", newSize=")
                    .put(usefulNewSize)
                    .put(", memoryTag=").put(memoryTag)
                    .put("], original message: ")
                    .put(oom.getMessage());
            System.err.println(e.getFlyweightMessage());
            throw e;
        }
    }

    public static void recordMemAlloc(long size, int memoryTag) {
        long mem = MEM_USED.addAndGet(size);
        assert mem >= 0;
        assert memoryTag >= 0 && memoryTag < MemoryTag.SIZE;
        COUNTERS[memoryTag].add(size);
        if (memoryTag >= MemoryTag.NATIVE_DEFAULT) {
            RSS_MEM_USED.addAndGet(size);
        }
    }

    public static void setRssMemLimit(long limit) {
        RSS_MEM_LIMIT = limit;
    }

    //#if jdk.version!=8
    private static long AccessibleObject_override_fieldOffset() {
        if (isJava8Or11()) {
            return getFieldOffset(AccessibleObject.class, "override");
        }
        // From Java 12 onwards, AccessibleObject#override is protected and cannot be accessed reflectively.
        boolean is32BitJVM = is32BitJVM();
        if (is32BitJVM) {
            return 8L;
        }
        if (getOrdinaryObjectPointersCompressionStatus(is32BitJVM)) {
            return 12L;
        }
        return 16L;
    }

    private static void checkAllocLimit(long size, int memoryTag) {
        if (size <= 0) {
            return;
        }
        if (RSS_MEM_LIMIT > 0 && memoryTag >= NATIVE_DEFAULT) {
            long usage = RSS_MEM_USED.get();
            if (usage + size > RSS_MEM_LIMIT) {
                throw CairoException.nonCritical().setOutOfMemory(true)
                        .put("global RSS memory limit exceeded [usage=")
                        .put(usage)
                        .put(", RSS_MEM_LIMIT=").put(RSS_MEM_LIMIT)
                        .put(", size=").put(size)
                        .put(", memoryTag=").put(memoryTag)
                        .put(']');
            }
        }
    }

    private static void checkMemoryTag(long rawPtr, long usefulSize) {
        long prologSize = prologSize(usefulSize);
        long epilogSize = epilogSize(usefulSize);
        for (long l = rawPtr, hi = rawPtr + prologSize; l < hi; l++) {
            if (UNSAFE.getByte(l) != 42) {
                throw new RuntimeException("Memory tag mismatch");
            }
        }
        for (long l = rawPtr + prologSize + usefulSize, hi = rawPtr + prologSize + usefulSize + epilogSize; l < hi; l++) {
            if (UNSAFE.getByte(l) != 42) {
                throw new RuntimeException("Memory tag mismatch");
            }
        }
    }
    //#endif

    private static long epilogSize(long usefulSize) {
//        return usefulSize;
        return Math.min(64, usefulSize);
//        return 8;
    }

    //#if jdk.version!=8
    private static boolean getOrdinaryObjectPointersCompressionStatus(boolean is32BitJVM) {
        class Probe {
            @SuppressWarnings("unused")
            private int intField; // Accessed through reflection

            boolean probe() {
                long offset = getFieldOffset(Probe.class, "intField");
                if (offset == 8L) {
                    assert is32BitJVM;
                    return false;
                }
                if (offset == 12L) {
                    return true;
                }
                if (offset == 16L) {
                    return false;
                }
                throw new AssertionError(offset);
            }
        }
        return new Probe().probe();
    }
    //#endif

    //#if jdk.version!=8
    private static boolean is32BitJVM() {
        String sunArchDataModel = System.getProperty("sun.arch.data.model");
        return sunArchDataModel.equals("32");
    }
    //#endif

    //#if jdk.version!=8
    private static boolean isJava8Or11() {
        String javaVersion = System.getProperty("java.version");
        return javaVersion.startsWith("11") || javaVersion.startsWith("1.8");
    }
    //#endif

    // most significant bit
    private static int msb(int value) {
        return 31 - Integer.numberOfLeadingZeros(value);
    }

    private static long prologSize(long usefulSize) {
//        return usefulSize;
//        return Math.min(64, usefulSize);
        return 16;
    }

    private static void tagMemoryAllocation(long rawPtr, long usefulSize) {
        long prologSize = prologSize(usefulSize);
        long epilogSize = epilogSize(usefulSize);
        for (long l = rawPtr, hi = rawPtr + prologSize; l < hi; l++) {
            UNSAFE.putByte(l, (byte) 42);
        }
        for (long l = rawPtr + prologSize + usefulSize, hi = rawPtr + prologSize + usefulSize + epilogSize; l < hi; l++) {
            UNSAFE.putByte(l, (byte) 42);
        }
    }

    interface AnonymousClassDefiner {
        Class<?> define(Class<?> hostClass, byte[] data);
    }

    /**
     * Based on {@code MethodHandles.Lookup#defineHiddenClass}.
     */
    static class MethodHandlesClassDefiner implements AnonymousClassDefiner {

        private static Method defineMethod;
        private static Object hiddenClassOptions;
        private static Object lookupBase;
        private static long lookupOffset;

        @Nullable
        public static MethodHandlesClassDefiner newInstance() {
            if (defineMethod == null) {
                try {
                    Field trustedLookupField = MethodHandles.Lookup.class.getDeclaredField("IMPL_LOOKUP");
                    lookupBase = UNSAFE.staticFieldBase(trustedLookupField);
                    lookupOffset = UNSAFE.staticFieldOffset(trustedLookupField);
                    hiddenClassOptions = hiddenClassOptions("NESTMATE");
                    defineMethod = MethodHandles.Lookup.class
                            .getMethod("defineHiddenClass", byte[].class, boolean.class, hiddenClassOptions.getClass());
                } catch (ReflectiveOperationException e) {
                    return null;
                }
            }
            return new MethodHandlesClassDefiner();
        }

        @Override
        public Class<?> define(Class<?> hostClass, byte[] data) {
            try {
                MethodHandles.Lookup trustedLookup = (MethodHandles.Lookup) UNSAFE.getObject(lookupBase, lookupOffset);
                MethodHandles.Lookup definedLookup =
                        (MethodHandles.Lookup) defineMethod.invoke(trustedLookup.in(hostClass), data, false, hiddenClassOptions);
                return definedLookup.lookupClass();
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }

        @SuppressWarnings("unchecked")
        private static Object hiddenClassOptions(String... options) throws ClassNotFoundException {
            @SuppressWarnings("rawtypes")
            Class optionClass = Class.forName(MethodHandles.Lookup.class.getName() + "$ClassOption");
            Object classOptions = Array.newInstance(optionClass, options.length);
            for (int i = 0; i < options.length; i++) {
                Array.set(classOptions, i, Enum.valueOf(optionClass, options[i]));
            }
            return classOptions;
        }
    }

    /**
     * Based on {@code Unsafe#defineAnonymousClass}.
     */
    static class UnsafeClassDefiner implements AnonymousClassDefiner {

        private static Method defineMethod;

        @Nullable
        public static UnsafeClassDefiner newInstance() {
            if (defineMethod == null) {
                try {
                    defineMethod = sun.misc.Unsafe.class
                            .getMethod("defineAnonymousClass", Class.class, byte[].class, Object[].class);
                } catch (ReflectiveOperationException e) {
                    return null;
                }
            }
            return new UnsafeClassDefiner();
        }

        @Override
        public Class<?> define(Class<?> hostClass, byte[] data) {
            try {
                return (Class<?>) defineMethod.invoke(UNSAFE, hostClass, data, null);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    static {
        try {
            Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) theUnsafe.get(null);

            BYTE_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(byte[].class);
            BYTE_SCALE = msb(Unsafe.getUnsafe().arrayIndexScale(byte[].class));

            INT_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(int[].class);
            INT_SCALE = msb(Unsafe.getUnsafe().arrayIndexScale(int[].class));

            LONG_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(long[].class);
            LONG_SCALE = msb(Unsafe.getUnsafe().arrayIndexScale(long[].class));

            //#if jdk.version!=8
            OVERRIDE = AccessibleObject_override_fieldOffset();
            implAddExports = Module.class.getDeclaredMethod("implAddExports", String.class, Module.class);
            //#endif

            AnonymousClassDefiner classDefiner = UnsafeClassDefiner.newInstance();
            if (classDefiner == null) {
                classDefiner = MethodHandlesClassDefiner.newInstance();
            }
            if (classDefiner == null) {
                throw new InstantiationException("failed to initialize class definer");
            }
            anonymousClassDefiner = classDefiner;
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
        //#if jdk.version!=8
        makeAccessible(implAddExports);
        //#endif

        for (int i = 0; i < COUNTERS.length; i++) {
            COUNTERS[i] = new LongAdder();
        }
    }
}
