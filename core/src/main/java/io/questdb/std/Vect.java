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

import io.questdb.cairo.BinarySearch;

public final class Vect {

    public static native double avgDoubleAcc(long pInt, long count, long pCount);

    public static native double avgIntAcc(long pInt, long count, long pCount);

    public static native double avgLongAcc(long pInt, long count, long pCount);

    public static native double avgShortAcc(long pInt, long count, long pCount);

    // Note: high is inclusive!
    public static native long binarySearch64Bit(long pData, long value, long low, long high, int scanDirection);

    // Note: high is inclusive!
    public static native long binarySearchIndexT(long pData, long value, long low, long high, int scanDirection);

    public static long boundedBinarySearch64Bit(long pData, long value, long low, long high, int scanDirection) {
        // Note: high is inclusive!
        long index = binarySearch64Bit(pData, value, low, high, scanDirection);
        if (index < 0) {
            return (-index - 1) - (scanDirection == BinarySearch.SCAN_UP ? 0 : 1);
        }
        return index;
    }

    public static long boundedBinarySearchIndexT(long pData, long value, long low, long high, int scanDirection) {
        // Negative values are not supported in timestamp index.
        if (value < 0) {
            return low - 1;
        }
        // Note: high is inclusive!
        long index = binarySearchIndexT(pData, value, low, high, scanDirection);
        if (index < 0) {
            return (-index - 1) - 1;
        }
        return index;
    }

    public static native void copyFromTimestampIndex(long pIndex, long indexLo, long indexHi, long pTs);

    public static native long countDouble(long pDouble, long count);

    public static native long countInt(long pLong, long count);

    public static native long countLong(long pLong, long count);

    public static native long dedupMergeStrBinColumnSize(long mergeIndexAddr, long mergeIndexCount, long srcDataFixAddr, long srcOooFixAddr);

    public static native long dedupMergeVarcharColumnSize(long mergeIndexAddr, long mergeIndexCount, long srcDataFixAddr, long srcOooFixAddr);

    public static long dedupMergeVarcharColumnSizeChecked(long mergeIndexAddr, long mergeIndexCount, long srcDataFixAddr, long srcOooFixAddr) {
        Unsafe.assertAllocatedMemory(mergeIndexAddr, mergeIndexCount * 16);
        return dedupMergeVarcharColumnSize(mergeIndexAddr, mergeIndexCount, srcDataFixAddr, srcOooFixAddr);
    }

    public static native long dedupSortedTimestampIndex(
            long inIndexAddr,
            long count,
            long outIndexAddr,
            long indexAddrTemp,
            int dedupColumnCount,
            long dedupColumnData
    );

    public static long dedupSortedTimestampIndexIntKeysChecked(
            long inIndexAddr,
            long count,
            long outIndexAddr,
            long indexAddrTemp,
            int dedupColumnCount,
            long dedupColumnData
    ) {
        long dedupCount = dedupSortedTimestampIndex(
                inIndexAddr,
                count,
                outIndexAddr,
                indexAddrTemp,
                dedupColumnCount,
                dedupColumnData
        );
        assert dedupCount != -1 : "unsorted data passed to deduplication";
        return dedupCount;
    }

    public static native void flattenIndex(long pIndex, long count);

    public static native long getPerformanceCounter(int index);

    public static native int getPerformanceCountersCount();

    public static native int getSupportedInstructionSet();

    public static String getSupportedInstructionSetName() {
        int inst = getSupportedInstructionSet();
        String base;
        if (inst >= 10) {
            base = "AVX512";
        } else if (inst >= 8) {
            base = "AVX2";
        } else if (inst >= 5) {
            base = "SSE4.1";
        } else if (inst >= 2) {
            base = "SSE2";
        } else {
            base = "Vanilla";
        }
        return " [" + base + "," + Vect.getSupportedInstructionSet() + "]";
    }

    public static native void indexReshuffle128Bit(long pSrc, long pDest, long pIndex, long count);

    public static native void indexReshuffle16Bit(long pSrc, long pDest, long pIndex, long count);

    public static native void indexReshuffle256Bit(long pSrc, long pDest, long pIndex, long count);

    public static native void indexReshuffle32Bit(long pSrc, long pDest, long pIndex, long count);

    public static native void indexReshuffle64Bit(long pSrc, long pDest, long pIndex, long count);

    public static native void indexReshuffle8Bit(long pSrc, long pDest, long pIndex, long count);

    public static native double maxDouble(long pDouble, long count);

    public static native int maxInt(long pInt, long count);

    public static native long maxLong(long pLong, long count);

    public static native int maxShort(long pLong, long count);

    public static void memcpy(long dst, long src, long len) {
        Unsafe.assertAllocatedMemory(dst, len);
        Unsafe.assertAllocatedMemory(src, len);

        // the split length was determined experimentally
        // using 'MemCopyBenchmark' bench
        if (len < 4096) {
            Unsafe.getUnsafe().copyMemory(src, dst, len);
        } else {
            memcpy0(src, dst, len);
        }
    }

    public static boolean memeq(long a, long b, long len) {
        Unsafe.assertAllocatedMemory(a, len);
        Unsafe.assertAllocatedMemory(b, len);
        // the split length was determined experimentally
        // using 'MemEqBenchmark' bench
        if (len < 128) {
            return memeq0(a, b, len);
        } else {
            return memcmp(a, b, len) == 0;
        }
    }

    public static native void memmove(long dst, long src, long len);

    public static native void memset(long dst, long len, int value);

    // note: memset only uses single byte of the given int
    public static void memsetChecked(long dst, long len, int value) {
        Unsafe.assertAllocatedMemory(dst, len);
        memset(dst, len, value);
    }

    public static native long mergeDedupTimestampWithLongIndexAsc(
            long pSrc,
            long srcLo,
            long srcHiInclusive,
            long pIndex,
            long indexLo,
            long indexHiInclusive,
            long pDestIndex
    );

    public static native long mergeDedupTimestampWithLongIndexIntKeys(
            long srcTimestampAddr,
            long mergeDataLo,
            long mergeDataHi,
            long sortedTimestampsAddr,
            long mergeOOOLo,
            long mergeOOOHi,
            long tempIndexAddr,
            int dedupKeyCount,
            long dedupColBuffs
    );

    public static void mergeLongIndexesAsc(long pIndexStructArray, int count, long mergedIndexAddr) {
        if (count < 2) {
            throw new IllegalArgumentException("Count of indexes to merge should at least be 2.");
        }

        mergeLongIndexesAscInner(pIndexStructArray, count, mergedIndexAddr);
    }

    public static native void mergeShuffle128Bit(long pSrc1, long pSrc2, long pDest, long pIndex, long count);

    public static void mergeShuffle128BitChecked(long pSrc1, long pSrc2, long pDest, long pIndex, long count) {
        Unsafe.assertAllocatedMemory(pDest, count * 16);
        mergeShuffle128Bit(pSrc1, pSrc2, pDest, pIndex, count);
    }

    public static native void mergeShuffle16Bit(long pSrc1, long pSrc2, long pDest, long pIndex, long count);

    public static void mergeShuffle16BitChecked(long pSrc1, long pSrc2, long pDest, long pIndex, long count) {
        Unsafe.assertAllocatedMemory(pDest, count * 2);
        mergeShuffle16Bit(pSrc1, pSrc2, pDest, pIndex, count);
    }

    public static native void mergeShuffle256Bit(long pSrc1, long pSrc2, long pDest, long pIndex, long count);

    public static void mergeShuffle256BitChecked(long pSrc1, long pSrc2, long pDest, long pIndex, long count) {
        Unsafe.assertAllocatedMemory(pDest, count * 32);
        mergeShuffle256Bit(pSrc1, pSrc2, pDest, pIndex, count);
    }

    public static native void mergeShuffle32Bit(long pSrc1, long pSrc2, long pDest, long pIndex, long count);

    public static void mergeShuffle32BitChecked(long pSrc1, long pSrc2, long pDest, long pIndex, long count) {
        Unsafe.assertAllocatedMemory(pDest, count * 4);
        mergeShuffle32Bit(pSrc1, pSrc2, pDest, pIndex, count);
    }

    public static native void mergeShuffle64Bit(long pSrc1, long pSrc2, long pDest, long pIndex, long count);

    public static void mergeShuffle64BitChecked(long pSrc1, long pSrc2, long pDest, long pIndex, long count) {
        Unsafe.assertAllocatedMemory(pDest, count * 8);
        mergeShuffle64Bit(pSrc1, pSrc2, pDest, pIndex, count);
    }

    public static native void mergeShuffle8Bit(long pSrc1, long pSrc2, long pDest, long pIndex, long count);

    public static void mergeShuffle8BitChecked(long pSrc1, long pSrc2, long pDest, long pIndex, long count) {
        Unsafe.assertAllocatedMemory(pDest, count);
        mergeShuffle8Bit(pSrc1, pSrc2, pDest, pIndex, count);
    }

    public static native long mergeTwoLongIndexesAsc(long pTs, long tsIndexLo, long tsCount, long pIndex2, long index2Count, long pIndexDest);

    public static native double minDouble(long pDouble, long count);

    public static native int minInt(long pInt, long count);

    public static native long minLong(long pLong, long count);

    public static native int minShort(long pLong, long count);

    public static native void oooCopyIndex(long mergeIndexAddr, long mergeIndexSize, long dstAddr);

    public static void oooCopyIndexChecked(long mergeIndexAddr, long mergeIndexSize, long dstAddr) {
        Unsafe.assertAllocatedMemory(dstAddr, mergeIndexSize * 8);
        oooCopyIndex(mergeIndexAddr, mergeIndexSize, dstAddr);
    }

    public static native void oooMergeCopyBinColumn(
            long mergeIndexAddr,
            long mergeIndexSize,
            long srcDataFixAddr,
            long srcDataVarAddr,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long dstFixAddr,
            long dstVarAddr,
            long dstVarOffset
    );

    public static void oooMergeCopyBinColumnChecked(
            long mergeIndexAddr,
            long mergeIndexSize,
            long srcDataFixAddr,
            long srcDataVarAddr,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long dstFixAddr,
            long dstVarAddr,
            long dstVarOffset
    ) {
        Unsafe.assertAllocatedMemory(dstFixAddr, mergeIndexSize * 8);
        oooMergeCopyBinColumn(
                mergeIndexAddr,
                mergeIndexSize,
                srcDataFixAddr,
                srcDataVarAddr,
                srcOooFixAddr,
                srcOooVarAddr,
                dstFixAddr,
                dstVarAddr,
                dstVarOffset
        );
    }

    public static native void oooMergeCopyStrColumn(
            long mergeIndexAddr,
            long mergeIndexSize,
            long srcDataFixAddr,
            long srcDataVarAddr,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long dstFixAddr,
            long dstVarAddr,
            long dstVarOffset
    );

    public static void oooMergeCopyStrColumnChecked(
            long mergeIndexAddr,
            long mergeIndexSize,
            long srcDataFixAddr,
            long srcDataVarAddr,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long dstFixAddr,
            long dstVarAddr,
            long dstVarOffset
    ) {
        Unsafe.assertAllocatedMemory(dstFixAddr, mergeIndexSize * 8);
        oooMergeCopyStrColumn(
                mergeIndexAddr,
                mergeIndexSize,
                srcDataFixAddr,
                srcDataVarAddr,
                srcOooFixAddr,
                srcOooVarAddr,
                dstFixAddr,
                dstVarAddr,
                dstVarOffset
        );
    }

    public static native void oooMergeCopyVarcharColumn(
            long mergeIndexAddr,
            long mergeIndexSize,
            long srcDataFixAddr,
            long srcDataVarAddr,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long dstFixAddr,
            long dstVarAddr,
            long dstVarOffset
    );

    public static native void quickSortLongIndexAscInPlace(long pLongData, long count);

    public static void quickSortLongIndexAscInPlaceChecked(long pLongData, long count) {
        Unsafe.assertAllocatedMemory(pLongData, count * 16);
        quickSortLongIndexAscInPlace(pLongData, count);
    }

    public static native void radixSortABLongIndexAsc(long pDataA, long countA, long pDataB, long countB, long pDataDest, long pDataCpy);

    public static void radixSortABLongIndexAscChecked(long pDataA, long countA, long pDataB, long countB, long pDataDest, long pDataCpy) {
        Unsafe.assertAllocatedMemory(pDataA, countA * 8);
        Unsafe.assertAllocatedMemory(pDataB, countB * 8);
        Unsafe.assertAllocatedMemory(pDataDest, countA * 8 + countB * 8);
        Unsafe.assertAllocatedMemory(pDataCpy, countA * 8 + countB * 8);
        radixSortABLongIndexAsc(pDataA, countA, pDataB, countB, pDataDest, pDataCpy);
    }

    public static native void radixSortLongIndexAscInPlace(long pLongData, long count, long pCpy);

    // This is not In Place sort, to be renamed later
    public static void radixSortLongIndexAscInPlaceChecked(long pLongData, long count, long pCpy) {
        Unsafe.assertAllocatedMemory(pLongData, count * 16);
        Unsafe.assertAllocatedMemory(pCpy, count * 16);
        radixSortLongIndexAscInPlace(pLongData, count, pCpy);
    }

    public static native void resetPerformanceCounters();

    public static native void setMemoryDouble(long pData, double value, long count);

    public static native void setMemoryFloat(long pData, float value, long count);

    public static native void setMemoryInt(long pData, int value, long count);

    public static native void setMemoryLong(long pData, long value, long count);

    public static native void setMemoryShort(long pData, short value, long count);

    public static native void setVarColumnRefs32Bit(long address, long initialOffset, long count);

    public static native void setVarColumnRefs64Bit(long address, long initialOffset, long count);

    public static native void setVarcharColumnNullRefs(long address, long initialOffset, long count);

    public static native void shiftCopyFixedSizeColumnData(long shift, long srcAddr, long srcLo, long srcHi, long dstAddr);

    public static native void shiftCopyVarcharColumnAux(long shift, long srcAddr, long srcLo, long srcHi, long dstAddr);

    public static native long shiftTimestampIndex(long pSrc, long count, long pDest);

    public static long shiftTimestampIndexChecked(long pSrc, long count, long pDest) {
        Unsafe.assertAllocatedMemory(pSrc, count * 8);
        Unsafe.assertAllocatedMemory(pDest, count * 8);
        return shiftTimestampIndex(pSrc, count, pDest);
    }

    /**
     * Sorts assuming 128-bit integers.
     * Can be used to sort pairs of longs from DirectLongList when both longs are positive
     *
     * @param pLongData memory address
     * @param count     count of 128bit integers to sort
     */
    public static native void sort128BitAscInPlace(long pLongData, long count);

    public static native void sort3LongAscInPlace(long address, long count);

    public static void sort3LongAscInPlaceChecked(long address, long count) {
        Unsafe.assertAllocatedMemory(address, count * 24);
        sort3LongAscInPlace(address, count);
    }

    public static native void sortLongIndexAscInPlace(long pLongData, long count);

    public static void sortLongIndexAscInPlaceChecked(long pLongData, long count) {
        Unsafe.assertAllocatedMemory(pLongData, count * 16);
        sortLongIndexAscInPlace(pLongData, count);
    }

    public static native void sortULongAscInPlace(long pLongData, long count);

    public static void sortULongAscInPlaceChecked(long pLongData, long count) {
        Unsafe.assertAllocatedMemory(pLongData, count * 8);
        sortULongAscInPlace(pLongData, count);
    }

    public static native long sortVarColumn(
            long mergedTimestampsAddr,
            long valueCount,
            long srcDataAddr,
            long srcIndxAddr,
            long tgtDataAddr,
            long tgtIndxAdd
    );

    public static native long sortVarcharColumn(
            long mergedTimestampsAddr,
            long valueCount,
            long srcDataAddr,
            long srcAuxAddr,
            long tgtDataAddr,
            long tgtAuxAdd
    );

    public static native double sumDouble(long pDouble, long count);

    public static native double sumDoubleKahan(long pDouble, long count);

    public static native double sumDoubleNeumaier(long pDouble, long count);

    public static native long sumInt(long pInt, long count);

    public static native long sumLong(long pLong, long count);

    public static native long sumShort(long pLong, long count);

    private static native int memcmp(long src, long dst, long len);

    private static native void memcpy0(long src, long dst, long len);

    private static boolean memeq0(long a, long b, long len) {
        long i = 0;
        for (; i + 7 < len; i += 8) {
            if (Unsafe.getUnsafe().getLong(a + i) != Unsafe.getUnsafe().getLong(b + i)) {
                return false;
            }
        }
        if (i + 3 < len) {
            if (Unsafe.getUnsafe().getInt(a + i) != Unsafe.getUnsafe().getInt(b + i)) {
                return false;
            }
            i += 4;
        }
        for (; i < len; i++) {
            if (Unsafe.getUnsafe().getByte(a + i) != Unsafe.getUnsafe().getByte(b + i)) {
                return false;
            }
        }
        return true;
    }

    // accept externally allocated memory for merged index of proper size
    private static native void mergeLongIndexesAscInner(long pIndexStructArray, int count, long mergedIndexAddr);

    static {
        Os.init();
    }
}
