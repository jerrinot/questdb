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

package io.questdb.std.str;

import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public final class StdoutSink implements Utf8Sink, Closeable {

    public static final StdoutSink INSTANCE = new StdoutSink();
    private final int bufferCapacity = 1024;
    private final long buffer = Unsafe.malloc(bufferCapacity, MemoryTag.NATIVE_DEFAULT);
    private final long limit = buffer + bufferCapacity;
    private long ptr = buffer;
    private final long stdout = Files.getStdOutFdInternal();

    @Override
    public void close() {
        Unsafe.free(buffer, bufferCapacity, MemoryTag.NATIVE_DEFAULT);
    }

    public void flush() {
        int len = (int) (ptr - buffer);
        if (len > 0) {
            Files.append(stdout, buffer, len);
            ptr = buffer;
        }
    }

    @Override
    public Utf8Sink put(@Nullable Utf8Sequence us) {
        if (us != null) {
            for (int i = 0, size = us.size(); i < size; i++) {
                put(us.byteAt(i));
            }
        }
        return this;
    }

    @Override
    public Utf8Sink put(byte b) {
        if (ptr == limit) {
            flush();
        }
        Unsafe.putByte(ptr++, b);
        return this;
    }

    @Override
    public Utf8Sink putNonAscii(long lo, long hi) {
        long remaining = hi - lo;
        while (remaining > 0) {
            final long avail = limit - ptr;
            if (avail > 0) {
                final long chunkSize = Math.min(avail, remaining);
                Vect.memcpy(ptr, hi - remaining, chunkSize);
                ptr += chunkSize;
                remaining -= chunkSize;
            }
            if (remaining > 0) {
                flush();
            }
        }
        return this;
    }
}
