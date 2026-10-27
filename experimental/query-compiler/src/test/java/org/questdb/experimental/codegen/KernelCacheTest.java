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

import org.junit.Test;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Ir.*;

public class KernelCacheTest {
    @Test public void evictionAndCloseKeepLeasedCodeAlive()throws Exception {
        var opens=new AtomicInteger();var closes=new AtomicInteger();
        Backend fake=new Backend() {
            @Override public String name(){return "fake";}
            @Override public CompiledKernel compile(Program p,Path root) {
                opens.incrementAndGet();
                return new CompiledKernel((a,b,c)->42,closes::incrementAndGet,root,123);
            }
        };
        Builder a=new Builder();a.exit(0);Builder b=new Builder();b.exit(1);
        var cache=new KernelCache(fake,1);
        var first=cache.compile(new Program(a.block()),Path.of("."));
        var hit=cache.compile(new Program(a.block()),Path.of("."));
        assertEquals(1,opens.get());assertEquals(0,hit.compilationNanos());assertEquals(1,cache.hits());
        var second=cache.compile(new Program(b.block()),Path.of("."));
        assertEquals(1,cache.size());assertEquals(0,closes.get());
        first.close();assertEquals(0,closes.get());
        assertEquals(42,hit.run(0,0,0));hit.close();assertEquals(1,closes.get());
        cache.close();assertEquals(1,closes.get());
        assertEquals(42,second.run(0,0,0));second.close();second.close();assertEquals(2,closes.get());
        assertThrows(IllegalStateException.class,()->cache.compile(new Program(a.block()),Path.of(".")));
    }
}
