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

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;

/** Small serial-use cache of immutable code. Query memory and scan factories are never cached here. */
public final class KernelCache implements Backend, AutoCloseable {
    private final Backend backend;
    private final int limit;
    private final LinkedHashMap<Ir.Program,Entry> entries=new LinkedHashMap<>(16,0.75f,true);
    private boolean closed;
    private long hits,misses;
    public KernelCache(Backend backend,int limit) {
        if(limit<1)throw new IllegalArgumentException("Positive cache limit required");
        this.backend=backend;this.limit=limit;
    }
    @Override public String name(){return backend.name()+"-cached";}
    public long hits(){return hits;}
    public long misses(){return misses;}
    public int size(){return entries.size();}
    @Override public CompiledKernel compile(Ir.Program program,Path artifacts)throws IOException {
        if(closed)throw new IllegalStateException("Cache closed");
        var entry=entries.get(program);
        boolean hit=entry!=null;
        if(hit)hits++;
        else {
            misses++;
            entry=new Entry(backend.compile(program,artifacts));
            entries.put(program,entry);
            if(entries.size()>limit)retire(entries.pollFirstEntry().getValue());
        }
        entry.leases++;
        var leased=entry;
        return new CompiledKernel(leased.code,()->release(leased),leased.code.artifacts(),hit?0:leased.code.compilationNanos());
    }
    private void release(Entry e){if(--e.leases==0&&!e.resident)e.code.close();}
    private void retire(Entry e){e.resident=false;if(e.leases==0)e.code.close();}
    @Override public void close() {
        if(!closed) {
            closed=true;
            RuntimeException failure=null;
            for(var e:entries.values()) {
                try {retire(e);}
                catch(RuntimeException problem){if(failure==null)failure=problem;else failure.addSuppressed(problem);}
            }
            entries.clear();
            if(failure!=null)throw failure;
        }
    }
    private static final class Entry {
        final CompiledKernel code;
        boolean resident=true;
        int leases;
        Entry(CompiledKernel code){this.code=code;}
    }
}
