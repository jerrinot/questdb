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
import java.io.IOException;
import java.nio.*;
import java.nio.file.Path;
import static org.junit.Assert.*;
import static org.questdb.experimental.codegen.Ir.*;

public class AsmJitBackendTest {
    @Test public void rejectsMalformedWireWithoutInstallingCode()throws Exception {
        long before=AsmJitBackend.liveFunctions();
        assertThrows(IOException.class,()->AsmJitBackend.compileBytes(new byte[1]));
        Builder b=new Builder();b.exit(0);byte[] valid=MachineCode.lower(new Program(b.block()));
        for(int offset:new int[]{0,4,8,24,28}) {
            byte[] invalid=valid.clone();ByteBuffer.wrap(invalid).order(ByteOrder.LITTLE_ENDIAN).putInt(offset,-1);
            assertThrows("byte offset "+offset,IOException.class,()->AsmJitBackend.compileBytes(invalid));
            assertEquals(before,AsmJitBackend.liveFunctions());
        }
    }
    @Test public void lazyExpressionsAndLifetime()throws Exception {
        long before=AsmJitBackend.liveFunctions();Builder b=new Builder();
        b.when(binary(Op.BOTH,bool(false),eq(load(nil()),i64(0))),fail->fail.exit(99));
        b.when(binary(Op.EITHER,bool(true),eq(load(nil()),i64(0))),pass->pass.exit(42));b.exit(1);
        var kernel=new AsmJitBackend().compile(new Program(b.block()),Path.of("unused"));
        assertEquals(before+1,AsmJitBackend.liveFunctions());assertEquals(42,kernel.run(0,0,0));
        kernel.close();kernel.close();assertEquals(before,AsmJitBackend.liveFunctions());
        assertThrows(IllegalStateException.class,()->kernel.run(0,0,0));
    }
}
