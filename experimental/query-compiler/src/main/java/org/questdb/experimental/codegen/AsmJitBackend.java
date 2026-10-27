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
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;

/** Shared typed IR -> register/label stream -> in-process AsmJit Compiler. No per-query source compiler. */
public final class AsmJitBackend implements Backend {
    private final boolean dump;
    public AsmJitBackend(){this(false);}
    public AsmJitBackend(boolean dump){this.dump=dump;}
    @Override public String name(){return "asmjit";}

    @Override public CompiledKernel compile(Ir.Program program,Path artifacts)throws IOException {
        long started=System.nanoTime();
        var prepared=IrOptimizer.prepare(PortableVectorLowering.lower(HelperInlining.expand(program)));
        if(!System.getProperty("os.name").equals("Linux")||!System.getProperty("os.arch").equals("amd64"))
            throw new IOException("The experimental AsmJit lowering currently supports Linux x86-64");
        byte[] wire=prepared.ssa()==null?MachineCode.lower(prepared.structured()):MachineCode.lower(prepared.ssa());
        Path directory=null;
        if(dump) {
            Files.createDirectories(artifacts);
            directory=Files.createTempDirectory(artifacts,"asmjit-");
            Files.writeString(directory.resolve("query.ir"),prepared.dump());
            Files.write(directory.resolve("query.bin"),wire);
        }
        long address=compileBytes(wire);
        Arena callbacks=Arena.ofShared();
        try {
            Kernel kernel=NativeCalls.bind(MemorySegment.ofAddress(address),callbacks);
            AutoCloseable lifetime=()->{try{release(address);}finally{callbacks.close();}};
            return new CompiledKernel(kernel,lifetime,directory,System.nanoTime()-started);
        }catch(ReflectiveOperationException|RuntimeException|Error failure) {
            try{release(address);}finally{callbacks.close();}
            if(failure instanceof Error error)throw error;
            throw new IOException("Could not bind generated AsmJit kernel",failure);
        }
    }
    static long compileBytes(byte[] wire)throws IOException {
        try(Arena temporary=Arena.ofConfined()) {
            var input=temporary.allocate(wire.length,8);
            input.copyFrom(MemorySegment.ofArray(wire));
            var error=temporary.allocate(1024);
            var result=(MemorySegment)Bridge.COMPILE.invokeExact(input,(long)wire.length,error,1024L);
            if(result.address()==0)throw new IOException("AsmJit compilation failed: "+error.getString(0));
            return result.address();
        }catch(IOException e){throw e;}
        catch(LinkageError e){throw new IOException("Could not load the experimental AsmJit bridge",e);}
        catch(Throwable e){if(e instanceof Error error)throw error;throw new IOException("AsmJit bridge failed",e);}
    }
    static void release(long address) {
        try{Bridge.RELEASE.invokeExact(MemorySegment.ofAddress(address));}
        catch(Throwable e){throw new IllegalStateException("Could not release AsmJit kernel",e);}
    }
    static long liveFunctions() {
        try{return (long)Bridge.LIVE.invokeExact();}
        catch(Throwable e){throw new IllegalStateException(e);}
    }
    private static final class Bridge {
        static final MethodHandle COMPILE,RELEASE,LIVE;
        static {
            try {
                Path classes=Path.of(AsmJitBackend.class.getProtectionDomain().getCodeSource().getLocation().toURI());
                Path library=Path.of(System.getProperty("questdb.codegen.asmjit.library",
                        classes.getParent().resolve("native/libquery_codegen.so").toString())).toAbsolutePath();
                var lookup=SymbolLookup.libraryLookup(library,Arena.global());
                var linker=Linker.nativeLinker();
                COMPILE=linker.downcallHandle(lookup.find("query_compile").orElseThrow(),FunctionDescriptor.of(
                        ValueLayout.ADDRESS,ValueLayout.ADDRESS,ValueLayout.JAVA_LONG,ValueLayout.ADDRESS,ValueLayout.JAVA_LONG));
                RELEASE=linker.downcallHandle(lookup.find("query_release").orElseThrow(),FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
                LIVE=linker.downcallHandle(lookup.find("query_live_functions").orElseThrow(),FunctionDescriptor.of(ValueLayout.JAVA_LONG));
            }catch(Exception e){throw new ExceptionInInitializerError(e);}
        }
    }
}
