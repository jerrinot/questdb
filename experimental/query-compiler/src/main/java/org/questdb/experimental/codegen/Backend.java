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
import java.util.List;

public interface Backend {
    default CompilerOptions options() { return CompilerOptions.DEFAULT; }
    /** Keep deferred lowering and any subsequently bound filter on the same planning snapshot. */
    default Backend withOptions(CompilerOptions options) {
        java.util.Objects.requireNonNull(options);
        if (options.equals(options())) return this;
        Backend delegate = this;
        return new Backend() {
            @Override public CompilerOptions options() { return options; }
            @Override public String name() { return delegate.name(); }
            @Override public CompiledKernel compile(Ir.Program program, Path path) throws IOException {
                return delegate.compile(program, path);
            }
        };
    }
    CompiledKernel compile(Ir.Program program, Path artifactRoot) throws IOException;
    /** Alternatives must implement exactly the same state transition and ABI. */
    default CompiledKernel compileAlternatives(Ir.Program fused, Ir.Program masked, Path artifactRoot) throws IOException {
        return compileAlternatives(List.of(fused, masked), artifactRoot);
    }
    default CompiledKernel compileAlternatives(List<Ir.Program> alternatives, Path artifactRoot) throws IOException {
        return AdaptiveKernel.compile(this, alternatives, artifactRoot);
    }
    String name();
}
