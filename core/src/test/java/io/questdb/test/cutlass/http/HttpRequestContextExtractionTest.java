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

package io.questdb.test.cutlass.http;

import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.HttpRequestContext;
import io.questdb.cutlass.http.HttpResponseSink;
import io.questdb.cutlass.http.SimpleResponse;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Structural checks for the {@link HttpRequestContext} interface extracted
 * in Wave 1 of the HTTP/2 integration per {@code HTTP2_INTEGRATION.md} §7.
 * The interface has to stay a strict subset of {@link HttpConnectionContext}'s
 * public API so the existing H1 path keeps compiling without changes.
 */
public class HttpRequestContextExtractionTest {

    @Test
    public void testHttpConnectionContextImplementsHttpRequestContext() {
        Assert.assertTrue(
                "HttpConnectionContext must implement HttpRequestContext so processors can be retargeted at the per-stream adapter in a later wave",
                HttpRequestContext.class.isAssignableFrom(HttpConnectionContext.class)
        );
    }

    @Test
    public void testInterfaceMethodsResolveOnHttpConnectionContext() throws NoSuchMethodException {
        for (Method m : HttpRequestContext.class.getMethods()) {
            if (Modifier.isStatic(m.getModifiers())) {
                continue;
            }
            final Method resolved = HttpConnectionContext.class.getMethod(m.getName(), m.getParameterTypes());
            Assert.assertTrue(
                    "HttpConnectionContext." + m.getName() + " must return a type assignable to the interface declaration",
                    m.getReturnType().isAssignableFrom(resolved.getReturnType())
            );
        }
    }

    @Test
    public void testSimpleResponseImplImplementsSimpleResponse() {
        Assert.assertTrue(
                "HttpResponseSink.SimpleResponseImpl must implement SimpleResponse so the H1 response sink satisfies HttpRequestContext.simpleResponse()",
                SimpleResponse.class.isAssignableFrom(HttpResponseSink.SimpleResponseImpl.class)
        );
    }
}
