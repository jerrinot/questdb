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

import io.questdb.Bootstrap;
import io.questdb.PropBootstrapConfiguration;
import io.questdb.ServerMain;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Map;

import static org.junit.Assert.*;

public class ServerLlvmTest {
    private static final String QUERY = "select sum(x * 2), count() from test_data where x % 3 = 0";
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void disabled() throws Exception {
        checkServer(false);
    }

    @Test public void enabled() throws Exception {
        checkServer(true);
    }

    private void checkServer(boolean enabled) throws Exception {
        long codeBefore = LlvmBackend.liveFunctions();
        var root = temporary.newFolder().toPath();
        Files.createDirectories(root.resolve("conf"));
        Files.writeString(root.resolve("conf/server.conf"), """
                cairo.sql.llvm.enabled=%s
                config.validation.strict=true
                http.bind.to=127.0.0.1:0
                pg.net.bind.to=127.0.0.1:0
                http.min.enabled=false
                line.tcp.enabled=false
                line.udp.enabled=false
                telemetry.enabled=false
                metrics.persist.enabled=false
                shared.worker.count=1
                shared.query.worker.count=2
                shared.write.worker.count=1
                http.worker.count=1
                pg.worker.count=1
                cairo.sql.page.frame.min.rows=32
                cairo.sql.page.frame.max.rows=128
                """.formatted(enabled));
        var bootstrap = new Bootstrap(new PropBootstrapConfiguration() {
            @Override public Map<String, String> getEnv() { return Map.of(); }
        }, Bootstrap.getServerMainArgs(root.toString()));
        try (var server = new ServerMain(bootstrap)) {
            server.start();
            var engine = server.getEngine();
            assertEquals(enabled, engine.getQueryCodeGenerator() instanceof ResolvedQueryCompiler);
            try (var context = new SqlExecutionContextImpl(engine, 2).with(AllowAllSecurityContext.INSTANCE)) {
                assertSame(engine.getQueryCodeGenerator(), context.getQueryCodeGenerator());
                engine.execute("create table test_data as (select x from long_sequence(1000))", context);
            }
            try (var client = HttpClient.newHttpClient()) {
                long beforeHttp = LlvmBackend.liveFunctions();
                for (int i = 0; i < 2; i++) {
                    String body = http(client, server, QUERY);
                    assertTrue(body, body.contains("\"dataset\":[[333666,333]]"));
                }
                if (enabled) assertTrue("HTTP must compile native code", LlvmBackend.liveFunctions() > beforeHttp);
                String fallback = http(client, server,
                        "select string_agg(cast(x as string), ',') from test_data where x <= 3");
                assertTrue(fallback, fallback.contains("\"dataset\":[[\"1,2,3\"]]"));
            }
            long beforePg = LlvmBackend.liveFunctions();
            try (var connection = DriverManager.getConnection(
                    "jdbc:postgresql://127.0.0.1:" + server.getPgWireServerPort() + "/qdb?prepareThreshold=1", "admin", "quest");
                 var statement = connection.prepareStatement(QUERY)) {
                for (int i = 0; i < 2; i++) {
                    try (var result = statement.executeQuery()) {
                        assertTrue(result.next());
                        assertEquals(333666, result.getLong(1));
                        assertEquals(333, result.getLong(2));
                        assertFalse(result.next());
                    }
                }
                if (enabled) assertTrue("PGWire must compile native code", LlvmBackend.liveFunctions() > beforePg);
            }
            if (!enabled) assertEquals(codeBefore, LlvmBackend.liveFunctions());
        }
        assertEquals("Server shutdown must release cached native code", codeBefore, LlvmBackend.liveFunctions());
    }

    private static String http(HttpClient client, ServerMain server, String sql) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.getHttpServerPort()
                        + "/exec?query=" + URLEncoder.encode(sql, StandardCharsets.UTF_8)))
                .timeout(Duration.ofSeconds(30)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(response.body(), 200, response.statusCode());
        return response.body();
    }
}
