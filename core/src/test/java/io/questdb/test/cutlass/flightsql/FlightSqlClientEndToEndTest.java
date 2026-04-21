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

package io.questdb.test.cutlass.flightsql;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.charset.StandardCharsets;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Wave 7b end-to-end tests: drive QuestDB's Flight SQL server with Arrow's
 * official {@link FlightSqlClient} (grpc-netty under the hood). Complements
 * {@link FlightSqlQueryEndToEndTest}, which exercises the same server with
 * hand-rolled H2/gRPC framing for byte-level regression coverage.
 */
public class FlightSqlClientEndToEndTest extends AbstractBootstrapTest {

    private static RootAllocator allocator;

    @BeforeClass
    public static void setUpAllocator() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterClass
    public static void tearDownAllocator() {
        if (allocator != null) {
            allocator.close();
            allocator = null;
        }
    }

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testHandshake() throws Exception {
        try (TestServerMain serverMain = startFlightSqlServer();
             FlightClient flightClient = openFlightClient(serverMain)) {
            flightClient.handshake();
        }
    }

    @Test
    public void testMalformedSqlRaisesInvalidArgument() throws Exception {
        try (TestServerMain serverMain = startFlightSqlServer();
             FlightClient flightClient = openFlightClient(serverMain);
             FlightSqlClient sqlClient = new FlightSqlClient(flightClient)) {
            FlightRuntimeException ex = null;
            try {
                sqlClient.execute("SELECT FROM nowhere");
            } catch (FlightRuntimeException e) {
                ex = e;
            }
            Assert.assertNotNull("expected FlightRuntimeException for malformed SQL", ex);
            Assert.assertEquals(FlightStatusCode.INVALID_ARGUMENT, ex.status().code());
        }
    }

    @Test
    public void testMultiBatchBoundary() throws Exception {
        try (TestServerMain serverMain = startFlightSqlServer();
             FlightClient flightClient = openFlightClient(serverMain);
             FlightSqlClient sqlClient = new FlightSqlClient(flightClient)) {
            FlightInfo info = sqlClient.execute("SELECT nullif(x, 4096) n FROM long_sequence(8192)");
            Assert.assertEquals(1, info.getEndpoints().size());
            try (FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
                int totalRows = 0;
                int totalNulls = 0;
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    BigIntVector n = (BigIntVector) root.getVector("n");
                    int rows = root.getRowCount();
                    for (int i = 0; i < rows; i++) {
                        if (n.isNull(i)) {
                            totalNulls++;
                        }
                    }
                    totalRows += rows;
                }
                Assert.assertEquals(8192, totalRows);
                Assert.assertEquals(1, totalNulls);
            }
        }
    }

    @Test
    public void testNullDate() throws Exception {
        try (TestServerMain serverMain = startFlightSqlServer();
             FlightClient flightClient = openFlightClient(serverMain);
             FlightSqlClient sqlClient = new FlightSqlClient(flightClient)) {
            FlightInfo info = sqlClient.execute("SELECT cast(null AS date) d FROM long_sequence(3)");
            try (FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
                Assert.assertTrue(stream.next());
                VectorSchemaRoot root = stream.getRoot();
                DateMilliVector d = (DateMilliVector) root.getVector("d");
                Assert.assertEquals(3, root.getRowCount());
                for (int i = 0; i < 3; i++) {
                    Assert.assertTrue("row " + i + " must be null", d.isNull(i));
                }
                Assert.assertFalse(stream.next());
            }
        }
    }

    @Test
    public void testNullInMiddleDouble() throws Exception {
        try (TestServerMain serverMain = startFlightSqlServer();
             FlightClient flightClient = openFlightClient(serverMain);
             FlightSqlClient sqlClient = new FlightSqlClient(flightClient)) {
            FlightInfo info = sqlClient.execute("SELECT cast(nullif(x, 2) AS double) d FROM long_sequence(4)");
            try (FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
                Assert.assertTrue(stream.next());
                VectorSchemaRoot root = stream.getRoot();
                Float8Vector d = (Float8Vector) root.getVector("d");
                Assert.assertEquals(4, root.getRowCount());
                Assert.assertFalse(d.isNull(0));
                Assert.assertTrue(d.isNull(1));
                Assert.assertFalse(d.isNull(2));
                Assert.assertFalse(d.isNull(3));
                Assert.assertFalse(stream.next());
            }
        }
    }

    @Test
    public void testNullInMiddleLong() throws Exception {
        try (TestServerMain serverMain = startFlightSqlServer();
             FlightClient flightClient = openFlightClient(serverMain);
             FlightSqlClient sqlClient = new FlightSqlClient(flightClient)) {
            FlightInfo info = sqlClient.execute("SELECT nullif(x, 2) n FROM long_sequence(4)");
            try (FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
                Assert.assertTrue(stream.next());
                VectorSchemaRoot root = stream.getRoot();
                BigIntVector n = (BigIntVector) root.getVector("n");
                Assert.assertEquals(4, root.getRowCount());
                Assert.assertFalse(n.isNull(0));
                Assert.assertTrue(n.isNull(1));
                Assert.assertFalse(n.isNull(2));
                Assert.assertFalse(n.isNull(3));
                Assert.assertEquals(1L, n.get(0));
                Assert.assertEquals(3L, n.get(2));
                Assert.assertEquals(4L, n.get(3));
                Assert.assertFalse(stream.next());
            }
        }
    }

    @Test
    public void testNullTimestamp() throws Exception {
        try (TestServerMain serverMain = startFlightSqlServer();
             FlightClient flightClient = openFlightClient(serverMain);
             FlightSqlClient sqlClient = new FlightSqlClient(flightClient)) {
            FlightInfo info = sqlClient.execute("SELECT cast(null AS timestamp) t FROM long_sequence(3)");
            try (FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
                Assert.assertTrue(stream.next());
                VectorSchemaRoot root = stream.getRoot();
                TimeStampMicroVector t = (TimeStampMicroVector) root.getVector("t");
                Assert.assertEquals(3, root.getRowCount());
                for (int i = 0; i < 3; i++) {
                    Assert.assertTrue("row " + i + " must be null", t.isNull(i));
                }
                Assert.assertFalse(stream.next());
            }
        }
    }

    @Test
    public void testScalarTypeSweep() throws Exception {
        try (TestServerMain serverMain = startFlightSqlServer();
             FlightClient flightClient = openFlightClient(serverMain);
             FlightSqlClient sqlClient = new FlightSqlClient(flightClient)) {
            FlightInfo info = sqlClient.execute(
                    "SELECT cast(x AS byte) b, cast(x AS short) s, cast(x AS int) i, x l, "
                            + "cast(x AS float) f, cast(x AS double) d, (x % 2 = 0) bo "
                            + "FROM long_sequence(3)");
            try (FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
                Assert.assertTrue(stream.next());
                VectorSchemaRoot root = stream.getRoot();
                Assert.assertEquals(3, root.getRowCount());

                FieldVector b = root.getVector("b");
                FieldVector s = root.getVector("s");
                FieldVector i = root.getVector("i");
                FieldVector l = root.getVector("l");
                FieldVector f = root.getVector("f");
                FieldVector d = root.getVector("d");
                FieldVector bo = root.getVector("bo");

                Assert.assertTrue("b must be TinyIntVector, was " + b.getClass(), b instanceof TinyIntVector);
                Assert.assertTrue("s must be SmallIntVector, was " + s.getClass(), s instanceof SmallIntVector);
                Assert.assertTrue("i must be IntVector, was " + i.getClass(), i instanceof IntVector);
                Assert.assertTrue("l must be BigIntVector, was " + l.getClass(), l instanceof BigIntVector);
                Assert.assertTrue("f must be Float4Vector, was " + f.getClass(), f instanceof Float4Vector);
                Assert.assertTrue("d must be Float8Vector, was " + d.getClass(), d instanceof Float8Vector);
                Assert.assertTrue("bo must be BitVector, was " + bo.getClass(), bo instanceof BitVector);

                Assert.assertEquals(1, ((TinyIntVector) b).get(0));
                Assert.assertEquals(1, ((SmallIntVector) s).get(0));
                Assert.assertEquals(1, ((IntVector) i).get(0));
                Assert.assertEquals(1L, ((BigIntVector) l).get(0));
                Assert.assertEquals(1.0f, ((Float4Vector) f).get(0), 0.0f);
                Assert.assertEquals(1.0, ((Float8Vector) d).get(0), 0.0);
                Assert.assertEquals(0, ((BitVector) bo).get(0));

                Assert.assertFalse(stream.next());
            }
        }
    }

    @Test
    public void testSelectDate() throws Exception {
        try (TestServerMain serverMain = startFlightSqlServer();
             FlightClient flightClient = openFlightClient(serverMain);
             FlightSqlClient sqlClient = new FlightSqlClient(flightClient)) {
            FlightInfo info = sqlClient.execute("SELECT cast('2024-01-01' AS date) d FROM long_sequence(3)");
            try (FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
                Assert.assertTrue(stream.next());
                VectorSchemaRoot root = stream.getRoot();
                DateMilliVector d = (DateMilliVector) root.getVector("d");
                Assert.assertEquals(3, root.getRowCount());
                long expected = 1_704_067_200_000L;
                for (int i = 0; i < 3; i++) {
                    Assert.assertFalse("row " + i + " must be valid", d.isNull(i));
                    Assert.assertEquals(expected, d.get(i));
                }
                Assert.assertFalse(stream.next());
            }
        }
    }

    @Test
    public void testSelectLongSequence() throws Exception {
        try (TestServerMain serverMain = startFlightSqlServer();
             FlightClient flightClient = openFlightClient(serverMain);
             FlightSqlClient sqlClient = new FlightSqlClient(flightClient)) {
            FlightInfo info = sqlClient.execute("SELECT x FROM long_sequence(3)");
            Assert.assertEquals(1, info.getEndpoints().size());
            try (FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
                Assert.assertTrue(stream.next());
                VectorSchemaRoot root = stream.getRoot();
                BigIntVector x = (BigIntVector) root.getVector("x");
                Assert.assertEquals(3, root.getRowCount());
                Assert.assertEquals(1L, x.get(0));
                Assert.assertEquals(2L, x.get(1));
                Assert.assertEquals(3L, x.get(2));
                Assert.assertFalse(stream.next());
            }
        }
    }

    @Test
    public void testSelectMultiColumnMixed() throws Exception {
        try (TestServerMain serverMain = startFlightSqlServer();
             FlightClient flightClient = openFlightClient(serverMain);
             FlightSqlClient sqlClient = new FlightSqlClient(flightClient)) {
            FlightInfo info = sqlClient.execute(
                    "SELECT x l, cast('v' || x AS varchar) v, "
                            + "x::double d, "
                            + "cast('2024-01-01T12:00:00.000000Z' AS timestamp) t "
                            + "FROM long_sequence(4)");
            try (FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
                Assert.assertTrue(stream.next());
                VectorSchemaRoot root = stream.getRoot();
                BigIntVector l = (BigIntVector) root.getVector("l");
                VarCharVector v = (VarCharVector) root.getVector("v");
                Float8Vector d = (Float8Vector) root.getVector("d");
                TimeStampMicroVector t = (TimeStampMicroVector) root.getVector("t");
                Assert.assertEquals(4, root.getRowCount());
                for (int i = 0; i < 4; i++) {
                    Assert.assertEquals(i + 1L, l.get(i));
                    Assert.assertEquals("v" + (i + 1), new String(v.get(i), StandardCharsets.UTF_8));
                    Assert.assertEquals((double) (i + 1), d.get(i), 0.0);
                    Assert.assertEquals(1_704_110_400_000_000L, t.get(i));
                }
                Assert.assertFalse(stream.next());
            }
        }
    }

    @Test
    public void testSelectString() throws Exception {
        try (TestServerMain serverMain = startFlightSqlServer();
             FlightClient flightClient = openFlightClient(serverMain);
             FlightSqlClient sqlClient = new FlightSqlClient(flightClient)) {
            FlightInfo info = sqlClient.execute("SELECT 'hello' AS s FROM long_sequence(3)");
            try (FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
                Assert.assertTrue(stream.next());
                VectorSchemaRoot root = stream.getRoot();
                VarCharVector s = (VarCharVector) root.getVector("s");
                Assert.assertEquals(3, root.getRowCount());
                for (int i = 0; i < 3; i++) {
                    Assert.assertFalse("row " + i + " must be valid", s.isNull(i));
                    Assert.assertEquals("hello", new String(s.get(i), StandardCharsets.UTF_8));
                }
                Assert.assertFalse(stream.next());
            }
        }
    }

    @Test
    public void testSelectSymbol() throws Exception {
        try (TestServerMain serverMain = startFlightSqlServer();
             FlightClient flightClient = openFlightClient(serverMain);
             FlightSqlClient sqlClient = new FlightSqlClient(flightClient)) {
            FlightInfo info = sqlClient.execute(
                    "SELECT rnd_symbol('red','green','blue') sy FROM long_sequence(5)");
            try (FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
                Assert.assertTrue(stream.next());
                VectorSchemaRoot root = stream.getRoot();
                VarCharVector sy = (VarCharVector) root.getVector("sy");
                Assert.assertEquals(5, root.getRowCount());
                for (int i = 0; i < 5; i++) {
                    Assert.assertFalse("row " + i + " must be valid", sy.isNull(i));
                    String v = new String(sy.get(i), StandardCharsets.UTF_8);
                    Assert.assertTrue("unexpected symbol: " + v,
                            "red".equals(v) || "green".equals(v) || "blue".equals(v));
                }
                Assert.assertFalse(stream.next());
            }
        }
    }

    @Test
    public void testSelectTimestampMicro() throws Exception {
        try (TestServerMain serverMain = startFlightSqlServer();
             FlightClient flightClient = openFlightClient(serverMain);
             FlightSqlClient sqlClient = new FlightSqlClient(flightClient)) {
            FlightInfo info = sqlClient.execute(
                    "SELECT cast('2024-01-01T12:00:00.000000Z' AS timestamp) t FROM long_sequence(3)");
            try (FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
                Assert.assertTrue(stream.next());
                VectorSchemaRoot root = stream.getRoot();
                TimeStampMicroVector t = (TimeStampMicroVector) root.getVector("t");
                Assert.assertEquals(3, root.getRowCount());
                long expected = 1_704_110_400_000_000L;
                for (int i = 0; i < 3; i++) {
                    Assert.assertFalse("row " + i + " must be valid", t.isNull(i));
                    Assert.assertEquals(expected, t.get(i));
                }
                Assert.assertFalse(stream.next());
            }
        }
    }

    @Test
    public void testSelectVarcharFromTable() throws Exception {
        try (TestServerMain serverMain = startFlightSqlServer()) {
            serverMain.compile("CREATE TABLE t (v VARCHAR)");
            serverMain.execute("INSERT INTO t VALUES ('abc'), (null), ('xyz')");
            try (FlightClient flightClient = openFlightClient(serverMain);
                 FlightSqlClient sqlClient = new FlightSqlClient(flightClient)) {
                FlightInfo info = sqlClient.execute("SELECT v FROM t");
                try (FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
                    Assert.assertTrue(stream.next());
                    VectorSchemaRoot root = stream.getRoot();
                    VarCharVector v = (VarCharVector) root.getVector("v");
                    Assert.assertEquals(3, root.getRowCount());
                    Assert.assertFalse(v.isNull(0));
                    Assert.assertEquals("abc", new String(v.get(0), StandardCharsets.UTF_8));
                    Assert.assertTrue("middle row must be null", v.isNull(1));
                    Assert.assertFalse(v.isNull(2));
                    Assert.assertEquals("xyz", new String(v.get(2), StandardCharsets.UTF_8));
                    Assert.assertFalse(stream.next());
                }
            }
        }
    }

    @Test
    public void testUnsupportedTypeRaisesUnimplemented() throws Exception {
        try (TestServerMain serverMain = startFlightSqlServer();
             FlightClient flightClient = openFlightClient(serverMain);
             FlightSqlClient sqlClient = new FlightSqlClient(flightClient)) {
            FlightRuntimeException ex = null;
            try {
                sqlClient.execute("SELECT rnd_uuid4() u FROM long_sequence(1)");
            } catch (FlightRuntimeException e) {
                ex = e;
            }
            Assert.assertNotNull("expected FlightRuntimeException for UUID column", ex);
            Assert.assertEquals(FlightStatusCode.UNIMPLEMENTED, ex.status().code());
        }
    }

    private static FlightClient openFlightClient(TestServerMain serverMain) {
        int port = serverMain.getConfiguration().getHttpServerConfiguration().getBindPort();
        Location location = Location.forGrpcInsecure("127.0.0.1", port);
        return FlightClient.builder(allocator, location).build();
    }

    private static TestServerMain startFlightSqlServer() {
        return startWithEnvVariables(
                PropertyKey.HTTP_H2_ENABLED.getEnvVarName(), "true",
                PropertyKey.FLIGHT_SQL_ENABLED.getEnvVarName(), "true"
        );
    }
}
