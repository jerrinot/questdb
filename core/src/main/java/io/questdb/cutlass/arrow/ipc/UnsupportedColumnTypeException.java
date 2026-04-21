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

package io.questdb.cutlass.arrow.ipc;

/**
 * Thrown when the Arrow IPC writers encounter a QuestDB column type that
 * Wave 6b does not yet map. Wave 6b supports LONG, DOUBLE, and INT only;
 * everything else surfaces as this exception so the Flight SQL handler
 * can translate it into {@code grpc-status: UNIMPLEMENTED}.
 * <p>
 * The exception carries the offending QuestDB column type code (see
 * {@link io.questdb.cairo.ColumnType}) so the handler can include it in
 * the {@code grpc-message} field for client diagnostics.
 */
public final class UnsupportedColumnTypeException extends RuntimeException {

    private final int columnType;

    public UnsupportedColumnTypeException(int columnType) {
        super("unsupported Arrow column type: " + columnType);
        this.columnType = columnType;
    }

    public int getColumnType() {
        return columnType;
    }
}
