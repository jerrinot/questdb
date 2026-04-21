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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Unsafe;

/**
 * Emits an Arrow IPC {@code Message} wrapping a {@code Schema} with one
 * or more scalar fields. Wave 6b supports LONG, DOUBLE, and INT only;
 * any other type throws {@link UnsupportedColumnTypeException} which the
 * Flight SQL handler translates into {@code grpc-status: UNIMPLEMENTED}.
 * <p>
 * All fields are emitted as {@code nullable=false}; Wave 6b does not
 * emit validity bitmaps. See {@code FLIGHT_SQL_DESIGN.md} §5.10 for the
 * full type-mapping table and the deferred null-handling plan.
 * <p>
 * Field indices (from Arrow's {@code Schema.fbs} /
 * {@code Message.fbs}) used here:
 * <ul>
 *   <li>{@code Int}: {@code bitWidth: int32 = 0}, {@code is_signed: bool = 1}.</li>
 *   <li>{@code FloatingPoint}: {@code precision: int16 = 0}.</li>
 *   <li>{@code Field}: {@code name: string = 0}, {@code nullable: bool = 1},
 *       {@code type_type: uint8 = 2}, {@code type: uoffset = 3}, (rest skipped).</li>
 *   <li>{@code Schema}: {@code endianness: int16 = 0}, {@code fields: [Field] = 1},
 *       (rest skipped).</li>
 *   <li>{@code Message}: {@code version: int16 = 0}, {@code header_type: uint8 = 1},
 *       {@code header: uoffset = 2}, {@code bodyLength: int64 = 3}, (rest skipped).</li>
 * </ul>
 * Enum values:
 * <ul>
 *   <li>{@code MetadataVersion.V5 = 4}.</li>
 *   <li>{@code MessageHeader.Schema = 1} (union discriminator).</li>
 *   <li>{@code Type.Int = 2}, {@code Type.FloatingPoint = 3} (union discriminators).</li>
 *   <li>{@code Precision.DOUBLE = 2}.</li>
 *   <li>{@code Endianness.Little = 0}.</li>
 * </ul>
 */
public final class ArrowSchemaWriter {

    private static final int ENDIANNESS_LITTLE = 0;
    private static final int MAX_INLINE_FIELDS = 64;
    private static final short METADATA_VERSION_V5 = 4;
    private static final int MESSAGE_HEADER_SCHEMA = 1;
    private static final short PRECISION_DOUBLE = 2;
    private static final int TYPE_FLOATING_POINT = 3;
    private static final int TYPE_INT = 2;

    private ArrowSchemaWriter() {
    }

    /**
     * Emits an Arrow IPC {@code Message} carrying a {@code Schema}
     * derived from {@code metadata}. Column types other than LONG /
     * DOUBLE / INT trigger {@link UnsupportedColumnTypeException}.
     * Field names come from {@link RecordMetadata#getColumnName(int)}
     * and are transcribed byte-by-byte via {@code nameScratch}; names
     * longer than {@code nameScratchCap} return {@code -1} (overflow).
     * <p>
     * Returns the byte length of the emitted message, or {@code -1} if
     * the FlatBuffer scratch in {@code writer} overflowed.
     */
    public static int writeSchemaMessage(FbWriter writer, long nameScratchAddr, int nameScratchCap,
                                         RecordMetadata metadata) {
        if (metadata == null) {
            throw new IllegalArgumentException("metadata must be non-null");
        }
        int columnCount = metadata.getColumnCount();
        if (columnCount <= 0) {
            throw new IllegalArgumentException("columnCount must be positive: " + columnCount);
        }
        if (columnCount > MAX_INLINE_FIELDS) {
            throw new IllegalArgumentException("too many columns: " + columnCount);
        }
        int[] fieldTableOffsets = new int[columnCount];
        try {
            for (int i = 0; i < columnCount; i++) {
                int columnType = metadata.getColumnType(i);
                String columnName = metadata.getColumnName(i);
                int typeTable = writeTypeTable(writer, columnType);
                int nameOffset = writeFieldName(writer, nameScratchAddr, nameScratchCap, columnName);
                if (nameOffset < 0) {
                    return -1;
                }
                int typeDiscriminator = typeDiscriminatorOf(columnType);
                fieldTableOffsets[i] = writeFieldTable(writer, nameOffset, typeTable, typeDiscriminator);
            }

            // Fields vector: element offsets must land in ascending order
            // in memory, so prepend the last offset first.
            writer.startVector(4, columnCount, 4);
            for (int i = columnCount - 1; i >= 0; i--) {
                writer.prependUoffset(fieldTableOffsets[i]);
            }
            int fieldsVector = writer.endVector(columnCount);

            // Schema table: endianness(0), fields(1).
            writer.startTable(2);
            writer.prependUoffset(fieldsVector);
            writer.slot(1, writer.cursorFromEnd());
            writer.prependInt16((short) ENDIANNESS_LITTLE);
            writer.slot(0, writer.cursorFromEnd());
            int schemaTable = writer.endTable();

            // Message table wrapping the Schema.
            writer.startTable(4);
            writer.prependInt64(0L);
            writer.slot(3, writer.cursorFromEnd());
            writer.prependUoffset(schemaTable);
            writer.slot(2, writer.cursorFromEnd());
            writer.prependUint8(MESSAGE_HEADER_SCHEMA);
            writer.slot(1, writer.cursorFromEnd());
            writer.prependInt16(METADATA_VERSION_V5);
            writer.slot(0, writer.cursorFromEnd());
            int messageTable = writer.endTable();

            writer.finish(messageTable);
            return writer.finishedLen();
        } catch (IllegalStateException overflow) {
            // FbWriter.ensureInBounds throws this on buffer overflow;
            // surface as -1 to the caller.
            return -1;
        }
    }

    private static int typeDiscriminatorOf(int columnType) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.LONG:
            case ColumnType.INT:
                return TYPE_INT;
            case ColumnType.DOUBLE:
                return TYPE_FLOATING_POINT;
            default:
                throw new UnsupportedColumnTypeException(columnType);
        }
    }

    private static int writeFieldName(FbWriter writer, long nameScratchAddr, int nameScratchCap, String columnName) {
        if (columnName == null) {
            throw new IllegalArgumentException("column name must be non-null");
        }
        int nameLen = columnName.length();
        if (nameLen > nameScratchCap) {
            return -1;
        }
        for (int i = 0; i < nameLen; i++) {
            char c = columnName.charAt(i);
            if (c > 0x7F) {
                throw new IllegalArgumentException("field name must be ASCII: " + columnName);
            }
            Unsafe.getUnsafe().putByte(nameScratchAddr + i, (byte) c);
        }
        return writer.writeString(nameScratchAddr, nameLen);
    }

    private static int writeFieldTable(FbWriter writer, int nameOffset, int typeTable, int typeDiscriminator) {
        writer.startTable(4);
        writer.prependUoffset(typeTable);
        writer.slot(3, writer.cursorFromEnd());
        writer.prependUint8(typeDiscriminator);
        writer.slot(2, writer.cursorFromEnd());
        writer.prependInt8((byte) 0); // nullable = false
        writer.slot(1, writer.cursorFromEnd());
        writer.prependUoffset(nameOffset);
        writer.slot(0, writer.cursorFromEnd());
        return writer.endTable();
    }

    private static int writeFloatingPointTable(FbWriter writer) {
        writer.startTable(1);
        writer.prependInt16(PRECISION_DOUBLE);
        writer.slot(0, writer.cursorFromEnd());
        return writer.endTable();
    }

    private static int writeIntTable(FbWriter writer, int bitWidth, boolean isSigned) {
        writer.startTable(2);
        writer.prependInt8(isSigned ? (byte) 1 : (byte) 0);
        writer.slot(1, writer.cursorFromEnd());
        writer.prependInt32(bitWidth);
        writer.slot(0, writer.cursorFromEnd());
        return writer.endTable();
    }

    private static int writeTypeTable(FbWriter writer, int columnType) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.LONG:
                return writeIntTable(writer, 64, true);
            case ColumnType.INT:
                return writeIntTable(writer, 32, true);
            case ColumnType.DOUBLE:
                return writeFloatingPointTable(writer);
            default:
                throw new UnsupportedColumnTypeException(columnType);
        }
    }
}
