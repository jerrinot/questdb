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

import io.questdb.std.Unsafe;

/**
 * Emits an Arrow IPC {@code Message} wrapping a {@code Schema} with a
 * single {@code Int} field. Wave 6a hard-codes the layout; later waves
 * generalise to arbitrary type unions via a column-emitter registry.
 * <p>
 * Field indices (from Arrow's {@code Schema.fbs} /
 * {@code Message.fbs}) are:
 * <ul>
 *   <li>{@code Int}: {@code bitWidth: int32 = 0}, {@code is_signed: bool = 1}.</li>
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
 *   <li>{@code Type.Int = 2} (union discriminator).</li>
 *   <li>{@code Endianness.Little = 0}.</li>
 * </ul>
 */
public final class ArrowSchemaWriter {

    private static final int ENDIANNESS_LITTLE = 0;
    private static final short METADATA_VERSION_V5 = 4;
    private static final int MESSAGE_HEADER_SCHEMA = 1;
    private static final int TYPE_INT = 2;

    private ArrowSchemaWriter() {
    }

    /**
     * Emits an Arrow IPC {@code Message} carrying a {@code Schema} with
     * a single Int64 non-null field at {@code [addr, limit)}. Returns
     * {@code -1} on buffer overflow, otherwise the byte length of the
     * emitted message. The message bytes occupy
     * {@code [addr + (limit - addr - returnedLen), limit)}; use
     * {@link FbWriter} API when a caller wants both the address and the
     * length in a zero-copy handoff.
     */
    public static int writeInt64SchemaMessage(FbWriter writer, long fieldNameAddr, int fieldNameLen,
                                              int bitWidth, boolean isSigned) {
        // 1) Int table
        writer.startTable(2);
        writer.prependInt8(isSigned ? (byte) 1 : (byte) 0);
        int isSignedOffset = writer.cursorFromEnd();
        writer.slot(1, isSignedOffset);
        writer.prependInt32(bitWidth);
        int bitWidthOffset = writer.cursorFromEnd();
        writer.slot(0, bitWidthOffset);
        int intTable = writer.endTable();

        // 2) Field name string
        int nameOffset = writer.writeString(fieldNameAddr, fieldNameLen);

        // 3) Field table
        // Field.children is an optional vector; Arrow Java's decoder is
        // fine with it being absent. Same for dictionary and
        // custom_metadata: absent fields fall back to defaults.
        writer.startTable(4);
        // Field has 7 fields in the schema: name(0), nullable(1),
        // type_type(2), type(3), dictionary(4), children(5),
        // custom_metadata(6). We only populate 0..3.
        writer.prependUoffset(intTable);
        writer.slot(3, writer.cursorFromEnd());
        writer.prependUint8(TYPE_INT);
        writer.slot(2, writer.cursorFromEnd());
        writer.prependInt8((byte) 0); // nullable = false
        writer.slot(1, writer.cursorFromEnd());
        writer.prependUoffset(nameOffset);
        writer.slot(0, writer.cursorFromEnd());
        int fieldTable = writer.endTable();

        // 4) Fields vector (one entry, the Field table)
        writer.startVector(4, 1, 4);
        writer.prependUoffset(fieldTable);
        int fieldsVector = writer.endVector(1);

        // 5) Schema table
        // Schema fields: endianness(0), fields(1), custom_metadata(2),
        // features(3). We populate 0 and 1 only.
        writer.startTable(2);
        writer.prependUoffset(fieldsVector);
        writer.slot(1, writer.cursorFromEnd());
        writer.prependInt16((short) ENDIANNESS_LITTLE);
        writer.slot(0, writer.cursorFromEnd());
        int schemaTable = writer.endTable();

        // 6) Message table wrapping the Schema
        // Message fields: version(0), header_type(1), header(2),
        // bodyLength(3), custom_metadata(4).
        writer.startTable(4);
        writer.prependInt64(0L); // bodyLength = 0 (Schema message has no body)
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
    }

    /**
     * Convenience wrapper that materialises the field name bytes as an
     * ASCII string. Allocates only within the caller-provided scratch
     * buffer.
     */
    public static int writeInt64SchemaMessage(FbWriter writer, long nameBufferAddr, int nameBufferCap,
                                              String fieldName, int bitWidth, boolean isSigned) {
        if (fieldName == null) {
            throw new IllegalArgumentException("fieldName must be non-null");
        }
        int nameLen = fieldName.length();
        if (nameLen > nameBufferCap) {
            return -1;
        }
        for (int i = 0; i < nameLen; i++) {
            char c = fieldName.charAt(i);
            if (c > 0x7F) {
                throw new IllegalArgumentException("field name must be ASCII: " + fieldName);
            }
            Unsafe.getUnsafe().putByte(nameBufferAddr + i, (byte) c);
        }
        return writeInt64SchemaMessage(writer, nameBufferAddr, nameLen, bitWidth, isSigned);
    }
}
