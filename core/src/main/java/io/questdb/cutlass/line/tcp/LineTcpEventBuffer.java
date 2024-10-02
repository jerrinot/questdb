/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.*;
import io.questdb.std.str.*;

import static io.questdb.cutlass.line.tcp.LineTcpParser.ENTITY_TYPE_NULL;

public class LineTcpEventBuffer {
    private final long bufLo;
    private final long bufSize;
    private final FlyweightDirectUtf16Sink tempSink = new FlyweightDirectUtf16Sink();
    private final FlyweightDirectUtf16Sink tempSinkB = new FlyweightDirectUtf16Sink();
    private final DirectUtf8String utf8Sequence = new DirectUtf8String();

    public LineTcpEventBuffer(long bufLo, long bufSize) {
        this.bufLo = bufLo;
        this.bufSize = bufLo + bufSize;
    }

    public long addBoolean(long address, byte value) {
        checkCapacity(address, Byte.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_BOOLEAN);
        Unsafe.getUnsafe().putByte(address + Byte.BYTES, value);
        return address + Byte.BYTES + Byte.BYTES;
    }

    public long addByte(long address, byte value) {
        checkCapacity(address, Byte.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_BYTE);
        Unsafe.getUnsafe().putByte(address + Byte.BYTES, value);
        return address + Byte.BYTES + Byte.BYTES;
    }

    public long addChar(long address, char value) {
        checkCapacity(address, Character.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_CHAR);
        Unsafe.putChar(address + Byte.BYTES, value);
        return address + Character.BYTES + Byte.BYTES;
    }

    public long addColumnIndex(long address, int colIndex) {
        checkCapacity(address, Integer.BYTES);
        Unsafe.putInt(address, colIndex);
        return address + Integer.BYTES;
    }

    public long addColumnName(long address, CharSequence colName, CharSequence principal) {
        int colNameLen = colName.length();
        int principalLen = principal != null ? principal.length() : 0;
        int colNameCapacity = Integer.BYTES + 2 * colNameLen;
        int fullCapacity = colNameCapacity + Integer.BYTES + 2 * principalLen;
        checkCapacity(address, fullCapacity);

        // Negative length indicates to the writer thread that column is passed by
        // name rather than by index. When value is positive (on the else branch)
        // the value is treated as column index.
        Unsafe.putInt(address, -colNameLen);
        Chars.copyStrChars(colName, 0, colNameLen, address + Integer.BYTES);

        // Now write principal name, so that we can call DdlListener#onColumnAdded()
        // when adding the column.
        Unsafe.putInt(address + colNameCapacity, principalLen);
        if (principalLen > 0) {
            Chars.copyStrChars(principal, 0, principalLen, address + colNameCapacity + Integer.BYTES);
        }

        return address + fullCapacity;
    }

    public long addDate(long address, long value) {
        checkCapacity(address, Long.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_DATE);
        Unsafe.putLong(address + Byte.BYTES, value);
        return address + Long.BYTES + Byte.BYTES;
    }

    public void addDesignatedTimestamp(long address, long timestamp) {
        checkCapacity(address, Long.BYTES + Byte.BYTES);
        Unsafe.putLong(address, timestamp);
    }

    public long addDouble(long address, double value) {
        checkCapacity(address, Double.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_DOUBLE);
        Unsafe.putDouble(address + Byte.BYTES, value);
        return address + Double.BYTES + Byte.BYTES;
    }

    public long addFloat(long address, float value) {
        checkCapacity(address, Float.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_FLOAT);
        Unsafe.putFloat(address + Byte.BYTES, value);
        return address + Float.BYTES + Byte.BYTES;
    }

    public long addGeoHash(long address, DirectUtf8Sequence value, int colTypeMeta) {
        long geohash;
        try {
            geohash = GeoHashes.fromAsciiTruncatingNl(value.lo(), value.hi(), Numbers.decodeLowShort(colTypeMeta));
        } catch (NumericException e) {
            geohash = GeoHashes.NULL;
        }
        switch (Numbers.decodeHighShort(colTypeMeta)) {
            default:
                checkCapacity(address, Long.BYTES + Byte.BYTES);
                Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_GEOLONG);
                Unsafe.putLong(address + Byte.BYTES, geohash);
                return address + Long.BYTES + Byte.BYTES;
            case ColumnType.GEOINT:
                checkCapacity(address, Integer.BYTES + Byte.BYTES);
                Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_GEOINT);
                Unsafe.putInt(address + Byte.BYTES, (int) geohash);
                return address + Integer.BYTES + Byte.BYTES;
            case ColumnType.GEOSHORT:
                checkCapacity(address, Short.BYTES + Byte.BYTES);
                Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_GEOSHORT);
                Unsafe.putShort(address + Byte.BYTES, (short) geohash);
                return address + Short.BYTES + Byte.BYTES;
            case ColumnType.GEOBYTE:
                checkCapacity(address, Byte.BYTES + Byte.BYTES);
                Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_GEOBYTE);
                Unsafe.getUnsafe().putByte(address + Byte.BYTES, (byte) geohash);
                return address + Byte.BYTES + Byte.BYTES;
        }
    }

    public long addInt(long address, int value) {
        checkCapacity(address, Integer.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_INTEGER);
        Unsafe.putInt(address + Byte.BYTES, value);
        return address + Integer.BYTES + Byte.BYTES;
    }

    public long addLong(long address, long value) {
        checkCapacity(address, Long.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_LONG);
        Unsafe.putLong(address + Byte.BYTES, value);
        return address + Long.BYTES + Byte.BYTES;
    }

    public long addLong256(long address, DirectUtf8Sequence value) {
        return addString(address, value, LineTcpParser.ENTITY_TYPE_LONG256);
    }

    public long addNull(long address) {
        checkCapacity(address, Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_NULL);
        return address + Byte.BYTES;
    }

    public void addNumOfColumns(long address, int numOfColumns) {
        checkCapacity(address, Integer.BYTES);
        Unsafe.putInt(address, numOfColumns);
    }

    public long addShort(long address, short value) {
        checkCapacity(address, Short.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_SHORT);
        Unsafe.putShort(address + Byte.BYTES, value);
        return address + Short.BYTES + Byte.BYTES;
    }

    public long addString(long address, DirectUtf8Sequence value) {
        return addString(address, value, LineTcpParser.ENTITY_TYPE_STRING);
    }

    public void addStructureVersion(long address, long structureVersion) {
        checkCapacity(address, Long.BYTES);
        Unsafe.putLong(address, structureVersion);
    }

    public long addSymbol(long address, DirectUtf8Sequence value, DirectUtf8SymbolLookup symbolLookup) {
        final int maxLen = 2 * value.size();
        checkCapacity(address, Byte.BYTES + Integer.BYTES + maxLen);
        final long strPos = address + Byte.BYTES + Integer.BYTES; // skip field type and string length

        // via temp string the utf8 decoder will be writing directly to our buffer
        tempSink.of(strPos, strPos + maxLen);

        final int symIndex = symbolLookup.keyOf(value);
        if (symIndex != SymbolTable.VALUE_NOT_FOUND) {
            // We know the symbol int value
            // Encode the int
            Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_CACHED_TAG);
            Unsafe.putInt(address + Byte.BYTES, symIndex);
            return address + Integer.BYTES + Byte.BYTES;
        } else {
            // Symbol value cannot be resolved at this point
            // Encode whole string value into the message
            if (value.isAscii()) {
                tempSink.put(value);
            } else {
                Utf8s.utf8ToUtf16(value, tempSink);
            }
            final int length = tempSink.length();
            Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_TAG);
            Unsafe.putInt(address + Byte.BYTES, length);
            return address + length * 2L + Integer.BYTES + Byte.BYTES;
        }
    }

    public long addTimestamp(long address, long value) {
        checkCapacity(address, Long.BYTES + Byte.BYTES);
        Unsafe.getUnsafe().putByte(address, LineTcpParser.ENTITY_TYPE_TIMESTAMP);
        Unsafe.putLong(address + Byte.BYTES, value);
        return address + Long.BYTES + Byte.BYTES;
    }

    /**
     * Add UUID encoded as a string to the buffer.
     * <p>
     * Technically, DirectByteCharSequence is UTF-8 encoded, but any non-ASCII character will cause the UUID
     * to be rejected by the parser. Hence, we do not have to bother with UTF-8 decoding.
     *
     * @param offset offset in the buffer to write to
     * @param value  value to write
     * @return new offset
     * @throws NumericException if the value is not a valid UUID string
     */
    public long addUuid(long offset, DirectUtf8Sequence value) throws NumericException {
        checkCapacity(offset, Byte.BYTES + 2 * Long.BYTES);
        final CharSequence csView = value.asAsciiCharSequence();
        Uuid.checkDashesAndLength(csView);
        long hi = Uuid.parseHi(csView);
        long lo = Uuid.parseLo(csView);
        Unsafe.getUnsafe().putByte(offset, LineTcpParser.ENTITY_TYPE_UUID);
        offset += Byte.BYTES;
        Unsafe.putLong(offset, lo);
        offset += Long.BYTES;
        Unsafe.putLong(offset, hi);
        return offset + Long.BYTES;
    }

    public long addVarchar(long address, DirectUtf8Sequence value) {
        final int valueSize = value.size();
        final int totalSize = Byte.BYTES + Byte.BYTES + Integer.BYTES + valueSize;
        checkCapacity(address, totalSize);
        Unsafe.getUnsafe().putByte(address++, LineTcpParser.ENTITY_TYPE_VARCHAR);
        Unsafe.getUnsafe().putByte(address++, (byte) (value.isAscii() ? 0 : 1));
        Unsafe.putInt(address, valueSize);
        address += Integer.BYTES;
        value.writeTo(address, 0, valueSize);
        return address + totalSize;
    }

    public long columnValueLength(byte entityType, long offset) {
        CharSequence cs;
        switch (entityType) {
            case LineTcpParser.ENTITY_TYPE_TAG:
            case LineTcpParser.ENTITY_TYPE_STRING:
            case LineTcpParser.ENTITY_TYPE_LONG256:
                cs = readUtf16Chars(offset);
                return cs.length() * 2L + Integer.BYTES;
            case LineTcpParser.ENTITY_TYPE_BYTE:
            case LineTcpParser.ENTITY_TYPE_GEOBYTE:
            case LineTcpParser.ENTITY_TYPE_BOOLEAN:
                return Byte.BYTES;
            case LineTcpParser.ENTITY_TYPE_SHORT:
            case LineTcpParser.ENTITY_TYPE_GEOSHORT:
                return Short.BYTES;
            case LineTcpParser.ENTITY_TYPE_CHAR:
                return Character.BYTES;
            case LineTcpParser.ENTITY_TYPE_CACHED_TAG:
            case LineTcpParser.ENTITY_TYPE_INTEGER:
            case LineTcpParser.ENTITY_TYPE_GEOINT:
                return Integer.BYTES;
            case LineTcpParser.ENTITY_TYPE_LONG:
            case LineTcpParser.ENTITY_TYPE_GEOLONG:
            case LineTcpParser.ENTITY_TYPE_DATE:
            case LineTcpParser.ENTITY_TYPE_TIMESTAMP:
                return Long.BYTES;
            case LineTcpParser.ENTITY_TYPE_FLOAT:
                return Float.BYTES;
            case LineTcpParser.ENTITY_TYPE_DOUBLE:
                return Double.BYTES;
            case LineTcpParser.ENTITY_TYPE_UUID:
                return Long128.BYTES;
            case ENTITY_TYPE_NULL:
                return 0;
            default:
                throw new UnsupportedOperationException("entityType " + entityType + " is not implemented!");
        }
    }

    public long getAddress() {
        return bufLo;
    }

    public long getAddressAfterHeader() {
        // The header contains structure version (long), timestamp (long) and number of columns (int).
        return bufLo + 2 * Long.BYTES + Integer.BYTES;
    }

    public byte readByte(long address) {
        return Unsafe.getUnsafe().getByte(address);
    }

    public char readChar(long address) {
        return Unsafe.getUnsafe().getChar(address);
    }

    public double readDouble(long address) {
        return Unsafe.getUnsafe().getDouble(address);
    }

    public float readFloat(long address) {
        return Unsafe.getUnsafe().getFloat(address);
    }

    public int readInt(long address) {
        return Unsafe.getUnsafe().getInt(address);
    }

    public long readLong(long address) {
        return Unsafe.getUnsafe().getLong(address);
    }

    public short readShort(long address) {
        return Unsafe.getUnsafe().getShort(address);
    }

    public CharSequence readUtf16Chars(long address) {
        int len = readInt(address);
        return readUtf16Chars(address + Integer.BYTES, len);
    }

    public CharSequence readUtf16Chars(long address, int length) {
        return tempSink.asCharSequence(address, address + length * 2L);
    }

    public CharSequence readUtf16CharsB(long address, int length) {
        return tempSinkB.asCharSequence(address, address + length * 2L);
    }

    public Utf8Sequence readVarchar(long address, boolean ascii) {
        int size = readInt(address);
        return utf8Sequence.of(address + Integer.BYTES, address + Integer.BYTES + size, ascii);
    }

    private long addString(long address, DirectUtf8Sequence value, byte entityTypeString) {
        int maxLen = 2 * value.size();
        checkCapacity(address, Byte.BYTES + Integer.BYTES + maxLen);
        long strPos = address + Byte.BYTES + Integer.BYTES; // skip field type and string length
        tempSink.of(strPos, strPos + maxLen);
        if (value.isAscii()) {
            tempSink.put(value);
        } else {
            Utf8s.utf8ToUtf16Unchecked(value, tempSink);
        }
        final int length = tempSink.length();
        Unsafe.getUnsafe().putByte(address, entityTypeString);
        Unsafe.putInt(address + Byte.BYTES, length);
        return address + length * 2L + Integer.BYTES + Byte.BYTES;
    }

    private void checkCapacity(long address, int length) {
        if (address + length > bufSize) {
            throw CairoException.critical(0).put("queue buffer overflow");
        }
    }
}
