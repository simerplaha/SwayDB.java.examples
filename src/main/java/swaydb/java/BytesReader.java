/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */
package swaydb.java;

import java.nio.charset.StandardCharsets;
import swaydb.data.slice.Slice;
import swaydb.data.slice.Slice$;

/**
 * The BytesReader wrapper.
 */
public class BytesReader {

    private final swaydb.data.slice.BytesReader reader;

    private BytesReader(Slice<Object> slice) {
        reader = Slice$.MODULE$.ByteSliceImplicits(slice).createReader();
    }

    /**
     * Creates the BytesReader object.
     * @param slice the slice
     *
     * @return the BytesReader object
     */
    public static BytesReader create(Slice<Object> slice) {
        return new BytesReader(slice);
    }

    /**
     * Reads the string data.
     * @param size the size
     *
     * @return the string data
     */
    public String readString(int size) {
        return reader.readString(size, StandardCharsets.UTF_8);
    }

    /**
     * Reads the int data.
     *
     * @return the int data
     */
    public int readInt() {
        return reader.readInt();
    }

    /**
     * Reads the long data.
     *
     * @return the long data
     */
    public long readLong() {
        return reader.readLong();
    }

    /**
     * Reads the byte data.
     *
     * @return the byte data
     */
    public byte readByte() {
        return (byte) reader.read(1).apply(0);
    }

    /**
     * Reads the boolean data.
     *
     * @return the boolean data
     */
    public boolean readBoolean() {
        return (byte) reader.read(1).apply(0) == 1;
    }
}
