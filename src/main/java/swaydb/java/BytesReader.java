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

public class BytesReader {

    private final swaydb.data.slice.BytesReader reader;

    private BytesReader(Slice<Object> slice) {
        reader = Slice$.MODULE$.ByteSliceImplicits(slice).createReader();
    }

    public static BytesReader create(Slice<Object> slice) {
        return new BytesReader(slice);
    }

    public String readString(int size) {
        return reader.readString(size, StandardCharsets.UTF_8);
    }

    public int readInt() {
        return reader.readInt();
    }

    public long readLong() {
        return reader.readLong();
    }

    public byte readByte() {
        return (byte) reader.read(1).apply(0);
    }

    public boolean readBoolean() {
        return (byte) reader.read(1).apply(0) == 1;
    }
}
