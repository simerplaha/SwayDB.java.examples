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
import swaydb.data.slice.Slice$;

/**
 * The RecoveryMode wrapper.
 */
public class Slice {

    private swaydb.data.slice.Slice<Object> slice;

    private Slice(int size) {
        slice = Slice$.MODULE$.create(size, scala.reflect.ClassTag$.MODULE$.Any());
    }

    /**
     * Creates the Slice object.
     * @param size the size
     *
     * @return the Slice object
     */
    public static Slice create(int size) {
        return new Slice(size);
    }

    /**
     * Adds string to the Slice object.
     * @param string the string
     *
     * @return the Slice object
     */
    public Slice addString(String string) {
        slice = Slice$.MODULE$.ByteSliceImplicits(slice)
                .addString(string, StandardCharsets.UTF_8);
        return this;
    }

    /**
     * Adds int to the Slice object.
     * @param value the value
     *
     * @return the Slice object
     */
    public Slice addInt(int value) {
        slice = Slice$.MODULE$.ByteSliceImplicits(slice).addInt(value);
        return this;
    }

    /**
     * Adds long to the Slice object.
     * @param value the value
     *
     * @return the Slice object
     */
    public Slice addLong(long value) {
        slice = Slice$.MODULE$.ByteSliceImplicits(slice).addLong(value);
        return this;
    }

    /**
     * Adds byte to the Slice object.
     * @param value the value
     *
     * @return the Slice object
     */
    public Slice addByte(byte value) {
        slice = Slice$.MODULE$.ByteSliceImplicits(slice).addByte(value);
        return this;
    }

    /**
     * Adds boolean to the Slice object.
     * @param value the value
     *
     * @return the Slice object
     */
    public Slice addBoolean(boolean value) {
        slice = Slice$.MODULE$.ByteSliceImplicits(slice).addBoolean(value);
        return this;
    }

    /**
     * Closes the Slice object.
     *
     * @return the Slice object
     */
    public swaydb.data.slice.Slice<Object> close() {
        return slice.close();
    }
}
