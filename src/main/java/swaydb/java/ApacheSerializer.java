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

import java.io.Serializable;
import org.apache.commons.lang3.SerializationUtils;
import swaydb.data.slice.Slice;
import swaydb.data.slice.Slice$;

/**
 * The ApacheSerializer.
 *
 * @param <T> the type of the data element
 */
public class ApacheSerializer<T extends Serializable> implements swaydb.serializers.Serializer<T> {

    /**
     * Writes a typed object for this slice.
     * @param myData the typed object
     *
     * @return the slice object
     */
    @Override
    public Slice<Object> write(T myData) {
        byte[] data = SerializationUtils.serialize(myData);
        return Slice$.MODULE$.ByteSliceImplicits(Slice$.MODULE$.create(data.length, true,
                scala.reflect.ClassTag$.MODULE$.Any()))
                .addBytes(Slice$.MODULE$.apply(data, scala.reflect.ClassTag$.MODULE$.Any()));
    }

    /**
     * Reads a typed object from this slice.
     * @param data the slice object
     *
     * @return the typed data object
     */
    @Override
    public T read(Slice<Object> data) {
        Slice<Object> byteSlice = Slice$.MODULE$.ByteSliceImplicits(data).createReaderUnsafe()
                .readRemaining();
        byte[] result = new byte[byteSlice.size()];
        for (int index = 0; index < byteSlice.size(); index += 1) {
            result[index] = (byte) byteSlice.apply(index);
        }
        return SerializationUtils.deserialize(result);
    }
}
