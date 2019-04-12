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

public class Slice {

    private swaydb.data.slice.Slice<Object> slice;

    public Slice(int size) {
        slice = Slice$.MODULE$.create(size, scala.reflect.ClassTag$.MODULE$.Any());
    }

    public static Slice create(int size) {
        return new Slice(size);
    }

    public Slice addString(String string) {
        slice = Slice$.MODULE$.ByteSliceImplicits(slice)
                .addString(string, StandardCharsets.UTF_8);
        return this;
    }

    public Slice addInt(int value) {
        slice = Slice$.MODULE$.ByteSliceImplicits(slice).addInt(value);
        return this;
    }

    public swaydb.data.slice.Slice<Object> close() {
        return slice.close();
    }
}
