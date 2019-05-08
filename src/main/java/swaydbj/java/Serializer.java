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
package swaydbj.java;

import java.util.HashMap;
import swaydb.serializers.Default;

public class Serializer {

    private static final java.util.Map<Class<?>, swaydb.serializers.Serializer> CLASS_TO_TYPE = new HashMap<>();

    static {
        CLASS_TO_TYPE.put(Integer.class, Default.IntSerializer$.MODULE$);
        CLASS_TO_TYPE.put(String.class, Default.StringSerializer$.MODULE$);
        CLASS_TO_TYPE.put(Long.class, Default.LongSerializer$.MODULE$);
        CLASS_TO_TYPE.put(Character.class, Default.CharSerializer$.MODULE$);
        CLASS_TO_TYPE.put(Float.class, Default.FloatSerializer$.MODULE$);
        CLASS_TO_TYPE.put(Short.class, Default.ShortSerializer$.MODULE$);
        CLASS_TO_TYPE.put(byte[].class, Default.ArraySerializer$.MODULE$);
        CLASS_TO_TYPE.put(Double.class, Default.DoubleSerializer$.MODULE$);
    }

    public static swaydb.serializers.Serializer classToType(Object clazz) {
        if (clazz instanceof swaydb.serializers.Serializer) {
            return (swaydb.serializers.Serializer) clazz;
        }
        return CLASS_TO_TYPE.getOrDefault(clazz, Default.StringSerializer$.MODULE$);
    }

}
