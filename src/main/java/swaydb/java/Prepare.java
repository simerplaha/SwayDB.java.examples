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

/**
 * Wrapper Java class to create SwayDB's scala Prepare classes.
 *
 * @param <K> the type of the key element
 * @param <V> the type of the value element
 */
public class Prepare<K, V> {

    /**
     * Returns the Prepare object for put.
     * @param key the key
     * @param value the value
     *
     * @return the Prepare object for put
     */
    public swaydb.Prepare.Put<K, V> put(K key, V value) {
        return new swaydb.Prepare.Put<>(key, value, scala.Option.empty());
    }

    /**
     * Returns the Prepare object for remove.
     * @param key the key
     *
     * @return the Prepare object for remove
     */
    public swaydb.Prepare.Remove<K> remove(K key) {
        return new swaydb.Prepare.Remove<>(key, scala.Option.empty(), scala.Option.empty());
    }
}
