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

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * The Stream of data.
 *
 * @param <K> the type of the key element
 * @param <V> the type of the value element
 */
public interface Stream<K, V> {

    /**
     * Starts the map function for this map.
     * @param function the function
     *
     * @return the stream object for this map
     */
    Stream<K, V> map(UnaryOperator<java.util.Map.Entry<K, V>> function);

    /**
     * Starts the materialize function for this map.
     *
     * @return the stream object for this map
     */
    Stream<K, V> materialize();

    /**
     * Starts the foreach function for this map.
     * @param consumer the consumer
     *
     * @return the stream object for this map
     */
    Stream<K, V> foreach(Consumer<Map.Entry<K, V>> consumer);
}
