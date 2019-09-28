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

import scala.Tuple2;
import scala.runtime.AbstractFunction1;
import swaydb.IO;

import java.util.AbstractMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import scala.collection.mutable.ListBuffer;

/**
 * The Stream of data.
 *
 * @param <K> the type of the key element
 * @param <V> the type of the value element
 */
public class Stream<K, V> {
    private final swaydb.Stream streamObject;
    private final IO success;

    /**
     * Constructs the Stream object.
     * @param streamObject the streamObject
     */
    public Stream(final swaydb.Stream streamObject) {
        this.streamObject = streamObject;
        this.success = null;
    }

    /**
     * Constructs the Stream object.
     * @param success the success
     */
    private Stream(final IO success) {
        this.streamObject = null;
        this.success = success;
    }

    /**
     * Starts the map function for this map.
     * @param function the function
     *
     * @return the stream object for this map
     */
    @SuppressWarnings("unchecked")
    public swaydb.java.Stream<K, V> map(UnaryOperator<Map.Entry<K, V>> function) {
        return new Stream<>(streamObject.map(new AbstractFunction1() {
            public Object apply(Object tuple2) {
                java.util.Map.Entry<K, V> result = function.apply(
                        new AbstractMap.SimpleEntry<>((K) ((Tuple2) tuple2)._1(), (V) ((Tuple2) tuple2)._2()));
                return IO.Right$.MODULE$.apply(Tuple2.apply(result.getKey(), result.getValue()), null);
            }
        }));
    }

    /**
     * Starts the materialize function for this map.
     *
     * @return the stream object for this map
     */
    public Stream<K, V> materialize() {
        return new Stream<>((IO.Right) streamObject.materialize());
    }

    /**
     * Starts the foreach function for this map.
     * @param consumer the consumer
     *
     * @return the stream object for this map
     */
    @SuppressWarnings("unchecked")
    public Stream<K, V> foreach(Consumer<Map.Entry<K, V>> consumer) {
        success.foreach(new AbstractFunction1() {
            public Object apply(Object t1) {
                if (!((ListBuffer) t1).seq().isEmpty() && ((ListBuffer) t1).seq().apply(0) instanceof IO.Right) {
                    scala.collection.Seq<IO.Right> entries = ((ListBuffer) t1).seq();
                    for (int index = 0; index < entries.size(); index += 1) {
                        consumer.accept(new AbstractMap.SimpleEntry<>(
                                (K) ((Tuple2) entries.apply(index).get())._1(),
                                (V) ((Tuple2) entries.apply(index).get())._2()));
                    }
                } else {
                    scala.collection.Seq<Tuple2> entries = ((ListBuffer) t1).seq();
                    for (int index = 0; index < entries.size(); index += 1) {
                        consumer.accept(new AbstractMap.SimpleEntry<>(
                                (K) entries.apply(index)._1(), (V) entries.apply(index)._2()));
                    }
                }
                return IO.Right$.MODULE$.apply(((ListBuffer) t1), null);
            }
        });
        return this;
    }

}
