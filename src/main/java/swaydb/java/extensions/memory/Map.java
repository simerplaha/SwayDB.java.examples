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
package swaydb.java.extensions.memory;

import java.io.Closeable;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import scala.Function1;
import scala.Option;
import scala.collection.Iterable;
import scala.collection.JavaConverters;
import scala.concurrent.ExecutionContext;
import scala.concurrent.duration.FiniteDuration;
import swaydb.Prepare;
import swaydb.data.accelerate.Accelerator;
import swaydb.data.accelerate.LevelZeroMeter;
import swaydb.data.api.grouping.GroupBy.KeyValues;
import swaydb.extensions.Maps;
import swaydb.extensions.memory.Map$;
import swaydb.java.Serializer;

/**
 * The memory Map of data.
 *
 * @param <K> the type of the key element
 * @param <V> the type of the value element
 */
public class Map<K, V> implements Closeable {

    private final swaydb.extensions.Map<K, V> database;

    private Map(swaydb.extensions.Map<K, V> database) {
        this.database = database;
    }

    /**
     * Returns the size of elements in this map.
     *
     * @return the size of elements in this map
     */
    public int size() {
        return database.baseMap().asScala().size();
    }

    /**
     * Checks the map is empty.
     *
     * @return {@code true} if a map is empty, {@code false} otherwise
     */
    public boolean isEmpty() {
        return (boolean) database.baseMap().isEmpty().get();
    }

    /**
     * Checks the map is not empty.
     *
     * @return {@code true} if a map is not empty, {@code false} otherwise
     */
    public boolean nonEmpty() {
        return (boolean) database.baseMap().nonEmpty().get();
    }

    /**
     * Checks if a map contains key.
     * @param key the key
     *
     * @return {@code true} if a map contains key, {@code false} otherwise
     */
    public boolean containsKey(K key) {
        return (boolean) database.contains(key).get();
    }

    /**
     * Checks if a map might contain key.
     * @param key the key
     *
     * @return {@code true} if a map might contains key, {@code false} otherwise
     */
    @SuppressWarnings("unchecked")
    public boolean mightContain(K key) {
        return (boolean) database.mightContain(key).get();
    }

    /**
     * Returns the head key for this map.
     *
     * @return the head key for this map
     */
    @SuppressWarnings("unchecked")
    public java.util.Map.Entry<K, V> head() {
        Object result = database.headOption().get();
        if (result instanceof scala.Some) {
            scala.Tuple2<K, V> tuple2 = (scala.Tuple2<K, V>) ((scala.Some) result).get();
            return new AbstractMap.SimpleEntry<>(tuple2._1(), tuple2._2());
        }
        return null;
    }

    /**
     * Returns the optional head key for this map.
     *
     * @return the optional head key for this map
     */
    @SuppressWarnings("unchecked")
    public Optional<java.util.Map.Entry<K, V>> headOption() {
        return Optional.ofNullable(head());
    }

    /**
     * Returns the last key for this map.
     *
     * @return the last key for this map
     */
    @SuppressWarnings("unchecked")
    public java.util.Map.Entry<K, V> last() {
        Object result = database.lastOption().get();
        if (result instanceof scala.Some) {
            scala.Tuple2<K, V> tuple2 = (scala.Tuple2<K, V>) ((scala.Some) result).get();
            return new AbstractMap.SimpleEntry<>(tuple2._1(), tuple2._2());
        }
        return null;
    }

    /**
     * Returns the optional last key for this map.
     *
     * @return the optional last key for this map
     */
    public Optional<java.util.Map.Entry<K, V>> lastOption() {
        return Optional.ofNullable(last());
    }

    /**
     * Puts the key/value pair for this map.
     * @param key the key
     * @param value the value
     *
     * @return the old value for this key or null
     */
    public V put(K key, V value) {
        V oldValue = get(key);
        database.put(key, value).get();
        return oldValue;
    }

    /**
     * Returns the value or null for key of this map.
     * @param key the key
     *
     * @return the value or null for key of this map
     */
    @SuppressWarnings("unchecked")
    public V get(K key) {
        Object result = database.get(key).get();
        if (result instanceof scala.Some) {
            return (V) ((scala.Some) result).get();
        }
        return null;
    }

    /**
     * Removes the value for key of this map.
     * @param key the key
     *
     * @return the old value or null for key of this map
     */
    public V remove(K key) {
        V oldValue = get(key);
        database.remove(key).get();
        return oldValue;
    }

    /**
     * Clears this map.
     */
    public void clear() {
        database.clear().get();
    }

    /**
     * Returns the Maps object of this map.
     *
     * @return the Maps object of this map
     */
    public Maps<K, V> maps() {
        return database.maps();
    }

    /**
     * Closes the database.
     */
    @Override
    public void close() {
        database.closeDatabase().get();
    }

    /**
     * Starts the commit function for this map.
     * @param prepares the prepares
     *
     * @return the IO.OK for this map
     */
    @SuppressWarnings("unchecked")
    public swaydb.data.IO.OK commit(Prepare<K, V>... prepares) {
        List<Prepare<K, V>> preparesList = Arrays.asList(prepares);
        Iterable<Prepare<K, V>> prepareIterator
                = JavaConverters.iterableAsScalaIterableConverter(preparesList).asScala();
        return database.commit(prepareIterator).get();
    }

    /**
     * Creates the map.
     * @param <K> the type of the key element
     * @param <V> the type of the value element
     * @param keySerializer the keySerializer
     * @param valueSerializer the valueSerializer
     *
     * @return the map
     */
    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> create(Object keySerializer, Object valueSerializer) {
        final int mapSize = Map$.MODULE$.apply$default$1();
        final int segmentSize = Map$.MODULE$.apply$default$2();
        final int cacheSize = Map$.MODULE$.apply$default$3();
        final FiniteDuration cacheCheckDelay = Map$.MODULE$.apply$default$4();
        final double bloomFilterFalsePositiveRate = Map$.MODULE$.apply$default$5();
        final boolean compressDuplicateValues = Map$.MODULE$.apply$default$6();
        final boolean deleteSegmentsEventually = Map$.MODULE$.apply$default$7();
        final Option groupingStrategy = Map$.MODULE$.apply$default$8();
        final Function1 acceleration = Map$.MODULE$.apply$default$9();
        final swaydb.data.order.KeyOrder keyOrder = Map$.MODULE$.apply$default$12(
                mapSize, segmentSize, cacheSize, cacheCheckDelay, bloomFilterFalsePositiveRate,
                compressDuplicateValues, deleteSegmentsEventually, groupingStrategy, acceleration);
        final ExecutionContext ec = Map$.MODULE$.apply$default$13(mapSize, segmentSize, cacheSize,
                cacheCheckDelay, bloomFilterFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                groupingStrategy, acceleration);
        return new Map(
                (swaydb.extensions.Map) Map$.MODULE$.apply(mapSize, segmentSize, cacheSize, cacheCheckDelay,
                        bloomFilterFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                        groupingStrategy, acceleration, Serializer.classToType(keySerializer),
                        Serializer.classToType(valueSerializer), keyOrder, ec, ec).get());
    }

    @SuppressWarnings({"checkstyle:JavadocMethod", "checkstyle:JavadocType"})
    public static class Builder<K, V> {

        private int mapSize = Map$.MODULE$.apply$default$1();
        private int segmentSize = Map$.MODULE$.apply$default$2();
        private int cacheSize = Map$.MODULE$.apply$default$3();
        private FiniteDuration cacheCheckDelay = Map$.MODULE$.apply$default$4();
        private double bloomFilterFalsePositiveRate = Map$.MODULE$.apply$default$5();
        private boolean compressDuplicateValues = Map$.MODULE$.apply$default$6();
        private boolean deleteSegmentsEventually = Map$.MODULE$.apply$default$7();
        private Option<KeyValueGroupingStrategy> groupingStrategy = Map$.MODULE$.apply$default$8();
        private Function1<LevelZeroMeter, Accelerator> acceleration = Map$.MODULE$.apply$default$9();
        private Object keySerializer;
        private Object valueSerializer;

        public Builder<K, V> withMapSize(int mapSize) {
            this.mapSize = mapSize;
            return this;
        }

        public Builder<K, V> withSegmentSize(int segmentSize) {
            this.segmentSize = segmentSize;
            return this;
        }

        public Builder<K, V> withCacheSize(int cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }

        public Builder<K, V> withCacheCheckDelay(FiniteDuration cacheCheckDelay) {
            this.cacheCheckDelay = cacheCheckDelay;
            return this;
        }

        public Builder<K, V> withBloomFilterFalsePositiveRate(double bloomFilterFalsePositiveRate) {
            this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
            return this;
        }

        public Builder<K, V> withCompressDuplicateValues(boolean compressDuplicateValues) {
            this.compressDuplicateValues = compressDuplicateValues;
            return this;
        }

        public Builder<K, V> withDeleteSegmentsEventually(boolean deleteSegmentsEventually) {
            this.deleteSegmentsEventually = deleteSegmentsEventually;
            return this;
        }

        public Builder<K, V> withGroupingStrategy(Option<KeyValueGroupingStrategy> groupingStrategy) {
            this.groupingStrategy = groupingStrategy;
            return this;
        }

        public Builder<K, V> withAcceleration(Function1<LevelZeroMeter, Accelerator> acceleration) {
            this.acceleration = acceleration;
            return this;
        }

        public Builder<K, V> withKeySerializer(Object keySerializer) {
            this.keySerializer = keySerializer;
            return this;
        }

        public Builder<K, V> withValueSerializer(Object valueSerializer) {
            this.valueSerializer = valueSerializer;
            return this;
        }

        @SuppressWarnings("unchecked")
        public Map<K, V> build() {
            final swaydb.data.order.KeyOrder keyOrder = Map$.MODULE$.apply$default$12(
                mapSize, segmentSize, cacheSize, cacheCheckDelay, bloomFilterFalsePositiveRate,
                compressDuplicateValues, deleteSegmentsEventually, groupingStrategy, acceleration);
            final ExecutionContext ec = Map$.MODULE$.apply$default$13(mapSize, segmentSize, cacheSize,
                cacheCheckDelay, bloomFilterFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                groupingStrategy, acceleration);
            return new Map(
                (swaydb.extensions.Map) Map$.MODULE$.apply(mapSize, segmentSize, cacheSize, cacheCheckDelay,
                        bloomFilterFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                        groupingStrategy, acceleration, Serializer.classToType(keySerializer),
                        Serializer.classToType(valueSerializer), keyOrder, ec, ec).get());
        }
    }

    /**
     * Creates the builder.
     * @param <K> the type of the key element
     * @param <V> the type of the value element
     *
     * @return the builder
     */
    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

}
