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
package swaydb.java.extensions.persistent;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import scala.Function1;
import scala.Option;
import scala.collection.Iterable;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.concurrent.ExecutionContext;
import scala.concurrent.duration.FiniteDuration;
import swaydb.Prepare;
import swaydb.data.accelerate.Accelerator;
import swaydb.data.accelerate.LevelZeroMeter;
import swaydb.data.api.grouping.GroupBy;
import swaydb.data.config.Dir;
import swaydb.data.config.MMAP;
import swaydb.data.config.RecoveryMode;
import swaydb.data.order.KeyOrder;
import swaydb.extensions.Maps;
import swaydb.java.Serializer;
import swaydb.persistent.Map$;

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
    public swaydb.IO.Done commit(Prepare<K, V>... prepares) {
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
     * @param dir the directory
     *
     * @return the map
     */
    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> create(Object keySerializer, Object valueSerializer,
            Path dir) {
        int maxOpenSegments = Map$.MODULE$.apply$default$2();
        int memoryCacheSize = Map$.MODULE$.apply$default$3();
        int blockSize = Map$.MODULE$.apply$default$4();
        int mapSize = Map$.MODULE$.apply$default$5();
        boolean mmapMaps = Map$.MODULE$.apply$default$6();
        RecoveryMode recoveryMode = Map$.MODULE$.apply$default$7();
        boolean mmapAppendix = Map$.MODULE$.apply$default$8();
        MMAP mmapSegments = Map$.MODULE$.apply$default$9();
        int segmentSize = Map$.MODULE$.apply$default$10();
        int appendixFlushCheckpointSize = Map$.MODULE$.apply$default$11();
        Seq otherDirs = Map$.MODULE$.apply$default$12();
        FiniteDuration memorySweeperPollInterval = Map$.MODULE$.apply$default$13();
        FiniteDuration fileSweeperPollInterval = Map$.MODULE$.apply$default$14();
        double mightContainFalsePositiveRate = Map$.MODULE$.apply$default$15();
        boolean compressDuplicateValues = Map$.MODULE$.apply$default$16();
        boolean deleteSegmentsEventually = Map$.MODULE$.apply$default$17();
        Option<GroupBy.KeyValues> lastLevelGroupBy = Map$.MODULE$.apply$default$18();
        Function1<LevelZeroMeter, Accelerator> acceleration = Map$.MODULE$.apply$default$19();
        KeyOrder keyOrder = Map$.MODULE$.apply$default$22(dir, maxOpenSegments, memoryCacheSize, blockSize, mapSize,
                mmapMaps, recoveryMode, mmapAppendix, mmapSegments, segmentSize, appendixFlushCheckpointSize,
                otherDirs, memorySweeperPollInterval, fileSweeperPollInterval, mightContainFalsePositiveRate,
                compressDuplicateValues, deleteSegmentsEventually, lastLevelGroupBy, acceleration);
        ExecutionContext fileSweeperEc = Map$.MODULE$.apply$default$23(dir, maxOpenSegments, memoryCacheSize,
                blockSize, mapSize, mmapMaps, recoveryMode, mmapAppendix, mmapSegments, segmentSize,
                appendixFlushCheckpointSize, otherDirs, memorySweeperPollInterval, fileSweeperPollInterval,
                mightContainFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                lastLevelGroupBy, acceleration);
        ExecutionContext memorySweeperEc = Map$.MODULE$.apply$default$24(dir, maxOpenSegments, memoryCacheSize,
                blockSize, mapSize, mmapMaps, recoveryMode, mmapAppendix, mmapSegments, segmentSize,
                appendixFlushCheckpointSize, otherDirs, memorySweeperPollInterval, fileSweeperPollInterval,
                mightContainFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                lastLevelGroupBy, acceleration);
        return new Map(
                (swaydb.extensions.Map) ((swaydb.IO.Right) swaydb.extensions.persistent.Map$.MODULE$.apply(dir,
                        maxOpenSegments, memoryCacheSize, blockSize, mapSize, mmapMaps, recoveryMode, mmapAppendix,
                        mmapSegments, segmentSize, appendixFlushCheckpointSize, otherDirs, memorySweeperPollInterval,
                        fileSweeperPollInterval, mightContainFalsePositiveRate, compressDuplicateValues,
                        deleteSegmentsEventually, lastLevelGroupBy, acceleration,
                        Serializer.classToType(keySerializer), Serializer.classToType(valueSerializer),
                        keyOrder, fileSweeperEc, memorySweeperEc).get()).get());
    }

    @SuppressWarnings({"checkstyle:JavadocMethod", "checkstyle:JavadocType"})
    public static class Builder<K, V> {

        private Path dir;
        private int maxOpenSegments = Map$.MODULE$.apply$default$2();
        private int memoryCacheSize = Map$.MODULE$.apply$default$3();
        private int blockSize = Map$.MODULE$.apply$default$4();
        private int mapSize = Map$.MODULE$.apply$default$5();
        private boolean mmapMaps = Map$.MODULE$.apply$default$6();
        private RecoveryMode recoveryMode = Map$.MODULE$.apply$default$7();
        private boolean mmapAppendix = Map$.MODULE$.apply$default$8();
        private MMAP mmapSegments = Map$.MODULE$.apply$default$9();
        private int segmentSize = Map$.MODULE$.apply$default$10();
        private int appendixFlushCheckpointSize = Map$.MODULE$.apply$default$11();
        private Seq<Dir> otherDirs = Map$.MODULE$.apply$default$12();
        private FiniteDuration memorySweeperPollInterval = Map$.MODULE$.apply$default$13();
        private FiniteDuration fileSweeperPollInterval = Map$.MODULE$.apply$default$14();
        private double mightContainFalsePositiveRate = Map$.MODULE$.apply$default$15();
        private boolean compressDuplicateValues = Map$.MODULE$.apply$default$16();
        private boolean deleteSegmentsEventually = Map$.MODULE$.apply$default$17();
        private Option<GroupBy.KeyValues> lastLevelGroupBy = Map$.MODULE$.apply$default$18();
        private Function1<LevelZeroMeter, Accelerator> acceleration = Map$.MODULE$.apply$default$19();
        private Object keySerializer;
        private Object valueSerializer;

        public Builder<K, V> withDir(Path dir) {
            this.dir = dir;
            return this;
        }

        public Builder<K, V> withMaxOpenSegments(int maxOpenSegments) {
            this.maxOpenSegments = maxOpenSegments;
            return this;
        }

        public Builder<K, V> withMemoryCacheSize(int memoryCacheSize) {
            this.memoryCacheSize = memoryCacheSize;
            return this;
        }

        public Builder<K, V> withBlockSize(int blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        public Builder<K, V> withMapSize(int mapSize) {
            this.mapSize = mapSize;
            return this;
        }

        public Builder<K, V> withMmapMaps(boolean mmapMaps) {
            this.mmapMaps = mmapMaps;
            return this;
        }

        public Builder<K, V> withRecoveryMode(swaydb.data.config.RecoveryMode recoveryMode) {
            this.recoveryMode = recoveryMode;
            return this;
        }

        public Builder<K, V> withMmapAppendix(boolean mmapAppendix) {
            this.mmapAppendix = mmapAppendix;
            return this;
        }

        public Builder<K, V> withMmapSegments(swaydb.data.config.MMAP mmapSegments) {
            this.mmapSegments = mmapSegments;
            return this;
        }

        public Builder<K, V> withSegmentSize(int segmentSize) {
            this.segmentSize = segmentSize;
            return this;
        }

        public Builder<K, V> withAppendixFlushCheckpointSize(int appendixFlushCheckpointSize) {
            this.appendixFlushCheckpointSize = appendixFlushCheckpointSize;
            return this;
        }

        public Builder<K, V> withOtherDirs(Seq<Dir> otherDirs) {
            this.otherDirs = otherDirs;
            return this;
        }

        public Builder<K, V> withMemorySweeperPollInterval(FiniteDuration memorySweeperPollInterval) {
            this.memorySweeperPollInterval = memorySweeperPollInterval;
            return this;
        }

        public Builder<K, V> withFileSweeperPollInterval(FiniteDuration fileSweeperPollInterval) {
            this.fileSweeperPollInterval = fileSweeperPollInterval;
            return this;
        }

        public Builder<K, V> withMightContainFalsePositiveRate(double mightContainFalsePositiveRate) {
            this.mightContainFalsePositiveRate = mightContainFalsePositiveRate;
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

        public Builder<K, V> withLastLevelGroupBy(Option<GroupBy.KeyValues> lastLevelGroupBy) {
            this.lastLevelGroupBy = lastLevelGroupBy;
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
            KeyOrder keyOrder = Map$.MODULE$.apply$default$22(dir, maxOpenSegments, memoryCacheSize, blockSize, mapSize,
                mmapMaps, recoveryMode, mmapAppendix, mmapSegments, segmentSize, appendixFlushCheckpointSize,
                otherDirs, memorySweeperPollInterval, fileSweeperPollInterval, mightContainFalsePositiveRate,
                compressDuplicateValues, deleteSegmentsEventually, lastLevelGroupBy, acceleration);
            ExecutionContext fileSweeperEc = Map$.MODULE$.apply$default$23(dir, maxOpenSegments, memoryCacheSize,
                blockSize, mapSize, mmapMaps, recoveryMode, mmapAppendix, mmapSegments, segmentSize,
                appendixFlushCheckpointSize, otherDirs, memorySweeperPollInterval, fileSweeperPollInterval,
                mightContainFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                lastLevelGroupBy, acceleration);
            ExecutionContext memorySweeperEc = Map$.MODULE$.apply$default$24(dir, maxOpenSegments, memoryCacheSize,
                blockSize, mapSize, mmapMaps, recoveryMode, mmapAppendix, mmapSegments, segmentSize,
                appendixFlushCheckpointSize, otherDirs, memorySweeperPollInterval, fileSweeperPollInterval,
                mightContainFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                lastLevelGroupBy, acceleration);
            return new Map(
                (swaydb.extensions.Map) ((swaydb.IO.Right) swaydb.extensions.persistent.Map$.MODULE$.apply(dir,
                        maxOpenSegments, memoryCacheSize, blockSize, mapSize, mmapMaps, recoveryMode, mmapAppendix,
                        mmapSegments, segmentSize, appendixFlushCheckpointSize, otherDirs, memorySweeperPollInterval,
                        fileSweeperPollInterval, mightContainFalsePositiveRate, compressDuplicateValues,
                        deleteSegmentsEventually, lastLevelGroupBy, acceleration,
                        Serializer.classToType(keySerializer), Serializer.classToType(valueSerializer),
                        keyOrder, fileSweeperEc, memorySweeperEc).get()).get());
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
