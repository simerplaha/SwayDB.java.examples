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
package swaydb.java.persistent;

import java.io.Closeable;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import scala.Function1;
import scala.Option;
import scala.collection.Iterable;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.Buffer;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;
import swaydb.Prepare;
import swaydb.data.IO;
import swaydb.data.accelerate.Level0Meter;
import swaydb.data.compaction.LevelMeter;
import swaydb.java.Serializer;

/**
 * The persistent Set of data.
 *
 * @param <K> the type of the key element
 */
public class Set<K> implements swaydb.java.Set<K>, Closeable {

    private final swaydb.Set<K, IO> database;

    /**
     * Constructs the Set object.
     * @param database the database
     */
    public Set(swaydb.Set<K, IO> database) {
        this.database = database;
    }

    /**
     * Checks if a set contains key.
     * @param key the key
     *
     * @return {@code true} if a set contains key, {@code false} otherwise
     */
    @Override
    public boolean contains(K key) {
        return (boolean) database.contains(key).get();
    }

    /**
     * Checks if a set might contains key.
     * @param key the key
     *
     * @return {@code true} if a set might contains key, {@code false} otherwise
     */
    @Override
    public boolean mightContain(K key) {
        return (boolean) database.mightContain(key).get();
    }

    /**
     * Returns the iterator of elements in this set.
     *
     * @return the iterator of elements in this set
     */
    @Override
    public Iterator<K> iterator() {
        Seq<K> entries = database.asScala().toSeq();
        java.util.List<K> result = new ArrayList<>();
        for (int index = 0; index < entries.size(); index += 1) {
            result.add(entries.apply(index));
        }
        return result.iterator();
    }

    /**
     * Returns the array of elements in this set.
     *
     * @return the array of elements in this set
     */
    @Override
    public Object[] toArray() {
        Seq<K> entries = database.asScala().toSeq();
        java.util.List<K> result = new ArrayList<>();
        for (int index = 0; index < entries.size(); index += 1) {
            result.add(entries.apply(index));
        }
        return result.toArray();
    }

    /**
     * Returns the typed array of elements in this set.
     *
     * @return the typed array of elements in this set
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T[] toArray(T[] a) {
        return (T[]) toArray();
    }

    /**
     * Adds the key to this set.
     * @param key the key
     *
     * @return {@code true} if a set contained key, {@code false} otherwise
     */
    @Override
    public boolean add(K key) {
        Object result = database.add(key).get();
        return result instanceof scala.Some;
    }

    /**
     * Adds the key with expire after to this set.
     * @param key the key
     * @param expireAfter the expireAfter
     * @param timeUnit the timeUnit
     *
     * @return {@code true} if a set contained key, {@code false} otherwise
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean add(K key, long expireAfter, TimeUnit timeUnit) {
        boolean result = contains(key);
        database.add(key, FiniteDuration.create(expireAfter, timeUnit)).get();
        return result;
    }

    /**
     * Adds the key with expire at to this set.
     * @param key the key
     * @param expireAt the expireAt
     *
     * @return {@code true} if a set contained key, {@code false} otherwise
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean add(K key, LocalDateTime expireAt) {
        boolean result = contains(key);
        int expireAtNano = Duration.between(LocalDateTime.now(), expireAt).getNano();
        database.add(key, FiniteDuration.create(expireAtNano, TimeUnit.NANOSECONDS).fromNow()).get();
        return result;
    }

    /**
     * Setups the expiration after for key to this set.
     * @param key the key
     * @param after the after
     * @param timeUnit the timeUnit
     *
     * @return the old value for this key or null
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean expire(K key, long after, TimeUnit timeUnit) {
        boolean result = contains(key);
        database.expire(key, FiniteDuration.create(after, timeUnit)).get();
        return result;
    }

    /**
     * Setups the expiration at for key to this set.
     * @param key the key
     * @param expireAt the expireAt
     *
     * @return the old value for this key or null
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean expire(K key, LocalDateTime expireAt) {
        boolean result = contains(key);
        int expireAtNano = Duration.between(LocalDateTime.now(), expireAt).getNano();
        database.expire(key, FiniteDuration.create(expireAtNano, TimeUnit.NANOSECONDS).fromNow()).get();
        return result;
    }

    /**
     * Checks if a set contains key collection.
     * @param collection the collection
     *
     * @return {@code true} if a set contains key, {@code false} otherwise
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean containsAll(Collection<K> collection) {
        return collection.stream()
                .allMatch(elem -> (boolean) database.contains(elem).get());
    }

    /**
     * Adds the keys to this set.
     * @param list the list
     *
     * @return {@code true} if a set contained keys, {@code false} otherwise
     */
    @Override
    public boolean add(List<? extends K> list) {
        Buffer<? extends K> entries = scala.collection.JavaConverters.asScalaBufferConverter(list).asScala();
        database.add(entries.toSet()).get();
        return true;
    }

    /**
     * Retains the keys to this set.
     * @param collection the collection
     *
     * @return {@code true} if a set contained keys, {@code false} otherwise
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean retainAll(Collection<K> collection) {
        Seq<K> entries = database.asScala().toSeq();
        java.util.List<K> result = new ArrayList<>();
        for (int index = 0; index < entries.size(); index += 1) {
            result.add(entries.apply(index));
        }
        result.stream()
                .filter(elem -> !collection.contains(elem))
                .forEach(database::remove);
        return true;
    }

    /**
     * Removes the keys of this set.
     * @param keys the keys
     */
    @Override
    public void remove(java.util.Set<K> keys) {
        database.remove(scala.collection.JavaConverters.asScalaSetConverter(keys).asScala()).get();
    }

    /**
     * Removes the keys of this set.
     * @param from the from
     * @param to the to
     */
    @Override
    public void remove(K from, K to) {
        database.remove(from, to).get();
    }

    /**
     * Returns the size of elements in this set.
     *
     * @return the size of elements in this set
     */
    @SuppressWarnings("unchecked")
    @Override
    public int size() {
        return database.asScala().size();
    }

    /**
     * Checks if a set is empty.
     *
     * @return {@code true} if a set is empty, {@code false} otherwise
     */
    @Override
    public boolean isEmpty() {
        return (boolean) database.isEmpty().get();
    }

    /**
     * Checks if a set is not empty.
     *
     * @return {@code true} if a set is not empty, {@code false} otherwise
     */
    @Override
    public boolean nonEmpty() {
        return (boolean) database.nonEmpty().get();
    }

    /**
     * Returns the expiration date for key in this set.
     * @param key the key
     *
     * @return the expiration date for key in this set
     */
    @Override
    public LocalDateTime expiration(K key) {
        Object result = database.expiration(key).get();
        if (result instanceof scala.Some) {
            Deadline expiration = (Deadline) ((scala.Some) result).get();
            return LocalDateTime.now().plusNanos(expiration.timeLeft().toNanos());
        }
        return null;
    }

    /**
     * Returns the time left for key in this set.
     * @param key the key
     *
     * @return the time left for key in this set
     */
    @Override
    public Duration timeLeft(K key) {
        Object result = database.timeLeft(key).get();
        if (result instanceof scala.Some) {
            FiniteDuration duration = (FiniteDuration) ((scala.Some) result).get();
            return Duration.ofNanos(duration.toNanos());
        }
        return null;
    }

    /**
     * Returns the size for segments for this set.
     *
     * @return the size for segments for this set
     */
    @Override
    public long sizeOfSegments() {
        return database.sizeOfSegments();
    }

    /**
     * Returns the level of meter for zerro level.
     *
     * @return the level of meter for zerro level
     */
    @Override
    public Level0Meter level0Meter() {
        return database.level0Meter();
    }

    /**
     * Returns the level of meter for first level.
     *
     * @return the level of meter for first level
     */
    public Optional<LevelMeter> level1Meter() {
        return levelMeter(1);
    }

    /**
     * Returns the level of meter for level.
     * @param levelNumber the level number
     *
     * @return the level of meter for first level
     */
    @Override
    public Optional<LevelMeter> levelMeter(int levelNumber) {
        Option<LevelMeter> levelMeter = database.levelMeter(levelNumber);
        return levelMeter.isEmpty() ? Optional.empty() : Optional.ofNullable(levelMeter.get());
    }

    /**
     * Clears this set.
     */
    @Override
    public void clear() {
        database.asScala().clear();
    }

    /**
     * Removes the key of this set.
     * @param key the key
     *
     * @return {@code true} if old key was present, {@code false} otherwise
     */
    @Override
    public boolean remove(K key) {
        Object result = database.remove(key).get();
        return result instanceof scala.Some;
    }

    /**
     * Returns the java set of this set.
     *
     * @return the java set of this set
     */
    @Override
    public java.util.Set<K> asJava() {
        return JavaConverters.setAsJavaSetConverter(database.asScala()).asJava();
    }

    /**
     * Closes the database.
     */
    @Override
    public void close() {
        database.closeDatabase().get();
    }

    /**
     * Starts the commit function for this set.
     * @param prepares the prepares
     *
     * @return the level zerro for this set
     */
    @SuppressWarnings("unchecked")
    @Override
    public Level0Meter commit(Prepare<K, scala.runtime.Nothing$>... prepares) {
        List<Prepare<K, scala.runtime.Nothing$>> preparesList = Arrays.asList(prepares);
        Iterable<Prepare<K, scala.runtime.Nothing$>> prepareIterator
                = JavaConverters.iterableAsScalaIterableConverter(preparesList).asScala();
        return (Level0Meter) database.commit(prepareIterator).get();
    }

    /**
     * Creates the set.
     * @param <K> the type of the key element
     * @param keySerializer the keySerializer
     * @param dir the dir
     *
     * @return the set
     */
    @SuppressWarnings("unchecked")
    public static <K> Set<K> create(Object keySerializer, Path dir) {
        int maxOpenSegments = swaydb.persistent.Map$.MODULE$.apply$default$2();
        int cacheSize = swaydb.persistent.Map$.MODULE$.apply$default$3();
        int mapSize = swaydb.persistent.Map$.MODULE$.apply$default$4();
        boolean mmapMaps = swaydb.persistent.Map$.MODULE$.apply$default$5();
        swaydb.data.config.RecoveryMode recoveryMode = swaydb.persistent.Map$.MODULE$.apply$default$6();
        boolean mmapAppendix = swaydb.persistent.Map$.MODULE$.apply$default$7();
        swaydb.data.config.MMAP mmapSegments = swaydb.persistent.Map$.MODULE$.apply$default$8();
        int segmentSize = swaydb.persistent.Map$.MODULE$.apply$default$9();
        int appendixFlushCheckpointSize = swaydb.persistent.Map$.MODULE$.apply$default$10();
        Seq otherDirs = swaydb.persistent.Map$.MODULE$.apply$default$11();
        FiniteDuration cacheCheckDelay = swaydb.persistent.Map$.MODULE$.apply$default$12();
        FiniteDuration segmentsOpenCheckDelay = swaydb.persistent.Map$.MODULE$.apply$default$13();
        double bloomFilterFalsePositiveRate = swaydb.persistent.Map$.MODULE$.apply$default$14();
        boolean compressDuplicateValues = swaydb.persistent.Map$.MODULE$.apply$default$15();
        boolean deleteSegmentsEventually = swaydb.persistent.Map$.MODULE$.apply$default$16();
        Option lastLevelGroupingStrategy = swaydb.persistent.Map$.MODULE$.apply$default$17();
        Function1 acceleration = swaydb.persistent.Map$.MODULE$.apply$default$18();
        swaydb.data.order.KeyOrder keyOrder = swaydb.persistent.Map$.MODULE$.apply$default$21(dir,
                maxOpenSegments, cacheSize, mapSize, mmapMaps, recoveryMode,
                mmapAppendix, mmapSegments, segmentSize, appendixFlushCheckpointSize, otherDirs,
                cacheCheckDelay, segmentsOpenCheckDelay,
                bloomFilterFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                lastLevelGroupingStrategy, acceleration);
        scala.concurrent.ExecutionContext ec = swaydb.persistent.Map$.MODULE$.apply$default$22(dir,
                maxOpenSegments, cacheSize, mapSize, mmapMaps,
                recoveryMode, mmapAppendix, mmapSegments, segmentSize, appendixFlushCheckpointSize,
                otherDirs, cacheCheckDelay, segmentsOpenCheckDelay,
                bloomFilterFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                lastLevelGroupingStrategy, acceleration);
        return new Set<>(
                (swaydb.Set<K, IO>) swaydb.persistent.Set$.MODULE$.apply(dir,
                maxOpenSegments, cacheSize, mapSize, mmapMaps, recoveryMode,
                mmapAppendix, mmapSegments, segmentSize, appendixFlushCheckpointSize, otherDirs,
                cacheCheckDelay, segmentsOpenCheckDelay,
                bloomFilterFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                lastLevelGroupingStrategy, acceleration, Serializer.classToType(keySerializer),
                keyOrder, ec).get());
    }

    @SuppressWarnings({"checkstyle:JavadocMethod", "checkstyle:JavadocType"})
    public static class Builder<K> {

        private Path dir;
        private int maxOpenSegments = swaydb.persistent.Map$.MODULE$.apply$default$2();
        private int cacheSize = swaydb.persistent.Map$.MODULE$.apply$default$3();
        private int mapSize = swaydb.persistent.Map$.MODULE$.apply$default$4();
        private boolean mmapMaps = swaydb.persistent.Map$.MODULE$.apply$default$5();
        private swaydb.data.config.RecoveryMode recoveryMode = swaydb.persistent.Map$.MODULE$.apply$default$6();
        private boolean mmapAppendix = swaydb.persistent.Map$.MODULE$.apply$default$7();
        private swaydb.data.config.MMAP mmapSegments = swaydb.persistent.Map$.MODULE$.apply$default$8();
        private int segmentSize = swaydb.persistent.Map$.MODULE$.apply$default$9();
        private int appendixFlushCheckpointSize = swaydb.persistent.Map$.MODULE$.apply$default$10();
        private Seq otherDirs = swaydb.persistent.Map$.MODULE$.apply$default$11();
        private FiniteDuration cacheCheckDelay = swaydb.persistent.Map$.MODULE$.apply$default$12();
        private FiniteDuration segmentsOpenCheckDelay = swaydb.persistent.Map$.MODULE$.apply$default$13();
        private double bloomFilterFalsePositiveRate = swaydb.persistent.Map$.MODULE$.apply$default$14();
        private boolean compressDuplicateValues = swaydb.persistent.Map$.MODULE$.apply$default$15();
        private boolean deleteSegmentsEventually = swaydb.persistent.Map$.MODULE$.apply$default$16();
        private Option lastLevelGroupingStrategy = swaydb.persistent.Map$.MODULE$.apply$default$17();
        private Function1 acceleration = swaydb.persistent.Map$.MODULE$.apply$default$18();
        private Object keySerializer;

        public Builder<K> withDir(Path dir) {
            this.dir = dir;
            return this;
        }

        public Builder<K> withMaxOpenSegments(int maxOpenSegments) {
            this.maxOpenSegments = maxOpenSegments;
            return this;
        }

        public Builder<K> withCacheSize(int cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }

        public Builder<K> withMapSize(int mapSize) {
            this.mapSize = mapSize;
            return this;
        }

        public Builder<K> withMmapMaps(boolean mmapMaps) {
            this.mmapMaps = mmapMaps;
            return this;
        }

        public Builder<K> withRecoveryMode(swaydb.data.config.RecoveryMode recoveryMode) {
            this.recoveryMode = recoveryMode;
            return this;
        }

        public Builder<K> withMmapAppendix(boolean mmapAppendix) {
            this.mmapAppendix = mmapAppendix;
            return this;
        }

        public Builder<K> withMmapSegments(swaydb.data.config.MMAP mmapSegments) {
            this.mmapSegments = mmapSegments;
            return this;
        }

        public Builder<K> withSegmentSize(int segmentSize) {
            this.segmentSize = segmentSize;
            return this;
        }

        public Builder<K> withAppendixFlushCheckpointSize(int appendixFlushCheckpointSize) {
            this.appendixFlushCheckpointSize = appendixFlushCheckpointSize;
            return this;
        }

        public Builder<K> withOtherDirs(Seq otherDirs) {
            this.otherDirs = otherDirs;
            return this;
        }

        public Builder<K> withCacheCheckDelay(FiniteDuration cacheCheckDelay) {
            this.cacheCheckDelay = cacheCheckDelay;
            return this;
        }

        public Builder<K> withSegmentsOpenCheckDelay(FiniteDuration segmentsOpenCheckDelay) {
            this.segmentsOpenCheckDelay = segmentsOpenCheckDelay;
            return this;
        }

        public Builder<K> withBloomFilterFalsePositiveRate(double bloomFilterFalsePositiveRate) {
            this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
            return this;
        }

        public Builder<K> withCompressDuplicateValues(boolean compressDuplicateValues) {
            this.compressDuplicateValues = compressDuplicateValues;
            return this;
        }

        public Builder<K> withDeleteSegmentsEventually(boolean deleteSegmentsEventually) {
            this.deleteSegmentsEventually = deleteSegmentsEventually;
            return this;
        }

        public Builder<K> withLastLevelGroupingStrategy(Option lastLevelGroupingStrategy) {
            this.lastLevelGroupingStrategy = lastLevelGroupingStrategy;
            return this;
        }

        public Builder<K> withAcceleration(Function1 acceleration) {
            this.acceleration = acceleration;
            return this;
        }

        public Builder<K> withKeySerializer(Object keySerializer) {
            this.keySerializer = keySerializer;
            return this;
        }

        @SuppressWarnings("unchecked")
        public Set<K> build() {
            swaydb.data.order.KeyOrder keyOrder = swaydb.persistent.Map$.MODULE$.apply$default$21(dir,
                    maxOpenSegments, cacheSize, mapSize, mmapMaps, recoveryMode,
                    mmapAppendix, mmapSegments, segmentSize, appendixFlushCheckpointSize, otherDirs,
                    cacheCheckDelay, segmentsOpenCheckDelay,
                    bloomFilterFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                    lastLevelGroupingStrategy, acceleration);
            scala.concurrent.ExecutionContext ec = swaydb.persistent.Map$.MODULE$.apply$default$22(dir,
                    maxOpenSegments, cacheSize, mapSize, mmapMaps,
                    recoveryMode, mmapAppendix, mmapSegments, segmentSize, appendixFlushCheckpointSize,
                    otherDirs, cacheCheckDelay, segmentsOpenCheckDelay,
                    bloomFilterFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                    lastLevelGroupingStrategy, acceleration);
            return new Set<>(
                    (swaydb.Set<K, IO>) swaydb.persistent.Set$.MODULE$.apply(dir,
                    maxOpenSegments, cacheSize, mapSize, mmapMaps, recoveryMode,
                    mmapAppendix, mmapSegments, segmentSize, appendixFlushCheckpointSize, otherDirs,
                    cacheCheckDelay, segmentsOpenCheckDelay,
                    bloomFilterFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                    lastLevelGroupingStrategy, acceleration, Serializer.classToType(keySerializer),
                    keyOrder, ec).get());
        }
    }

    /**
     * Creates the builder.
     * @param <K> the type of the key element
     *
     * @return the builder
     */
    public static <K> Builder<K> builder() {
        return new Builder<>();
    }

}
