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
package swaydb.extensions.persistent;

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
import scala.concurrent.duration.FiniteDuration;
import swaydb.Prepare;
import swaydb.data.accelerate.Level0Meter;
import swaydb.extensions.Maps;
import swaydb.java.Serializer;

public class Map<K, V> implements Closeable {

    private final swaydb.extensions.Map<K, V> database;

    private Map(swaydb.extensions.Map<K, V> database) {
        this.database = database;
    }

    public int size() {
        return database.baseMap().asScala().size();
    }

    public boolean isEmpty() {
        return (boolean) database.baseMap().isEmpty().get();
    }

    public boolean nonEmpty() {
        return (boolean) database.baseMap().nonEmpty().get();
    }
    
    public boolean containsKey(K key) {
        return (boolean) database.contains(key).get();
    }

    @SuppressWarnings("unchecked")
    public boolean mightContain(K key) {
        return (boolean) database.mightContain(key).get();
    }

    @SuppressWarnings("unchecked")
    public java.util.Map.Entry<K, V> head() {
        Object result = database.headOption().get();
        if (result instanceof scala.Some) {
            scala.Tuple2<K, V> tuple2 = (scala.Tuple2<K, V>) ((scala.Some) result).get();
            return new AbstractMap.SimpleEntry<>(tuple2._1(), tuple2._2());
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public Optional<java.util.Map.Entry<K, V>> headOption() {
        return Optional.ofNullable(head());
    }

    @SuppressWarnings("unchecked")
    public java.util.Map.Entry<K, V> last() {
        Object result = database.lastOption().get();
        if (result instanceof scala.Some) {
            scala.Tuple2<K, V> tuple2 = (scala.Tuple2<K, V>) ((scala.Some) result).get();
            return new AbstractMap.SimpleEntry<>(tuple2._1(), tuple2._2());
        }
        return null;
    }

    public Optional<java.util.Map.Entry<K, V>> lastOption() {
        return Optional.ofNullable(last());
    }

    public V put(K key, V value) {
        V oldValue = get(key);
        database.put(key, value).get();
        return oldValue;
    }

    @SuppressWarnings("unchecked")
    public V get(K key) {
        Object result = database.get(key).get();
        if (result instanceof scala.Some) {
            return (V) ((scala.Some) result).get();
        }
        return null;
    }

    public V remove(K key) {
        V oldValue = get(key);
        database.remove(key).get();
        return oldValue;
    }

    public void clear() {
        database.clear().get();
    }

    public Maps<K, V> maps() {
        return database.maps();
    }

    @Override
    public void close() {
        database.closeDatabase().get();
    }

    @SuppressWarnings("unchecked")
    public Level0Meter commit(Prepare<K, V>... prepares) {
        List<Prepare<K, V>> preparesList = Arrays.asList(prepares);
        Iterable<Prepare<K, V>> prepareIterator
                = JavaConverters.iterableAsScalaIterableConverter(preparesList).asScala();
        return database.commit(prepareIterator).get();
    }

    @SuppressWarnings("unchecked")
    public static <K, V> swaydb.extensions.persistent.Map<K, V> create(Object keySerializer, Object valueSerializer,
            Path dir) {
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
        swaydb.data.order.KeyOrder keyOrder = swaydb.persistent.Map$.MODULE$.apply$default$21(
                dir, maxOpenSegments, cacheSize, mapSize, mmapMaps, recoveryMode,
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
        return new swaydb.extensions.persistent.Map(
                (swaydb.extensions.Map) swaydb.extensions.persistent.Map$.MODULE$.apply(dir,
                maxOpenSegments, cacheSize, mapSize, mmapMaps, recoveryMode,
                mmapAppendix, mmapSegments, segmentSize, appendixFlushCheckpointSize, otherDirs,
                cacheCheckDelay, segmentsOpenCheckDelay,
                bloomFilterFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                lastLevelGroupingStrategy, acceleration,
                Serializer.classToType(keySerializer), Serializer.classToType(valueSerializer), keyOrder, ec).get());
    }

    public static class Builder<K, V> {

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
        private Object valueSerializer;

        public Builder<K, V> withDir(Path dir) {
            this.dir = dir;
            return this;
        }

        public Builder<K, V> withMaxOpenSegments(int maxOpenSegments) {
            this.maxOpenSegments = maxOpenSegments;
            return this;
        }

        public Builder<K, V> withCacheSize(int cacheSize) {
            this.cacheSize = cacheSize;
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

        public Builder<K, V> withOtherDirs(Seq otherDirs) {
            this.otherDirs = otherDirs;
            return this;
        }

        public Builder<K, V> withCacheCheckDelay(FiniteDuration cacheCheckDelay) {
            this.cacheCheckDelay = cacheCheckDelay;
            return this;
        }

        public Builder<K, V> withSegmentsOpenCheckDelay(FiniteDuration segmentsOpenCheckDelay) {
            this.segmentsOpenCheckDelay = segmentsOpenCheckDelay;
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

        public Builder<K, V> withLastLevelGroupingStrategy(Option lastLevelGroupingStrategy) {
            this.lastLevelGroupingStrategy = lastLevelGroupingStrategy;
            return this;
        }

        public Builder<K, V> withAcceleration(Function1 acceleration) {
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
        public swaydb.extensions.persistent.Map<K, V> build() {
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
            return new Map(
                (swaydb.extensions.Map) swaydb.extensions.persistent.Map$.MODULE$.apply(dir,
                maxOpenSegments, cacheSize, mapSize, mmapMaps, recoveryMode,
                mmapAppendix, mmapSegments, segmentSize, appendixFlushCheckpointSize, otherDirs,
                cacheCheckDelay, segmentsOpenCheckDelay,
                bloomFilterFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually,
                lastLevelGroupingStrategy, acceleration,
                Serializer.classToType(keySerializer), Serializer.classToType(valueSerializer), keyOrder, ec).get());
        }
    }

    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

}
