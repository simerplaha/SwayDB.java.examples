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
package swaydb.memory;

import java.io.Closeable;
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
import scala.concurrent.ExecutionContext;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;
import swaydb.Prepare;
import swaydb.data.IO;
import swaydb.data.accelerate.Accelerator;
import swaydb.data.accelerate.Level0Meter;
import swaydb.data.api.grouping.KeyValueGroupingStrategy;
import swaydb.data.compaction.LevelMeter;
import swaydb.java.Serializer;

public class Set<K> implements Closeable {

    private final swaydb.Set<K, IO> database;

    private Set(swaydb.Set<K, IO> database) {
        this.database = database;
    }

    public boolean contains(K elem) {
        return (boolean) database.contains(elem).get();
    }

    @SuppressWarnings("unchecked")
    public boolean mightContain(Object key) {
        return (boolean) database.mightContain((K) key).get();
    }

    public Iterator<K> iterator() {
        Seq<K> entries = database.asScala().toSeq();
        java.util.List<K> result = new ArrayList<>();
        for (int index = 0; index < entries.size(); index += 1) {
            result.add(entries.apply(index));
        }
        return result.iterator();
    }

    public Object[] toArray() {
        Seq<K> entries = database.asScala().toSeq();
        java.util.List<K> result = new ArrayList<>();
        for (int index = 0; index < entries.size(); index += 1) {
            result.add(entries.apply(index));
        }
        return result.toArray();
    }

    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        return (T[]) toArray();
    }

    public boolean add(K key) {
        Object result = database.add(key).get();
        return result instanceof scala.Some;
    }

    @SuppressWarnings("unchecked")
    public boolean add(K key, long expireAfter, TimeUnit timeUnit) {
        boolean result = contains(key);
        database.add(key, FiniteDuration.create(expireAfter, timeUnit)).get();
        return result;
    }

    @SuppressWarnings("unchecked")
    public boolean add(K key, LocalDateTime expireAt) {
        boolean result = contains(key);
        int expireAtNano = Duration.between(LocalDateTime.now(), expireAt).getNano();
        database.add(key, FiniteDuration.create(expireAtNano, TimeUnit.NANOSECONDS).fromNow()).get();
        return result;
    }

    @SuppressWarnings("unchecked")
    public boolean expire(K key, long after, TimeUnit timeUnit) {
        boolean result = contains(key);
        database.expire(key, FiniteDuration.create(after, timeUnit)).get();
        return result;
    }

    @SuppressWarnings("unchecked")
    public boolean expire(K key, LocalDateTime expireAt) {
        boolean result = contains(key);
        int expireAtNano = Duration.between(LocalDateTime.now(), expireAt).getNano();
        database.expire(key, FiniteDuration.create(expireAtNano, TimeUnit.NANOSECONDS).fromNow()).get();
        return result;
    }

    @SuppressWarnings("unchecked")
    public boolean containsAll(Collection<?> collection) {
        return collection.stream()
                .allMatch(elem -> (boolean) database.contains((K) elem).get());
    }

    public boolean addAll(Collection<? extends K> collection) {
        collection.forEach(this::add);
        return true;
    }

    @SuppressWarnings("unchecked")
    public boolean retainAll(Collection<?> collection) {
        collection.stream()
                .filter(elem -> !(boolean) database.contains((K) elem).get())
                .forEach(this::remove);
        return true;
    }

    public boolean removeAll(Collection<?> collection) {
        collection.forEach(this::remove);
        return true;
    }

    @SuppressWarnings("unchecked")
    public int size() {
        return database.asScala().size();
    }

    public boolean isEmpty() {
        return (boolean) database.isEmpty().get();
    }

    public boolean nonEmpty() {
        return (boolean) database.nonEmpty().get();
    }

    public LocalDateTime expiration(K key) {
        Object result = database.expiration(key).get();
        if (result instanceof scala.Some) {
            Deadline expiration = (Deadline) ((scala.Some) result).get();
            return LocalDateTime.now().plusNanos(expiration.timeLeft().toNanos());
        }
        return null;
    }

    public Duration timeLeft(K key) {
        Object result = database.timeLeft(key).get();
        if (result instanceof scala.Some) {
            FiniteDuration duration = (FiniteDuration) ((scala.Some) result).get();
            return Duration.ofNanos(duration.toNanos());
        }
        return null;
    }

    public long sizeOfSegments() {
        return database.sizeOfSegments();
    }

    public Level0Meter level0Meter() {
        return database.level0Meter();
    }

    public Optional<LevelMeter> level1Meter() {
        return levelMeter(1);
    }

    public Optional<LevelMeter> levelMeter(int levelNumber) {
        Option<LevelMeter> levelMeter = database.levelMeter(levelNumber);
        return levelMeter.isEmpty() ? Optional.empty() : Optional.ofNullable(levelMeter.get());
    }

    public void clear() {
        database.asScala().clear();
    }

    @SuppressWarnings("unchecked")
    public boolean remove(Object key) {
        Object result = database.remove((K) key).get();
        return result instanceof scala.Some;
    }
    
    public java.util.Set<K> asJava() {
        return JavaConverters.setAsJavaSetConverter(database.asScala()).asJava();
    }

    @Override
    public void close() {
        database.closeDatabase().get();
    }

    @SuppressWarnings("unchecked")
    public Level0Meter commit(Prepare<K, scala.runtime.Nothing$>... prepares) {
        List<Prepare<K, scala.runtime.Nothing$>> preparesList = Arrays.asList(prepares);
        Iterable<Prepare<K, scala.runtime.Nothing$>> prepareIterator
                = JavaConverters.iterableAsScalaIterableConverter(preparesList).asScala();
        return (Level0Meter) database.commit(prepareIterator).get();
    }

    @SuppressWarnings("unchecked")
    public static <K> swaydb.memory.Set<K> create(Class<K> keySerializer) {
        int mapSize = Map$.MODULE$.apply$default$1();
        int segmentSize = Map$.MODULE$.apply$default$2();
        int cacheSize = Map$.MODULE$.apply$default$3();
        FiniteDuration cacheCheckDelay = Map$.MODULE$.apply$default$4();
        double bloomFilterFalsePositiveRate = Map$.MODULE$.apply$default$5();
        boolean compressDuplicateValues = Map$.MODULE$.apply$default$6();
        boolean deleteSegmentsEventually = Map$.MODULE$.apply$default$7();
        Option<KeyValueGroupingStrategy> groupingStrategy = Map$.MODULE$.apply$default$8();
        Function1<Level0Meter, Accelerator> acceleration = Map$.MODULE$.apply$default$9();
        swaydb.data.order.KeyOrder keyOrder = Map$.MODULE$.apply$default$12(mapSize, segmentSize,
                cacheSize, cacheCheckDelay, bloomFilterFalsePositiveRate, compressDuplicateValues,
                deleteSegmentsEventually, groupingStrategy, acceleration);
        ExecutionContext ec = Map$.MODULE$.apply$default$13(mapSize, segmentSize, cacheSize,
                cacheCheckDelay, bloomFilterFalsePositiveRate,
                compressDuplicateValues, deleteSegmentsEventually, groupingStrategy, acceleration);
        swaydb.memory.Set<K> memorySet = new swaydb.memory.Set<>(
                (swaydb.Set<K, IO>) Set$.MODULE$.apply(mapSize, segmentSize, cacheSize,
                cacheCheckDelay, bloomFilterFalsePositiveRate, compressDuplicateValues,
                deleteSegmentsEventually, groupingStrategy, acceleration, Serializer.classToType(keySerializer),
                keyOrder, ec).get());
        return memorySet;
    }

    public static class Builder<K> {

        private int mapSize = Map$.MODULE$.apply$default$1();
        private int segmentSize = Map$.MODULE$.apply$default$2();
        private int cacheSize = Map$.MODULE$.apply$default$3();
        private FiniteDuration cacheCheckDelay = Map$.MODULE$.apply$default$4();
        private double bloomFilterFalsePositiveRate = Map$.MODULE$.apply$default$5();
        private boolean compressDuplicateValues = Map$.MODULE$.apply$default$6();
        private boolean deleteSegmentsEventually = Map$.MODULE$.apply$default$7();
        private Option<KeyValueGroupingStrategy> groupingStrategy = Map$.MODULE$.apply$default$8();
        private Function1<Level0Meter, Accelerator> acceleration = Map$.MODULE$.apply$default$9();
        private Class<?> keySerializer;

        public Builder<K> withMapSize(int mapSize) {
            this.mapSize = mapSize;
            return this;
        }

        public Builder<K> withSegmentSize(int segmentSize) {
            this.segmentSize = segmentSize;
            return this;
        }

        public Builder<K> withCacheSize(int cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }

        public Builder<K> withCacheCheckDelay(FiniteDuration cacheCheckDelay) {
            this.cacheCheckDelay = cacheCheckDelay;
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

        public Builder<K> withGroupingStrategy(Option<KeyValueGroupingStrategy> groupingStrategy) {
            this.groupingStrategy = groupingStrategy;
            return this;
        }

        public Builder<K> withAcceleration(Function1<Level0Meter, Accelerator> acceleration) {
            this.acceleration = acceleration;
            return this;
        }

        public Builder<K> withKeySerializer(Class<?> keySerializer) {
            this.keySerializer = keySerializer;
            return this;
        }

        @SuppressWarnings("unchecked")
        public swaydb.memory.Set<K> build() {
            swaydb.data.order.KeyOrder keyOrder = Map$.MODULE$.apply$default$12(mapSize, segmentSize,
                    cacheSize, cacheCheckDelay, bloomFilterFalsePositiveRate, compressDuplicateValues,
                    deleteSegmentsEventually, groupingStrategy, acceleration);
            ExecutionContext ec = Map$.MODULE$.apply$default$13(mapSize, segmentSize, cacheSize,
                    cacheCheckDelay, bloomFilterFalsePositiveRate,
                    compressDuplicateValues, deleteSegmentsEventually, groupingStrategy, acceleration);
            swaydb.memory.Set<K> memorySet = new swaydb.memory.Set<>(
                    (swaydb.Set<K, IO>) Set$.MODULE$.apply(mapSize, segmentSize, cacheSize,
                    cacheCheckDelay, bloomFilterFalsePositiveRate, compressDuplicateValues,
                    deleteSegmentsEventually, groupingStrategy, acceleration, Serializer.classToType(keySerializer),
                    keyOrder, ec).get());
            return memorySet;
        }
    }

    public static <K> Builder<K> builder() {
        return new Builder<>();
    }

}
