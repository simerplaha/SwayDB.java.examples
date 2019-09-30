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
package swaydb.java.memory;

import java.io.Closeable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import scala.Function1;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterable;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.concurrent.ExecutionContext;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;
import scala.runtime.AbstractFunction1;
import swaydb.Apply;
import swaydb.Prepare;
import swaydb.IO;
import swaydb.data.accelerate.Accelerator;
import swaydb.data.accelerate.LevelZeroMeter;
import swaydb.data.api.grouping.GroupBy.KeyValues;
import swaydb.data.compaction.LevelMeter;
import swaydb.data.order.KeyOrder;
import swaydb.memory.Map$;
import swaydb.java.Serializer;

/**
 * The memory Map of data.
 *
 * @param <K> the type of the key element
 * @param <V> the type of the value element
 */
public class Map<K, V> implements swaydb.java.Map<K, V>, Closeable {

    private final swaydb.Map<K, V, IO> database;

    /**
     * Constructs the Map object.
     * @param database the database
     */
    public Map(swaydb.Map<K, V, IO> database) {
        this.database = database;
    }

    /**
     * Returns the size of elements in this map.
     *
     * @return the size of elements in this map
     */
    @Override
    public int size() {
        return database.asScala().size();
    }

    /**
     * Checks the map is empty.
     *
     * @return {@code true} if a map is empty, {@code false} otherwise
     */
    @Override
    public boolean isEmpty() {
        return (boolean) database.isEmpty().get();
    }

    /**
     * Checks the map is not empty.
     *
     * @return {@code true} if a map is not empty, {@code false} otherwise
     */
    @Override
    public boolean nonEmpty() {
        return (boolean) database.nonEmpty().get();
    }

    /**
     * Returns the expiration date for key in this map.
     * @param key the key
     *
     * @return the expiration date for key in this map
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
     * Returns the time left for key in this map.
     * @param key the key
     *
     * @return the time left for key in this map
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
     * Returns the key size in bytes for this map.
     * @param key the key
     *
     * @return the key size in bytes for this map
     */
    @Override
    public int keySize(K key) {
        return database.keySize(key);
    }

    /**
     * Returns the value size in bytes for this map.
     * @param value the value
     *
     * @return the value size in bytes for this map
     */
    @Override
    public int valueSize(V value) {
        return database.valueSize(value);
    }

    /**
     * Returns the size for segments for this map.
     *
     * @return the size for segments for this map
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
    public LevelZeroMeter level0Meter() {
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
     * Checks if a map contains key.
     * @param key the key
     *
     * @return {@code true} if a map contains key, {@code false} otherwise
     */
    @Override
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
    @Override
    public boolean mightContain(K key) {
        return (boolean) database.mightContain(key).get();
    }

    /**
     * Returns the head key for this map.
     *
     * @return the head key for this map
     */
    @SuppressWarnings("unchecked")
    @Override
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
    @Override
    public Optional<java.util.Map.Entry<K, V>> headOption() {
        return Optional.ofNullable(head());
    }

    /**
     * Returns the last key for this map.
     *
     * @return the last key for this map
     */
    @SuppressWarnings("unchecked")
    @Override
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
    @Override
    public Optional<java.util.Map.Entry<K, V>> lastOption() {
        return Optional.ofNullable(last());
    }

    /**
     * Checks if a map contains value.
     * @param value the value
     *
     * @return {@code true} if a map contains value, {@code false} otherwise
     */
    @Override
    public boolean containsValue(V value) {
        return values().contains(value);
    }

    /**
     * Puts a map object to this map.
     * @param map the map
     */
    @Override
    public void put(java.util.Map<K, V> map) {
        scala.collection.mutable.Map<K, V> entries =
                scala.collection.JavaConverters.mapAsScalaMapConverter(map).asScala();
        database.put(entries.toSet()).get();
    }

    /**
     * Puts an entry object to this map.
     * @param entry the entry
     */
    @SuppressWarnings("unchecked")
    @Override
    public void put(java.util.Map.Entry<K, V> entry) {
        database.put(entry.getKey(), entry.getValue());
    }

    /**
     * Updates map entries for this map.
     * @param map the map
     */
    @Override
    public void update(java.util.Map<K, V> map) {
        scala.collection.mutable.Map<K, V> entries =
                scala.collection.JavaConverters.mapAsScalaMapConverter(map).asScala();
        database.update(entries.toSet()).get();
    }

    /**
     * Clears this map.
     */
    @Override
    public void clear() {
        database.asScala().clear();
    }

    /**
     * Returns the key set for this map.
     *
     * @return the key set for this map
     */
    @Override
    public Set<K> keySet() {
        Seq<Tuple2<K, V>> entries = database.asScala().toSeq();
        Set<K> result = new LinkedHashSet<>();
        for (int index = 0; index < entries.size(); index += 1) {
            Tuple2<K, V> tuple2 = entries.apply(index);
            result.add(tuple2._1());
        }
        return result;
    }

    /**
     * Returns the head key for this map.
     *
     * @return the head key for this map
     */
    @SuppressWarnings("unchecked")
    @Override
    public K keysHead() {
        Object result = database.keys().headOption().get();
        if (result instanceof scala.Some) {
            return (K) ((scala.Some) result).get();
        }
        return null;
    }

    /**
     * Returns the optional head key for this map.
     *
     * @return the optional head key for this map
     */
    @Override
    public Optional<K> keysHeadOption() {
        return Optional.ofNullable(keysHead());
    }

    /**
     * Returns the last key for this map.
     *
     * @return the last key for this map
     */
    @SuppressWarnings("unchecked")
    @Override
    public K keysLast() {
        Object result = database.keys().lastOption().get();
        if (result instanceof scala.Some) {
            return (K) ((scala.Some) result).get();
        }
        return null;
    }

    /**
     * Returns the optional last key for this map.
     *
     * @return the optional last key for this map
     */
    @Override
    public Optional<K> keysLastOption() {
        return Optional.ofNullable(keysLast());
    }

    /**
     * Returns the values for this map.
     *
     * @return the values last key for this map
     */
    @Override
    public List<V> values() {
        Seq<Tuple2<K, V>> entries = database.asScala().toSeq();
        List<V> result = new ArrayList<>();
        for (int index = 0; index < entries.size(); index += 1) {
            Tuple2<K, V> tuple2 = entries.apply(index);
            result.add(tuple2._2());
        }
        return result;
    }

    /**
     * Returns the entrues for this map.
     *
     * @return the entrues last key for this map
     */
    @SuppressWarnings("unchecked")
    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        Seq<Tuple2<K, V>> entries = database.asScala().toSeq();
        Set<java.util.Map.Entry<K, V>> result = new LinkedHashSet<>();
        for (int index = 0; index < entries.size(); index += 1) {
            Tuple2<K, V> tuple2 = entries.apply(index);
            result.add(new AbstractMap.SimpleEntry<>(tuple2._1(), tuple2._2()));
        }
        return result;
    }

    /**
     * Puts the key/value pair for this map.
     * @param key the key
     * @param value the value
     *
     * @return the old value for this key or null
     */
    @Override
    public V put(K key, V value) {
        V oldValue = get(key);
        database.put(key, value).get();
        return oldValue;
    }

    /**
     * Puts the key/value pair for this map with expiration after data.
     * @param key the key
     * @param value the value
     * @param expireAfter the expireAfter
     * @param timeUnit the timeUnit
     *
     * @return the old value for this key or null
     */
    @Override
    public V put(K key, V value, long expireAfter, TimeUnit timeUnit) {
        V oldValue = get(key);
        database.put(key, value, FiniteDuration.create(expireAfter, timeUnit)).get();
        return oldValue;
    }

    /**
     * Puts the key/value pair for this map with expiration at data.
     * @param key the key
     * @param value the value
     * @param expireAt the expireAt
     *
     * @return the old value for this key or null
     */
    @Override
    public V put(K key, V value, LocalDateTime expireAt) {
        V oldValue = get(key);
        int expireAtNano = Duration.between(LocalDateTime.now(), expireAt).getNano();
        database.put(key, value, FiniteDuration.create(expireAtNano, TimeUnit.NANOSECONDS).fromNow()).get();
        return oldValue;
    }

    /**
     * Setups the expiration after for key to this map.
     * @param key the key
     * @param after the after
     * @param timeUnit the timeUnit
     *
     * @return the old value for this key or null
     */
    @Override
    public V expire(K key, long after, TimeUnit timeUnit) {
        V oldValue = get(key);
        database.expire(key, FiniteDuration.create(after, timeUnit)).get();
        return oldValue;
    }

    /**
     * Setups the expiration at for key to this map.
     * @param key the key
     * @param expireAt the expireAt
     *
     * @return the old value for this key or null
     */
    @Override
    public V expire(K key, LocalDateTime expireAt) {
        V oldValue = get(key);
        int expireAtNano = Duration.between(LocalDateTime.now(), expireAt).getNano();
        database.expire(key, FiniteDuration.create(expireAtNano, TimeUnit.NANOSECONDS).fromNow()).get();
        return oldValue;
    }

    /**
     * Updates the key/value for this map.
     * @param key the key
     * @param value the value
     *
     * @return the old value for this key or null
     */
    @Override
    public V update(K key, V value) {
        V oldValue = get(key);
        database.update(key, value).get();
        return oldValue;
    }

    /**
     * Returns the value or null for key of this map.
     * @param key the key
     *
     * @return the value or null for key of this map
     */
    @SuppressWarnings("unchecked")
    @Override
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
    @Override
    public V remove(K key) {
        V oldValue = get(key);
        database.remove(key).get();
        return oldValue;
    }

    /**
     * Removes the values for key set of this map.
     * @param keys the keys
     */
    @Override
    public void remove(Set<K> keys) {
        database.remove(scala.collection.JavaConverters.asScalaSetConverter(keys).asScala()).get();
    }

    /**
     * Removes the values for keys of this map.
     * @param from the from
     * @param to the to
     */
    @Override
    public void remove(K from, K to) {
        database.remove(from, to).get();
    }

    /**
     * Returns the java map of this map.
     *
     * @return the java map of this map
     */
    @Override
    public java.util.Map<K, V> asJava() {
        return JavaConverters.mapAsJavaMapConverter(database.asScala()).asJava();
    }

    /**
     * Registers the function for this map.
     * @param functionId the functionId
     * @param function the function
     *
     * @return the function id
     */
    @Override
    public K registerFunction(K functionId, Function<V, Apply.Map<V>> function) {
        return database.registerFunction(functionId, new AbstractFunction1<V, Apply.Map<V>>() {
            @Override
            public Apply.Map<V> apply(V value) {
                return function.apply(value);
            }
        });
    }

    /**
     * Executes the registered function for this map.
     * @param key the key
     * @param functionId the functionId
     */
    @Override
    public void applyFunction(K key, K functionId) {
        database.applyFunction(key, functionId);
    }

    /**
     * Returns the map object which starts from key for this map.
     * @param key the key
     *
     * @return the map object
     */
    @Override
    public Map<K, V> from(K key) {
        return new Map<>(database.from(key));
    }

    /**
     * Returns the map object which starts or after key for this map.
     * @param key the key
     *
     * @return the map object
     */
    @Override
    public Map<K, V> fromOrAfter(K key) {
        return new Map<>(database.fromOrAfter(key));
    }

    /**
     * Returns the map object which starts or before key for this map.
     * @param key the key
     *
     * @return the map object
     */
    @Override
    public Map<K, V> fromOrBefore(K key) {
        return new Map<>(database.fromOrBefore(key));
    }

    /**
     * Returns the key objects for this map.
     *
     * @return the key objects for this map
     */
    @Override
    public swaydb.Set<K, IO> keys() {
        return database.keys();
    }

    /**
     * Returns the reversed map object for this map.
     *
     * @return the reversed map object for this map
     */
    @Override
    public Map<K, V> reverse() {
        return new Map<>(database.reverse());
    }

    /**
     * Starts the map function for this map.
     * @param function the function
     *
     * @return the stream object for this map
     */
    @Override
    public swaydb.java.Stream<K, V> map(UnaryOperator<java.util.Map.Entry<K, V>> function) {
        return new swaydb.java.Stream<>(database.map(new AbstractFunction1<Tuple2<K, V>, Object>() {
            @Override
            public Object apply(Tuple2<K, V> tuple2) {
                java.util.Map.Entry<K, V> result = function.apply(
                      new AbstractMap.SimpleEntry<>(tuple2._1(), tuple2._2()));
                return Tuple2.apply(result.getKey(), result.getValue());
            }
        }));
    }

    /**
     * Starts the drop function for this map.
     * @param count the count
     *
     * @return the stream object for this map
     */
    @Override
    public swaydb.java.Stream<K, V> drop(int count) {
        return new swaydb.java.Stream<>(database.drop(count));
    }

    /**
     * Starts the dropWhile function for this map.
     * @param predicate the function
     *
     * @return the stream object for this map
     */
    @Override
    public swaydb.java.Stream<K, V> dropWhile(final Predicate<java.util.Map.Entry<K, V>> predicate) {
        return new swaydb.java.Stream<>(database.dropWhile(new AbstractFunction1<Tuple2<K, V>, Object>() {
            @Override
            public Object apply(Tuple2<K, V> tuple2) {
                return predicate.test(new AbstractMap.SimpleEntry<>(tuple2._1(), tuple2._2()));
            }
        }));
    }

    /**
     * Starts the take function for this map.
     * @param count the count
     *
     * @return the stream object for this map
     */
    @Override
    public swaydb.java.Stream<K, V> take(int count) {
        return new swaydb.java.Stream<>(database.take(count));
    }

    /**
     * Starts the takeWhile function for this map.
     * @param predicate the function
     *
     * @return the stream object for this map
     */
    @Override
    public swaydb.java.Stream<K, V> takeWhile(final Predicate<java.util.Map.Entry<K, V>> predicate) {
        return new swaydb.java.Stream<>(database.takeWhile(new AbstractFunction1<Tuple2<K, V>, Object>() {
            @Override
            public Object apply(Tuple2<K, V> tuple2) {
                return predicate.test(new AbstractMap.SimpleEntry<>(tuple2._1(), tuple2._2()));
            }
        }));
    }

    /**
     * Starts the foreach function for this map.
     * @param consumer the consumer
     *
     * @return the stream object for this map
     */
    @Override
    public swaydb.java.Stream<K, V> foreach(Consumer<java.util.Map.Entry<K, V>> consumer) {
        return new swaydb.java.Stream<>(database.foreach(new AbstractFunction1<Tuple2<K, V>, Object>() {
            @Override
            public Object apply(Tuple2<K, V> tuple2) {
                consumer.accept(new AbstractMap.SimpleEntry<>(tuple2._1(), tuple2._2()));
                return null;
            }
        }));
    }

    /**
     * Starts the filter function for this map.
     * @param predicate the function
     *
     * @return the stream object for this map
     */
    @Override
    public swaydb.java.Stream<K, V> filter(final Predicate<java.util.Map.Entry<K, V>> predicate) {
        return new swaydb.java.Stream<>(database.filter(new AbstractFunction1<Tuple2<K, V>, Object>() {
            @Override
            public Object apply(Tuple2<K, V> tuple2) {
                return predicate.test(new AbstractMap.SimpleEntry<>(tuple2._1(), tuple2._2()));
            }
        }));
    }

    /**
     * Closes the database.
     */
    @Override
    public void close() {
        database.close().get();
    }

    /**
     * Starts the commit function for this map.
     * @param prepares the prepares
     *
     * @return the level zerro for this map
     */
    @SuppressWarnings("unchecked")
    @Override
    public swaydb.IO.Done commit(Prepare<K, V>... prepares) {
        List<Prepare<K, V>> preparesList = Arrays.asList(prepares);
        Iterable<Prepare<K, V>> prepareIterator
                = JavaConverters.iterableAsScalaIterableConverter(preparesList).asScala();
        return (swaydb.IO.Done) database.commit(prepareIterator).get();
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
        int mapSize = Map$.MODULE$.apply$default$1();
        int segmentSize = Map$.MODULE$.apply$default$2();
        int memoryCacheSize = Map$.MODULE$.apply$default$3();
        int maxOpenSegments = Map$.MODULE$.apply$default$4();
        FiniteDuration memorySweeperPollInterval = Map$.MODULE$.apply$default$5();
        FiniteDuration fileSweeperPollInterval = Map$.MODULE$.apply$default$6();
        double mightContainFalsePositiveRate = Map$.MODULE$.apply$default$7();
        boolean compressDuplicateValues = Map$.MODULE$.apply$default$8();
        boolean deleteSegmentsEventually = Map$.MODULE$.apply$default$9();
        Option<KeyValues> groupBy = Map$.MODULE$.apply$default$10();
        Function1 acceleration = Map$.MODULE$.apply$default$11();
        KeyOrder keyOrder = Map$.MODULE$.apply$default$14(mapSize, segmentSize, memoryCacheSize, maxOpenSegments,
                memorySweeperPollInterval, fileSweeperPollInterval,
                mightContainFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually, groupBy,
                acceleration);
        ExecutionContext fileSweeperEc = Map$.MODULE$.apply$default$15(mapSize, segmentSize, memoryCacheSize,
                maxOpenSegments,
                memorySweeperPollInterval, fileSweeperPollInterval, mightContainFalsePositiveRate,
                compressDuplicateValues, deleteSegmentsEventually, groupBy, acceleration);
        ExecutionContext memorySweeperEc = Map$.MODULE$.apply$default$16(mapSize, segmentSize, memoryCacheSize,
                maxOpenSegments,
                memorySweeperPollInterval, fileSweeperPollInterval, mightContainFalsePositiveRate,
                compressDuplicateValues, deleteSegmentsEventually, groupBy, acceleration);

        return new Map<>(
                (swaydb.Map<K, V, IO>) Map$.MODULE$.apply(mapSize, segmentSize, memoryCacheSize,
                        maxOpenSegments, memorySweeperPollInterval, fileSweeperPollInterval,
                        mightContainFalsePositiveRate, compressDuplicateValues,
                deleteSegmentsEventually, groupBy, acceleration, Serializer.<K>classToType(keySerializer),
                Serializer.<V>classToType(valueSerializer), keyOrder, fileSweeperEc, memorySweeperEc).get());
    }

    @SuppressWarnings({"checkstyle:JavadocMethod", "checkstyle:JavadocType"})
    public static class Builder<K, V> {

        private int mapSize = Map$.MODULE$.apply$default$1();
        private int segmentSize = Map$.MODULE$.apply$default$2();
        private int memoryCacheSize = Map$.MODULE$.apply$default$3();
        private int maxOpenSegments = Map$.MODULE$.apply$default$4();
        private FiniteDuration memorySweeperPollInterval = Map$.MODULE$.apply$default$5();
        private FiniteDuration fileSweeperPollInterval = Map$.MODULE$.apply$default$6();
        private double mightContainFalsePositiveRate = Map$.MODULE$.apply$default$7();
        private boolean compressDuplicateValues = Map$.MODULE$.apply$default$8();
        private boolean deleteSegmentsEventually = Map$.MODULE$.apply$default$9();
        private Option<KeyValues> groupBy = Map$.MODULE$.apply$default$10();
        private Function1<LevelZeroMeter, Accelerator> acceleration = Map$.MODULE$.apply$default$11();
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

        public Builder<K, V> withMemoryCacheSize(int memoryCacheSize) {
            this.memoryCacheSize = memoryCacheSize;
            return this;
        }

        public Builder<K, V> withMaxOpenSegments(int maxOpenSegments) {
            this.maxOpenSegments = maxOpenSegments;
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

        public Builder<K, V> withGroupingStrategy(Option<KeyValues> groupBy) {
            this.groupBy = groupBy;
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
            KeyOrder keyOrder = Map$.MODULE$.apply$default$14(mapSize, segmentSize, memoryCacheSize, maxOpenSegments,
                    memorySweeperPollInterval, fileSweeperPollInterval,
                    mightContainFalsePositiveRate, compressDuplicateValues, deleteSegmentsEventually, groupBy,
                    acceleration);
            ExecutionContext fileSweeperEc = Map$.MODULE$.apply$default$15(mapSize, segmentSize, memoryCacheSize,
                    maxOpenSegments,
                    memorySweeperPollInterval, fileSweeperPollInterval, mightContainFalsePositiveRate,
                    compressDuplicateValues, deleteSegmentsEventually, groupBy, acceleration);
            ExecutionContext memorySweeperEc = Map$.MODULE$.apply$default$16(mapSize, segmentSize, memoryCacheSize,
                    maxOpenSegments,
                    memorySweeperPollInterval, fileSweeperPollInterval, mightContainFalsePositiveRate,
                    compressDuplicateValues, deleteSegmentsEventually, groupBy, acceleration);
            return new Map<>(
                    (swaydb.Map<K, V, IO>) Map$.MODULE$.apply(mapSize, segmentSize, memoryCacheSize,
                        maxOpenSegments, memorySweeperPollInterval, fileSweeperPollInterval,
                        mightContainFalsePositiveRate, compressDuplicateValues,
                deleteSegmentsEventually, groupBy, acceleration, Serializer.<K>classToType(keySerializer),
                Serializer.<V>classToType(valueSerializer), keyOrder, fileSweeperEc, memorySweeperEc).get());
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
