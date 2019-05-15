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

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import swaydb.data.IO;
import swaydb.data.accelerate.Level0Meter;
import swaydb.data.compaction.LevelMeter;

/**
 * The Map of data.
 *
 * @param <K> the type of the key element
 * @param <V> the type of the value element
 */
public interface Map<K, V> {

    /**
     * Returns the size of elements in this map.
     *
     * @return the size of elements in this map
     */
    int size();

    /**
     * Checks the map is empty.
     *
     * @return {@code true} if a map is empty, {@code false} otherwise
     */
    boolean isEmpty();

    /**
     * Checks the map is not empty.
     *
     * @return {@code true} if a map is not empty, {@code false} otherwise
     */
    boolean nonEmpty();

    /**
     * Returns the expiration date for key in this map.
     * @param key the key
     *
     * @return the expiration date for key in this map
     */
    LocalDateTime expiration(K key);

    /**
     * Returns the time left for key in this map.
     * @param key the key
     *
     * @return the time left for key in this map
     */
    java.time.Duration timeLeft(K key);

    /**
     * Returns the key size in bytes for this map.
     * @param key the key
     *
     * @return the key size in bytes for this map
     */
    int keySize(K key);

    /**
     * Returns the value size in bytes for this map.
     * @param value the value
     *
     * @return the value size in bytes for this map
     */
    int valueSize(V value);

    /**
     * Returns the size for segments for this map.
     *
     * @return the size for segments for this map
     */
    long sizeOfSegments();

    /**
     * Returns the level of meter for zerro level.
     *
     * @return the level of meter for zerro level
     */
    Level0Meter level0Meter();

    /**
     * Returns the level of meter for level.
     * @param levelNumber the level number
     *
     * @return the level of meter for first level
     */
    Optional<LevelMeter> levelMeter(int levelNumber);

    /**
     * Checks if a map contains key.
     * @param key the key
     *
     * @return {@code true} if a map contains key, {@code false} otherwise
     */
    boolean containsKey(K key);

    /**
     * Checks if a map might contain key.
     * @param key the key
     *
     * @return {@code true} if a map might contains key, {@code false} otherwise
     */
    boolean mightContain(K key);

    /**
     * Returns the head key for this map.
     *
     * @return the head key for this map
     */
    java.util.Map.Entry<K, V> head();

    /**
     * Returns the optional head key for this map.
     *
     * @return the optional head key for this map
     */
    Optional<java.util.Map.Entry<K, V>> headOption();

    /**
     * Returns the last key for this map.
     *
     * @return the last key for this map
     */
    java.util.Map.Entry<K, V> last();

    /**
     * Returns the optional last key for this map.
     *
     * @return the optional last key for this map
     */
    Optional<java.util.Map.Entry<K, V>> lastOption();

    /**
     * Checks if a map contains value.
     * @param value the value
     *
     * @return {@code true} if a map contains value, {@code false} otherwise
     */
    boolean containsValue(V value);

    /**
     * Puts a map object to this map.
     * @param map the map
     */
    void put(java.util.Map<K, V> map);

    /**
     * Puts an entry object to this map.
     * @param entry the entry
     */
    void put(java.util.Map.Entry<K, V> entry);

    /**
     * Updates map entries for this map.
     * @param map the map
     */
    void update(java.util.Map<K, V> map);

    /**
     * Clears this map.
     */
    void clear();

    /**
     * Returns the key set for this map.
     *
     * @return the key set for this map
     */
    java.util.Set<K> keySet();

    /**
     * Returns the head key for this map.
     *
     * @return the head key for this map
     */
    K keysHead();

    /**
     * Returns the optional head key for this map.
     *
     * @return the optional head key for this map
     */
    Optional<K> keysHeadOption();

    /**
     * Returns the last key for this map.
     *
     * @return the last key for this map
     */
    K keysLast();

    /**
     * Returns the optional last key for this map.
     *
     * @return the optional last key for this map
     */
    Optional<K> keysLastOption();

    /**
     * Returns the values for this map.
     *
     * @return the values last key for this map
     */
    List<V> values();

    /**
     * Returns the entrues for this map.
     *
     * @return the entrues last key for this map
     */
    java.util.Set<java.util.Map.Entry<K, V>> entrySet();

    /**
     * Puts the key/value pair for this map.
     * @param key the key
     * @param value the value
     *
     * @return the old value for this key or null
     */
    V put(K key, V value);

    /**
     * Puts the key/value pair for this map with expiration after data.
     * @param key the key
     * @param value the value
     * @param expireAfter the expireAfter
     * @param timeUnit the timeUnit
     *
     * @return the old value for this key or null
     */
    V put(K key, V value, long expireAfter, TimeUnit timeUnit);

    /**
     * Puts the key/value pair for this map with expiration at data.
     * @param key the key
     * @param value the value
     * @param expireAt the expireAt
     *
     * @return the old value for this key or null
     */
    V put(K key, V value, LocalDateTime expireAt);

    /**
     * Setups the expiration after for key to this map.
     * @param key the key
     * @param after the after
     * @param timeUnit the timeUnit
     *
     * @return the old value for this key or null
     */
    V expire(K key, long after, TimeUnit timeUnit);

    /**
     * Setups the expiration at for key to this map.
     * @param key the key
     * @param expireAt the expireAt
     *
     * @return the old value for this key or null
     */
    V expire(K key, LocalDateTime expireAt);

    /**
     * Updates the key/value for this map.
     * @param key the key
     * @param value the value
     *
     * @return the old value for this key or null
     */
    V update(K key, V value);

    /**
     * Returns the value or null for key of this map.
     * @param key the key
     *
     * @return the value or null for key of this map
     */
    V get(K key);

    /**
     * Removes the value for key of this map.
     * @param key the key
     *
     * @return the old value or null for key of this map
     */
    V remove(K key);

    /**
     * Removes the values for key set of this map.
     * @param keys the keys
     */
    void remove(java.util.Set<K> keys);

    /**
     * Removes the values for keys of this map.
     * @param from the from
     * @param to the to
     */
    void remove(K from, K to);

    /**
     * Returns the java map of this map.
     *
     * @return the java map of this map
     */
    java.util.Map<K, V> asJava();

    /**
     * Registers the function for this map.
     * @param functionId the functionId
     * @param function the function
     *
     * @return the function id
     */
    K registerFunction(K functionId, Function<V, swaydb.Apply.Map<V>> function);

    /**
     * Executes the registered function for this map.
     * @param key the key
     * @param functionId the functionId
     */
    void applyFunction(K key, K functionId);

    /**
     * Returns the map object which starts from key for this map.
     * @param key the key
     *
     * @return the map object
     */
    Map<K, V> from(K key);

    /**
     * Returns the map object which starts or after key for this map.
     * @param key the key
     *
     * @return the map object
     */
    Map<K, V> fromOrAfter(K key);

    /**
     * Returns the map object which starts or before key for this map.
     * @param key the key
     *
     * @return the map object
     */
    Map<K, V> fromOrBefore(K key);

    /**
     * Returns the key objects for this map.
     *
     * @return the key objects for this map
     */
    swaydb.Set<K, IO> keys();

    /**
     * Returns the reversed map object for this map.
     *
     * @return the reversed map object for this map
     */
    swaydb.Map<K, V, IO> reverse();

    /**
     * Starts the map function for this map.
     * @param function the function
     *
     * @return the stream object for this map
     */
    swaydb.java.Stream<K, V> map(UnaryOperator<java.util.Map.Entry<K, V>> function);

    /**
     * Starts the drop function for this map.
     * @param count the count
     *
     * @return the stream object for this map
     */
    swaydb.java.Stream<K, V> drop(int count);

    /**
     * Starts the dropWhile function for this map.
     * @param predicate the predicate
     *
     * @return the stream object for this map
     */
    swaydb.java.Stream<K, V> dropWhile(final Predicate<java.util.Map.Entry<K, V>> predicate);

    /**
     * Starts the take function for this map.
     * @param count the count
     *
     * @return the stream object for this map
     */
    swaydb.java.Stream<K, V> take(int count);

    /**
     * Starts the takeWhile function for this map.
     * @param predicate the predicate
     *
     * @return the stream object for this map
     */
    swaydb.java.Stream<K, V> takeWhile(final Predicate<java.util.Map.Entry<K, V>> predicate);

    /**
     * Starts the foreach function for this map.
     * @param consumer the consumer
     *
     * @return the stream object for this map
     */
    swaydb.java.Stream<K, V> foreach(Consumer<java.util.Map.Entry<K, V>> consumer);

    /**
     * Starts the filter function for this map.
     * @param predicate the predicate
     *
     * @return the stream object for this map
     */
    swaydb.java.Stream<K, V> filter(final Predicate<java.util.Map.Entry<K, V>> predicate);

    /**
     * Starts the commit function for this map.
     * @param prepares the prepares
     *
     * @return the level zerro for this map
     */
    @SuppressWarnings("unchecked")
    Level0Meter commit(swaydb.Prepare<K, V>... prepares);
}
