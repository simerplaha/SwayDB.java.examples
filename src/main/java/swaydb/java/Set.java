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

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import swaydb.Prepare;
import swaydb.data.accelerate.Level0Meter;
import swaydb.data.compaction.LevelMeter;

/**
 * The Set of data.
 *
 * @param <K> the type of the key element
 */
public interface Set<K> {

    /**
     * Checks if a set contains key.
     * @param key the key
     *
     * @return {@code true} if a set contains key, {@code false} otherwise
     */
    boolean contains(K key);

    /**
     * Checks if a set might contains key.
     * @param key the key
     *
     * @return {@code true} if a set might contains key, {@code false} otherwise
     */
    boolean mightContain(K key);

    /**
     * Returns the iterator of elements in this set.
     *
     * @return the iterator of elements in this set
     */
    Iterator<K> iterator();

    /**
     * Returns the array of elements in this set.
     *
     * @return the array of elements in this set
     */
    Object[] toArray();

    /**
     * Returns the typed array of elements in this set.
     * @param <T> the type of the array
     * @param a the typed object
     *
     * @return the typed array of elements in this set
     */
    <T> T[] toArray(T[] a);

    /**
     * Adds the key to this set.
     * @param key the key
     *
     * @return {@code true} if a set contained key, {@code false} otherwise
     */
    boolean add(K key);

    /**
     * Adds the key with expire after to this set.
     * @param key the key
     * @param expireAfter the expireAfter
     * @param timeUnit the timeUnit
     *
     * @return {@code true} if a set contained key, {@code false} otherwise
     */
    boolean add(K key, long expireAfter, TimeUnit timeUnit);

    /**
     * Adds the key with expire at to this set.
     * @param key the key
     * @param expireAt the expireAt
     *
     * @return {@code true} if a set contained key, {@code false} otherwise
     */
    boolean add(K key, LocalDateTime expireAt);

    /**
     * Setups the expiration after for key to this set.
     * @param key the key
     * @param after the after
     * @param timeUnit the timeUnit
     *
     * @return the old value for this key or null
     */
    boolean expire(K key, long after, TimeUnit timeUnit);

    /**
     * Setups the expiration at for key to this set.
     * @param key the key
     * @param expireAt the expireAt
     *
     * @return the old value for this key or null
     */
    boolean expire(K key, LocalDateTime expireAt);

    /**
     * Checks if a set contains key collection.
     * @param collection the collection
     *
     * @return {@code true} if a set contains key, {@code false} otherwise
     */
    boolean containsAll(Collection<K> collection);

    /**
     * Adds the keys to this set.
     * @param list the list
     *
     * @return {@code true} if a set contained keys, {@code false} otherwise
     */
    boolean add(List<? extends K> list);

    /**
     * Retains the keys to this set.
     * @param collection the collection
     *
     * @return {@code true} if a set contained keys, {@code false} otherwise
     */
    boolean retainAll(Collection<K> collection);

    /**
     * Removes the keys of this set.
     * @param keys the keys
     */
    void remove(java.util.Set<K> keys);

    /**
     * Removes the keys of this set.
     * @param from the from
     * @param to the to
     */
    void remove(K from, K to);

    /**
     * Returns the size of elements in this set.
     *
     * @return the size of elements in this set
     */
    int size();

    /**
     * Checks if a set is empty.
     *
     * @return {@code true} if a set is empty, {@code false} otherwise
     */
    boolean isEmpty();

    /**
     * Checks if a set is not empty.
     *
     * @return {@code true} if a set is not empty, {@code false} otherwise
     */
    boolean nonEmpty();

    /**
     * Returns the expiration date for key in this set.
     * @param key the key
     *
     * @return the expiration date for key in this set
     */
    LocalDateTime expiration(K key);

    /**
     * Returns the time left for key in this set.
     * @param key the key
     *
     * @return the time left for key in this set
     */
    Duration timeLeft(K key);

    /**
     * Returns the size for segments for this set.
     *
     * @return the size for segments for this set
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
     * Clears this set.
     */
    void clear();

    /**
     * Removes the key of this set.
     * @param key the key
     *
     * @return {@code true} if old key was present, {@code false} otherwise
     */
    boolean remove(K key);

    /**
     * Returns the java set of this set.
     *
     * @return the java set of this set
     */
    java.util.Set<K> asJava();

    /**
     * Starts the commit function for this set.
     * @param prepares the prepares
     *
     * @return the level zerro for this set
     */
    @SuppressWarnings("unchecked")
    Level0Meter commit(Prepare<K, scala.runtime.Nothing$>... prepares);
}
