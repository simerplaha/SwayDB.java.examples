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

public interface Set<K> {

    boolean contains(K elem);

    boolean mightContain(K key);

    Iterator<K> iterator();

    Object[] toArray();

    <T> T[] toArray(T[] a);

    boolean add(K key);

    boolean add(K key, long expireAfter, TimeUnit timeUnit);

    boolean add(K key, LocalDateTime expireAt);

    boolean expire(K key, long after, TimeUnit timeUnit);

    boolean expire(K key, LocalDateTime expireAt);

    boolean containsAll(Collection<K> collection);

    boolean add(List<? extends K> list);

    boolean retainAll(Collection<K> collection);

    void remove(java.util.Set<K> keys);

    void remove(K from, K to);

    int size();

    boolean isEmpty();

    boolean nonEmpty();

    LocalDateTime expiration(K key);

    Duration timeLeft(K key);

    long sizeOfSegments();

    Level0Meter level0Meter();

    Optional<LevelMeter> levelMeter(int levelNumber);

    void clear();

    boolean remove(K key);

    java.util.Set<K> asJava();

    @SuppressWarnings("unchecked")
    Level0Meter commit(Prepare<K, scala.runtime.Nothing$>... prepares);
}
