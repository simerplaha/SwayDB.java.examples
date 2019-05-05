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
import java.util.function.Function;
import scala.Function1;
import scala.Tuple2;
import scala.runtime.BoxedUnit;
import swaydb.Stream;
import swaydb.data.IO;
import swaydb.data.accelerate.Level0Meter;
import swaydb.data.compaction.LevelMeter;

public interface Map<K, V> {

    int size();

    boolean isEmpty();

    boolean nonEmpty();

    LocalDateTime expiration(K key);

    java.time.Duration timeLeft(K key);

    int keySize(K key);

    int valueSize(V value);

    long sizeOfSegments();

    Level0Meter level0Meter();

    Optional<LevelMeter> levelMeter(int levelNumber);

    boolean containsKey(K key);

    boolean mightContain(K key);

    java.util.Map.Entry<K, V> head();

    Optional<java.util.Map.Entry<K, V>> headOption();

    java.util.Map.Entry<K, V> last();

    Optional<java.util.Map.Entry<K, V>> lastOption();

    boolean containsValue(V value);

    void put(java.util.Map<K, V> map);

    void put(scala.collection.mutable.Seq seq);

    void update(java.util.Map<K, V> map);

    void clear();

    java.util.Set<K> keySet();

    K keysHead();

    Optional<K> keysHeadOption();

    K keysLast();

    Optional<K> keysLastOption();

    List<V> values();

    java.util.Set<java.util.Map.Entry<K, V>> entrySet();

    V put(K key, V value);

    V put(K key, V value, long expireAfter, TimeUnit timeUnit);

    V put(K key, V value, LocalDateTime expireAt);

    V expire(K key, long after, TimeUnit timeUnit);

    V expire(K key, LocalDateTime expireAt);

    V update(K key, V value);

    V get(K key);

    V remove(K key);

    void remove(java.util.Set<K> keys);

    void remove(K from, K to);

    java.util.Map<K, V> asJava();

    K registerFunction(K functionId, Function<V, swaydb.Apply.Map<V>> function);

    void applyFunction(K key, K functionId);

    swaydb.Map<K, V, IO> from(K key);

    swaydb.Map<K, V, IO> fromOrAfter(K key);

    swaydb.Map<K, V, IO> fromOrBefore(K key);

    swaydb.Set<K, IO> keys();

    Stream<Object, IO> map(Function1<Tuple2<K, V>, Object> function);

    Stream<Tuple2<K, V>, IO> filter(Function1<Tuple2<K, V>, Object> function);

    Stream<BoxedUnit, IO> foreach(Function1<Tuple2<K, V>, Object> function);

    @SuppressWarnings("unchecked")
    Level0Meter commit(swaydb.Prepare<K, V>... prepares);
}
