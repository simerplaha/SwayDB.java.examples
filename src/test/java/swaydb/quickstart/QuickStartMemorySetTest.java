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
package swaydb.quickstart;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import org.junit.Test;
import swaydb.base.TestBase;

public class QuickStartMemorySetTest extends TestBase {

    @SuppressWarnings("unchecked")
    @Test
    public void memorySetInt() {
        // Create a memory set database.
        // val db = memory.Set[Int].get

        final swaydb.memory.Set<Integer> db = swaydb.memory.Set.create(
                Integer.class);
        // db.add(1).get
        db.add(1);
        // db.get(1).get
        boolean result = db.contains(1);
        assertThat("result contains value", result, equalTo(true));
        // db.remove(1).get
        db.remove(1);
        boolean result2 = db.contains(1);
        assertThat("Empty result", result2, equalTo(false));

        db.commit(
                new swaydb.java.Prepare<Integer, scala.runtime.Nothing$>().put(2, null),
                new swaydb.java.Prepare().remove(1)
        );

        assertThat("two value", db.contains(2), equalTo(true));
        assertThat(db.contains(1), equalTo(false));
    }

    @Test
    public void memorySetIntIterator() {
        final swaydb.memory.Set<Integer> db = swaydb.memory.Set
                .<Integer>builder()
                .withKeySerializer(Integer.class)
                .build();
        db.add(1);
        assertThat(db.iterator().next(), equalTo(1));
    }

    @Test
    public void memorySetIntToArray() {
        final swaydb.memory.Set<Integer> db = swaydb.memory.Set
                .<Integer>builder()
                .withKeySerializer(Integer.class)
                .build();
        db.add(1);
        assertThat(Arrays.toString(db.toArray()), equalTo("[1]"));
        assertThat(Arrays.toString(db.toArray(new Integer[]{})), equalTo("[1]"));
    }

    @Test
    public void memorySetIntAddExpireAfter() {
        try (swaydb.memory.Set<Integer> db = swaydb.memory.Set
                        .<Integer>builder()
                        .withKeySerializer(Integer.class)
                        .build()) {
            db.add(1, 100, TimeUnit.MILLISECONDS);
            assertThat(db.contains(1), equalTo(true));
            await().atMost(1800, TimeUnit.MILLISECONDS).until(() -> {
                assertThat(db.contains(1), equalTo(false));
                return true;
            });
        }
    }

    @Test
    public void memorySetIntAddExpireAt() {
        try (swaydb.memory.Set<Integer> db = swaydb.memory.Set
                        .<Integer>builder()
                        .withKeySerializer(Integer.class)
                        .build()) {
            db.add(1, LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100)));
            assertThat(db.contains(1), equalTo(true));
            await().atMost(1800, TimeUnit.MILLISECONDS).until(() -> {
                assertThat(db.contains(1), equalTo(false));
                return true;
            });
        }
    }

    @Test
    public void memorySetIntExpireAfter() {
        try (swaydb.memory.Set<Integer> db = swaydb.memory.Set
                        .<Integer>builder()
                        .withKeySerializer(Integer.class)
                        .build()) {
            db.add(1);
            db.expire(1, 100, TimeUnit.MILLISECONDS);
            assertThat(db.contains(1), equalTo(true));
            await().atMost(1800, TimeUnit.MILLISECONDS).until(() -> {
                assertThat(db.contains(1), equalTo(false));
                return true;
            });
        }
    }

    @Test
    public void memorySetIntExpireAt() {
        try (swaydb.memory.Set<Integer> db = swaydb.memory.Set
                        .<Integer>builder()
                        .withKeySerializer(Integer.class)
                        .build()) {
            db.add(1);
            db.expire(1, LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100)));
            assertThat(db.contains(1), equalTo(true));
            await().atMost(1800, TimeUnit.MILLISECONDS).until(() -> {
                assertThat(db.contains(1), equalTo(false));
                return true;
            });
        }
    }

    @Test
    public void memorySetIntContainsAll() {
        try (swaydb.memory.Set<Integer> db = swaydb.memory.Set
                        .<Integer>builder()
                        .withKeySerializer(Integer.class)
                        .build()) {
            db.add(1);
            assertThat(db.containsAll(Arrays.asList(1)), equalTo(true));
        }
    }

    @Test
    public void memorySetIntAddAll() {
        try (swaydb.memory.Set<Integer> db = swaydb.memory.Set
                        .<Integer>builder()
                        .withKeySerializer(Integer.class)
                        .build()) {
            db.add(1);
            db.add(Arrays.asList(2));
            assertThat(db.containsAll(Arrays.asList(1, 2)), equalTo(true));
        }
    }

    @Test
    public void memorySetIntRetainAll() {
        try (swaydb.memory.Set<Integer> db = swaydb.memory.Set
                        .<Integer>builder()
                        .withKeySerializer(Integer.class)
                        .build()) {
            db.add(1);
            db.add(Arrays.asList(2));
            db.retainAll(Arrays.asList(1));
            assertThat(db.containsAll(Arrays.asList(1)), equalTo(true));
            db.retainAll(Arrays.asList(3));
            assertThat(db.containsAll(Arrays.asList(1)), equalTo(false));
        }
    }

    @Test
    public void memorySetIntRetainAll2() {
        try (swaydb.memory.Set<String> boxes = swaydb.memory.Set
                        .<String>builder()
                        .withKeySerializer(String.class)
                        .build()) {
            List<String> bags = new ArrayList<>();
            bags.add("pen");
            bags.add("pencil");
            bags.add("paper");

            boxes.add("pen");
            boxes.add("paper");
            boxes.add("books");
            boxes.add("rubber");

            boxes.retainAll(bags);
            assertThat(Arrays.toString(boxes.toArray()), equalTo("[paper, pen]"));
        }
    }

    @Test
    public void memorySetIntRemove() {
        try (swaydb.memory.Set<Integer> db = swaydb.memory.Set
                        .<Integer>builder()
                        .withKeySerializer(Integer.class)
                        .build()) {
            db.add(1);
            db.add(2);
            db.remove(new HashSet<>(Arrays.asList(1, 2)));
            assertThat(Arrays.toString(db.toArray()), equalTo("[]"));
            db.add(3);
            db.add(4);
            db.remove(3, 4);
            assertThat(Arrays.toString(db.toArray()), equalTo("[]"));
        }
    }

    @Test
    public void memorySetIntSize() {
        try (swaydb.memory.Set<Integer> db = swaydb.memory.Set
                        .<Integer>builder()
                        .withKeySerializer(Integer.class)
                        .build()) {
            db.add(1);
            db.add(Arrays.asList(2));
            assertThat(db.size(), equalTo(2));
            db.remove(1);
            assertThat(db.size(), equalTo(1));
        }
    }

    @Test
    public void memorySetIntIsEmpty() {
        try (swaydb.memory.Set<Integer> db = swaydb.memory.Set
                        .<Integer>builder()
                        .withKeySerializer(Integer.class)
                        .build()) {
            assertThat(db.isEmpty(), equalTo(true));
            db.add(1);
            assertThat(db.isEmpty(), equalTo(false));
        }
    }

    @Test
    public void memorySetIntNonEmpty() {
        try (swaydb.memory.Set<Integer> db = swaydb.memory.Set
                        .<Integer>builder()
                        .withKeySerializer(Integer.class)
                        .build()) {
            assertThat(db.nonEmpty(), equalTo(false));
            db.add(1);
            assertThat(db.nonEmpty(), equalTo(true));
        }
    }

    @Test
    public void memorySetIntExpiration() {
        try (swaydb.memory.Set<Integer> db = swaydb.memory.Set
                        .<Integer>builder()
                        .withKeySerializer(Integer.class)
                        .build()) {
            LocalDateTime expireAt = LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100));
            db.add(1, expireAt);
            assertThat(db.expiration(1).truncatedTo(ChronoUnit.SECONDS).toString(),
                    equalTo(expireAt.truncatedTo(ChronoUnit.SECONDS).toString()));
            assertThat(db.expiration(2), nullValue());
        }
    }

    @Test
    public void memorySetIntTimeLeft() {
        try (swaydb.memory.Set<Integer> db = swaydb.memory.Set
                        .<Integer>builder()
                        .withKeySerializer(Integer.class)
                        .build()) {
            LocalDateTime expireAt = LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100));
            db.add(1, expireAt);
            assertThat(db.timeLeft(1).getSeconds(), equalTo(0L));
            assertThat(db.timeLeft(2), nullValue());
        }
    }

    @Test
    public void memorySetIntSizes() {
        try (swaydb.memory.Set<Integer> db = swaydb.memory.Set
                        .<Integer>builder()
                        .withKeySerializer(Integer.class)
                        .build()) {
            db.add(1);
            assertThat(db.sizeOfSegments(), equalTo(0L));
            assertThat(db.level0Meter().currentMapSize(), equalTo(4000000L));
            assertThat(db.level1Meter().get().levelSize(), equalTo(0L));
            assertThat(db.levelMeter(1).get().levelSize(), equalTo(0L));
            assertThat(db.levelMeter(8).isPresent(), equalTo(false));
        }
    }

    @Test
    public void memorySetIntMightContain() {
        try (swaydb.memory.Set<Integer> db = swaydb.memory.Set
                        .<Integer>builder()
                        .withKeySerializer(Integer.class)
                        .build()) {
            db.add(1);
            assertThat(db.mightContain(1), equalTo(true));
        }
    }

    @Test
    public void memorySetIntAsJava() {
        try (swaydb.memory.Set<Integer> db = swaydb.memory.Set
                        .<Integer>builder()
                        .withKeySerializer(Integer.class)
                        .build()) {
            db.add(1);
            assertThat(db.asJava().size(), equalTo(1));
        }
    }

    @Test
    public void memorySetIntClear() {
        try (swaydb.memory.Set<Integer> db = swaydb.memory.Set
                        .<Integer>builder()
                        .withKeySerializer(Integer.class)
                        .build()) {
            db.add(1);
            assertThat(db.isEmpty(), equalTo(false));
            db.clear();
            assertThat(db.isEmpty(), equalTo(true));
        }
    }

    @Test
    public void persistentSetIntFromBuilder() {
        // val db = memory.Set[Int].get
        final swaydb.memory.Set<Integer> db = swaydb.memory.Set
                .<Integer>builder()
                .withKeySerializer(Integer.class)
                .withMapSize(4000000)
                .withSegmentSize(2000000)
                .withCacheSize(100000000)
                .withCacheCheckDelay(scala.concurrent.duration.FiniteDuration.apply(5, TimeUnit.SECONDS))
                .withBloomFilterFalsePositiveRate(0.01)
                .withCompressDuplicateValues(true)
                .withDeleteSegmentsEventually(false)
                .withGroupingStrategy(scala.Option.empty())
                .withAcceleration(swaydb.memory.Map$.MODULE$.apply$default$9())
                .build();
        // db.add(1).get
        db.add(1);
        // db.contains(1).get
        boolean result = db.contains(1);
        assertThat("result contains value", result, equalTo(true));
        // db.remove(1).get
        db.remove(1);
        boolean result2 = db.contains(1);
        assertThat("Empty result", result2, equalTo(false));
    }
}
