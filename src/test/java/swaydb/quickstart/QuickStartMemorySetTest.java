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
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import org.junit.Test;
import swaydb.base.TestBase;

public class QuickStartMemorySetTest extends TestBase {

    @SuppressWarnings("unchecked")
    @Test
    public void memorySetInt() {
        // Create a memory set database.
        // val db = memory.Set[Int].get

        final swaydb.memory.Set<Integer> db = swaydb.memory.Set.<Integer>create(
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
            await().atMost(1800, TimeUnit.MILLISECONDS).until((Callable<Boolean>) () -> {
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
            await().atMost(1800, TimeUnit.MILLISECONDS).until((Callable<Boolean>) () -> {
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
            await().atMost(1800, TimeUnit.MILLISECONDS).until((Callable<Boolean>) () -> {
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
            await().atMost(1800, TimeUnit.MILLISECONDS).until((Callable<Boolean>) () -> {
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
            db.addAll(Arrays.asList(2));
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
            db.addAll(Arrays.asList(2));
            db.retainAll(Arrays.asList(1));
            assertThat(db.containsAll(Arrays.asList(1)), equalTo(true));
        }
    }
    
    @Test
    public void memorySetIntRemoveAll() {  
        try (swaydb.memory.Set<Integer> db = swaydb.memory.Set
                        .<Integer>builder()
                        .withKeySerializer(Integer.class)
                        .build()) {
            db.add(1);
            db.addAll(Arrays.asList(2));
            db.removeAll(Arrays.asList(1));
            assertThat(Arrays.toString(db.toArray()), equalTo("[2]"));
        }
    }

    @Test
    public void memorySetIntSize() {
        try (swaydb.memory.Set<Integer> db = swaydb.memory.Set
                        .<Integer>builder()
                        .withKeySerializer(Integer.class)
                        .build()) {
            db.add(1);
            db.addAll(Arrays.asList(2));
            assertThat(db.size(), equalTo(2));
            db.removeAll(Arrays.asList(1));
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
