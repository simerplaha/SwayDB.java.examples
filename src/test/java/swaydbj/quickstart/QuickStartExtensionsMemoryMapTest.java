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
package swaydbj.quickstart;

import java.util.concurrent.TimeUnit;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import org.junit.Test;

public class QuickStartExtensionsMemoryMapTest {

    @SuppressWarnings("unchecked")
    @Test
    public void memoryMapIntStringFrom() {
        // Create a memory database
        // val db = memory.Map[Int, String]().get
        try (swaydbj.extensions.memory.Map<Integer, String> db = swaydbj.extensions.memory.Map.create(
                Integer.class, String.class)) {
            // db.put(1, "one").get
            db.put(1, "one");
            // db.get(1).get
            String result = db.get(1);
            assertThat("result contains value", result, notNullValue());
            assertThat(result, equalTo("one"));
            // db.remove(1).get
            db.remove(1);
            String result2 = db.get(1);
            assertThat("Empty result", result2, nullValue());
            // db.put(1, "one value").get
            db.put(1, "one value");

            db.commit(
                    new swaydbj.java.Prepare<Integer, String>().put(2, "two value"),
                    new swaydbj.java.Prepare().remove(1)
            );

            assertThat(db.get(2), equalTo("two value"));
            assertThat(db.get(1), nullValue());
        }
    }

    @Test
    public void memoryMapIntStringClear() {
        // Create a memory database
        try (swaydbj.extensions.memory.Map<Integer, String> db = swaydbj.extensions.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            // db.put(1, "one").get
            db.put(1, "one");
            // db.get(1).get
            String result = db.get(1);
            assertThat("result contains value", result, notNullValue());
            assertThat(result, equalTo("one"));
            db.clear();
            String result2 = db.get(1);
            assertThat("Empty result", result2, nullValue());
        }
    }

    @Test
    public void memoryMapIntStringSize() {
        try (swaydbj.extensions.memory.Map<Integer, String> db = swaydbj.extensions.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            assertThat(db.size(), equalTo(6));
            db.put(1, "one");
            assertThat(db.size(), equalTo(7));
            db.remove(1);
            assertThat(db.size(), equalTo(6));
        }
    }

    @Test
    public void memoryMapIntStringIsEmpty() {
        try (swaydbj.extensions.memory.Map<Integer, String> db = swaydbj.extensions.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            assertThat(db.isEmpty(), equalTo(false));
            assertThat(db.nonEmpty(), equalTo(true));
        }
    }

    @Test
    public void memoryMapIntStringMightContain() {
        try (swaydbj.extensions.memory.Map<Integer, String> db = swaydbj.extensions.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "one");
            assertThat(db.mightContain(1), equalTo(true));
        }
    }

    @Test
    public void memoryMapIntStringHead() {
        try (swaydbj.extensions.memory.Map<Integer, String> db = swaydbj.extensions.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "one");
            assertThat(db.head().toString(), equalTo("1=one"));
            assertThat(db.headOption().toString(), equalTo("Optional[1=one]"));
            db.remove(1);
            assertThat(db.head(), nullValue());
        }
    }

    @Test
    public void memoryMapIntStringLast() {
        try (swaydbj.extensions.memory.Map<Integer, String> db = swaydbj.extensions.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "one");
            db.put(2, "two");
            assertThat(db.last().toString(), equalTo("2=two"));
            assertThat(db.lastOption().toString(), equalTo("Optional[2=two]"));
            db.clear();
            assertThat(db.last(), nullValue());
        }
    }

    @Test
    public void memoryMapIntStringMaps() {
        try (swaydbj.extensions.memory.Map<String, String> rootMap = swaydbj.extensions.memory.Map
                .<String, String>builder()
                .withKeySerializer(String.class)
                .withValueSerializer(String.class)
                .build()) {
            swaydb.extensions.Map<String, String> subMap1 =
                    rootMap.maps().put("sub map 1", "another map").get();
            swaydb.extensions.Map<String, String> subMap2 = subMap1.maps().put("sub map 2", "another nested map").get();

            assertThat(subMap1.contains("sub map 1").get(), equalTo(false));
            assertThat(subMap2.contains("sub map 2").get(), equalTo(false));
        }
    }

    @Test
    public void memoryMapIntStringFromBuilder() {
        try (swaydbj.extensions.memory.Map<Integer, String> db = swaydbj.extensions.memory.Map
                        .<Integer, String>builder()
                        .withKeySerializer(Integer.class)
                        .withValueSerializer(String.class)
                        .withMapSize(4000000)
                        .withSegmentSize(2000000)
                        .withCacheSize(100000000)
                        .withCacheCheckDelay(scala.concurrent.duration.FiniteDuration.apply(5, TimeUnit.SECONDS))
                        .withBloomFilterFalsePositiveRate(0.01)
                        .withCompressDuplicateValues(true)
                        .withDeleteSegmentsEventually(false)
                        .withGroupingStrategy(scala.Option.empty())
                        .withAcceleration(swaydb.memory.Map$.MODULE$.apply$default$9())
                        .build()) {
            // db.put(1, "one").get
            db.put(1, "one");
            // db.get(1).get
            String result = db.get(1);
            assertThat("result contains value", result, notNullValue());
            assertThat("Key 1 is present", db.containsKey(1), equalTo(true));
            assertThat(result, equalTo("one"));
            // db.remove(1).get
            db.remove(1);
            String result2 = db.get(1);
            assertThat("Empty result", result2, nullValue());
        }
    }

}
