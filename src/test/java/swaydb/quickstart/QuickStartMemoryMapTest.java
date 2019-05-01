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

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import org.junit.Test;
import scala.collection.mutable.ListBuffer;
import scala.runtime.AbstractFunction1;
import swaydb.data.slice.Slice;
import swaydb.java.ApacheSerializer;
import swaydb.java.Apply;
import swaydb.java.BytesReader;

public class QuickStartMemoryMapTest {

    @SuppressWarnings("unchecked")
    @Test
    public void memoryMapIntStringFrom() {
        // Create a memory database        
        // val db = memory.Map[Int, String]().get
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map.create(
                Integer.class, String.class)) {
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
            // db.put(1, "one value").get
            db.put(1, "one value");

            db.commit(
                    new swaydb.java.Prepare<Integer, String>().put(2, "two value"),
                    new swaydb.java.Prepare().remove(1)
            );

            assertThat(db.get(2), equalTo("two value"));
            assertThat(db.get(1), nullValue());

            // write 100 key-values atomically
            db.put(IntStream.rangeClosed(1, 100)
                    .mapToObj(index -> new AbstractMap.SimpleEntry<>(index, String.valueOf(index)))
                    .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue())));

            // Iteration: fetch all key-values withing range 10 to 90, update values
            // and atomically write updated key-values
            ((swaydb.data.IO.Success) db
                    .from(10)
                    .takeWhile(new AbstractFunction1() {
                        @Override
                        public Object apply(Object t1) {
                            return (Integer) ((scala.Tuple2) t1)._1() <= 90;
                        }
                    })
                    .map(new AbstractFunction1() {
                        @Override
                        public Object apply(Object t1) {
                            Integer key = (Integer) ((scala.Tuple2) t1)._1();
                            String value = (String) ((scala.Tuple2) t1)._2();
                            return scala.Tuple2.apply(key, value + "_updated");
                        }
                    })
                    .materialize()).foreach(new AbstractFunction1<Object, Object>() {
                        @Override
                        public Object apply(Object t1) {
                            db.put(((ListBuffer) t1).seq());
                            return null;
                        }
                    });

            // assert the key-values were updated
            IntStream.rangeClosed(10, 90)
                    .mapToObj(item -> new AbstractMap.SimpleEntry<>(item, db.get(item)))
                    .forEach(pair -> assertThat(pair.getValue().endsWith("_updated"), equalTo(true)));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void memoryMapIntStringFromOrAfter() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map.create(
                Integer.class, String.class)) {
            // write 100 key-values atomically
            db.put(IntStream.rangeClosed(1, 100)
                    .mapToObj(index -> new AbstractMap.SimpleEntry<>(index, String.valueOf(index)))
                    .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue())));

            ((swaydb.data.IO.Success) db
                    .fromOrAfter(10)
                    .takeWhile(new AbstractFunction1() {
                        @Override
                        public Object apply(Object t1) {
                            return (Integer) ((scala.Tuple2) t1)._1() <= 90;
                        }
                    })
                    .map(new AbstractFunction1() {
                        @Override
                        public Object apply(Object t1) {
                            Integer key = (Integer) ((scala.Tuple2) t1)._1();
                            String value = (String) ((scala.Tuple2) t1)._2();
                            return scala.Tuple2.apply(key, value + "_updated");
                        }
                    })
                    .materialize()).foreach(new AbstractFunction1<Object, Object>() {
                        @Override
                        public Object apply(Object t1) {
                            db.put(((ListBuffer) t1).seq());
                            return null;
                        }
                    });

            // assert the key-values were updated
            IntStream.rangeClosed(10, 90)
                    .mapToObj(item -> new AbstractMap.SimpleEntry<>(item, db.get(item)))
                    .forEach(pair -> assertThat(pair.getValue().endsWith("_updated"), equalTo(true)));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void memoryMapIntStringFromOrBefore() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map.create(
                Integer.class, String.class)) {
            // write 100 key-values atomically
            db.put(IntStream.rangeClosed(1, 100)
                    .mapToObj(index -> new AbstractMap.SimpleEntry<>(index, String.valueOf(index)))
                    .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue())));

            ((swaydb.data.IO.Success) db
                    .fromOrBefore(10)
                    .takeWhile(new AbstractFunction1() {
                        @Override
                        public Object apply(Object t1) {
                            return (Integer) ((scala.Tuple2) t1)._1() <= 90;
                        }
                    })
                    .map(new AbstractFunction1() {
                        @Override
                        public Object apply(Object t1) {
                            Integer key = (Integer) ((scala.Tuple2) t1)._1();
                            String value = (String) ((scala.Tuple2) t1)._2();
                            return scala.Tuple2.apply(key, value + "_updated");
                        }
                    })
                    .materialize()).foreach(new AbstractFunction1<Object, Object>() {
                        @Override
                        public Object apply(Object t1) {
                            db.put(((ListBuffer) t1).seq());
                            return null;
                        }
                    });

            // assert the key-values were updated
            IntStream.rangeClosed(10, 90)
                    .mapToObj(item -> new AbstractMap.SimpleEntry<>(item, db.get(item)))
                    .forEach(pair -> assertThat(pair.getValue().endsWith("_updated"), equalTo(true)));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void memoryMapIntStringKeys() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map.create(
                Integer.class, String.class)) {
            // write 100 key-values atomically
            db.put(IntStream.rangeClosed(1, 100)
                    .mapToObj(index -> new AbstractMap.SimpleEntry<>(index, String.valueOf(index)))
                    .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue())));

            final Set<Integer> result = new LinkedHashSet<>();
            db
                    .keys()
                    .reverse()
                    .fromOrBefore(10)
                    .take(5)
                    .materialize()
                    .foreach(new AbstractFunction1() {
                        @Override
                        public Object apply(Object t1) {
                            scala.collection.Seq<Integer> entries = ((ListBuffer) t1).seq();
                            for (int index = 0; index < entries.size(); index += 1) {
                                result.add(entries.apply(index));
                            }
                            return null;
                        }
                    });
            assertThat(result.toString(), equalTo("[10, 9, 8, 7, 6]"));
        }
    }

    @Test
    public void memoryMapIntStringClear() {
        // Create a memory database        
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            // db.put(1, "one").get
            db.put(1, "one");
            // db.get(1).get
            String result = db.get(1);
            assertThat("result contains value", result, notNullValue());
            assertThat("Key 1 is present", db.containsKey(1), equalTo(true));
            assertThat(result, equalTo("one"));
            db.clear();
            String result2 = db.get(1);
            assertThat("Empty result", result2, nullValue());
        }
    }

    @Test
    public void memoryMapIntStringSize() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            assertThat(db.size(), equalTo(0));
            db.put(1, "one");
            assertThat(db.size(), equalTo(1));
            db.remove(1);
            assertThat(db.size(), equalTo(0));
        }
    }

    @Test
    public void memoryMapIntStringIsEmpty() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            assertThat(db.isEmpty(), equalTo(true));
            assertThat(db.nonEmpty(), equalTo(false));
        }
    }

    @Test
    public void memoryMapIntStringContainsValue() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "one");
            assertThat(db.containsValue("one"), equalTo(true));
        }
    }

    @Test
    public void memoryMapIntStringMightContain() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
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
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
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
    public void memoryMapIntStringKeysHead() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "one");
            assertThat(db.keysHead().toString(), equalTo("1"));
            assertThat(db.keysHeadOption().toString(), equalTo("Optional[1]"));
            db.remove(1);
            assertThat(db.keysHead(), nullValue());
        }
    }

    @Test
    public void memoryMapIntStringKeysLast() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "one");
            db.put(2, "two");
            assertThat(db.keysLast().toString(), equalTo("2"));
            assertThat(db.keysLastOption().toString(), equalTo("Optional[2]"));
            db.clear();
            assertThat(db.keysLast(), nullValue());
        }
    }

    @Test
    public void memoryMapIntStringLast() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
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
    public void memoryMapIntStringPutMap() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            Map<Integer, String> data = new LinkedHashMap<>();
            data.put(1, "one");
            db.put(data);
            assertThat(db.containsValue("one"), equalTo(true));
        }
    }

    @Test
    public void memoryMapIntStringUpdateMap() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "zerro");
            Map<Integer, String> data = new LinkedHashMap<>();
            data.put(1, "one");
            db.update(data);
            assertThat(db.containsValue("one"), equalTo(true));
        }
    }

    @Test
    public void memoryMapIntStringKeySet() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "one");
            assertThat(db.keySet().toString(), equalTo("[1]"));
        }
    }

    @Test
    public void memoryMapIntStringValues() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "one");
            assertThat(db.values().toString(), equalTo("[one]"));
        }
    }

    @Test
    public void memoryMapIntStringEntrySet() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "one");
            assertThat(db.entrySet().toString(), equalTo("[1=one]"));
        }
    }

    @Test
    public void memoryMapIntStringPutExpireAfter() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "one", 100, TimeUnit.MILLISECONDS);
            assertThat(db.entrySet().toString(), equalTo("[1=one]"));
            await().atMost(1600, TimeUnit.MILLISECONDS).until(() -> {
                assertThat(db.get(1), nullValue());
                return true;
            });
        }
    }

    @Test
    public void memoryMapIntStringPutExpireAt() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "one", LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100)));
            assertThat(db.entrySet().toString(), equalTo("[1=one]"));
            await().atMost(1200, TimeUnit.MILLISECONDS).until(() -> {
                assertThat(db.get(1), nullValue());
                return true;
            });
        }
    }

    @Test
    public void memoryMapIntStringExpiration() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            LocalDateTime expireAt = LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100));
            db.put(1, "one", expireAt);
            assertThat(db.expiration(1).truncatedTo(ChronoUnit.SECONDS).toString(),
                    equalTo(expireAt.truncatedTo(ChronoUnit.SECONDS).toString()));
            assertThat(db.expiration(2), nullValue());
        }
    }

    @Test
    public void memoryMapIntStringTimeLeft() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            LocalDateTime expireAt = LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100));
            db.put(1, "one", expireAt);
            assertThat(db.timeLeft(1).getSeconds(), equalTo(0L));
            assertThat(db.timeLeft(2), nullValue());
        }
    }

    @Test
    public void memoryMapIntStringKeySize() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "one");
            assertThat(db.keySize(1), equalTo(4));
        }
    }

    @Test
    public void memoryMapIntStringValueSize() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "one");
            assertThat(db.valueSize("one"), equalTo(3));
        }
    }

    @Test
    public void memoryMapIntStringSizes() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "one");
            assertThat(db.sizeOfSegments(), equalTo(0L));
            assertThat(db.level0Meter().currentMapSize(), equalTo(4000000L));
            assertThat(db.level1Meter().get().levelSize(), equalTo(0L));
            assertThat(db.levelMeter(1).get().levelSize(), equalTo(0L));
            assertThat(db.levelMeter(8).isPresent(), equalTo(false));
        }
    }

    @Test
    public void memoryMapIntStringExpireAfter() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "one");
            db.expire(1, 100, TimeUnit.MILLISECONDS);
            assertThat(db.entrySet().toString(), equalTo("[1=one]"));
            await().atMost(1800, TimeUnit.MILLISECONDS).until(() -> {
                assertThat(db.get(1), nullValue());
                return true;
            });
        }
    }

    @Test
    public void memoryMapIntStringExpireAt() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "one");
            db.expire(1, LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100)));
            assertThat(db.entrySet().toString(), equalTo("[1=one]"));
            await().atMost(1800, TimeUnit.MILLISECONDS).until(() -> {
                assertThat(db.get(1), nullValue());
                return true;
            });
        }
    }

    @Test
    public void memoryMapIntStringUpdate() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "one");
            db.update(1, "one+1");
            assertThat(db.get(1), equalTo("one+1"));
        }
    }

    @Test
    public void memoryMapIntStringAsJava() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "one");
            assertThat(db.asJava().size(), equalTo(1));
        }
    }
    
    @Test
    public void memoryMapIntStringRemove() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "one");
            db.put(2, "two");
            db.remove(1, 2);
            assertThat(db.asJava().size(), equalTo(0));
            db.put(3, "three");
            db.put(4, "four");
            db.remove(new HashSet<>(Arrays.asList(3, 4)));
            assertThat(db.asJava().size(), equalTo(0));
        }
    }
    
    @Test
    public void memoryMapStringIntRegisterApplyFunctionUpdate() {
        try (swaydb.memory.Map<String, Integer> likesMap = swaydb.memory.Map
                .<String, Integer>builder()
                .withKeySerializer(String.class)
                .withValueSerializer(Integer.class)
                .build()) {
            // initial entry with 0 likes.
            likesMap.put("SwayDB", 0);

            String likesFunctionId = likesMap.registerFunction(
                    "increment likes counts", (Integer likesCount) -> Apply.update(likesCount + 1));
            IntStream.rangeClosed(1, 100).forEach(index -> likesMap.applyFunction("SwayDB", likesFunctionId));
            assertThat(likesMap.get("SwayDB"), equalTo(100));
        }
    }    

    @Test
    public void memoryMapStringIntRegisterApplyFunctionExpire() {
        try (swaydb.memory.Map<String, Integer> likesMap = swaydb.memory.Map
                .<String, Integer>builder()
                .withKeySerializer(String.class)
                .withValueSerializer(Integer.class)
                .build()) {
            likesMap.put("SwayDB", 0);

            String likesFunctionId = likesMap.registerFunction(
                    "expire likes counts", (Integer likesCount) ->
                            Apply.expire(LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100))));
            likesMap.applyFunction("SwayDB", likesFunctionId);
            assertThat(likesMap.get("SwayDB"), equalTo(0));
            await().atMost(1200, TimeUnit.MILLISECONDS).until(() -> {
                assertThat(likesMap.get("SwayDB"), nullValue());
                return true;
            });
        }
    }

    @Test
    public void memoryMapStringIntRegisterApplyFunctionRemove() {
        try (swaydb.memory.Map<String, Integer> likesMap = swaydb.memory.Map
                .<String, Integer>builder()
                .withKeySerializer(String.class)
                .withValueSerializer(Integer.class)
                .build()) {
            likesMap.put("SwayDB", 0);

            String likesFunctionId = likesMap.registerFunction(
                    "remove likes counts", (Integer likesCount) -> Apply.remove());
            likesMap.applyFunction("SwayDB", likesFunctionId);
            assertThat(likesMap.get("SwayDB"), equalTo(null));
        }
    }

    @Test
    public void memoryMapStringIntRegisterApplyFunctionNothing() {
        try (swaydb.memory.Map<String, Integer> likesMap = swaydb.memory.Map
                .<String, Integer>builder()
                .withKeySerializer(String.class)
                .withValueSerializer(Integer.class)
                .build()) {
            likesMap.put("SwayDB", 0);

            String likesFunctionId = likesMap.registerFunction(
                    "nothing likes counts", (Integer likesCount) -> Apply.nothing());
            likesMap.applyFunction("SwayDB", likesFunctionId);
            assertThat(likesMap.get("SwayDB"), equalTo(0));
        }
    }

    @Test
    public void memoryMapIntStringFromBuilder() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
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

    @Test
    public void memoryMapIntCustom() {

        class MyData {

            public String key;
            public String value;
            public long longValue;
            public byte byteValue;
            public boolean boolValue;

            public MyData(String key, String value,
                    long longValue, byte byteValue, boolean boolValue) {
                this.key = key;
                this.value = value;
                this.longValue = longValue;
                this.byteValue = byteValue;
                this.boolValue = boolValue;
            }
        }

        class MyDataSerializer implements swaydb.serializers.Serializer<MyData> {

            @Override
            public Slice<Object> write(MyData data) {
                return swaydb.java.Slice.create(4 + data.key.length() + 4 + data.value.length() + 10)
                        .addInt(data.key.length())
                        .addString(data.key)
                        .addInt(data.value.length())
                        .addString(data.value)
                        .addLong(data.longValue)
                        .addByte(data.byteValue)
                        .addBoolean(data.boolValue)
                        .close();
            }

            @Override
            public MyData read(Slice<Object> data) {
                final swaydb.java.BytesReader reader = BytesReader.create(data);
                int keyLength = reader.readInt();
                String key = reader.readString(keyLength);
                int valueLength = reader.readInt();
                String value = reader.readString(valueLength);
                long longValue = reader.readLong();
                byte byteValue = reader.readByte();
                boolean boolValue = reader.readBoolean();
                return new MyData(key, value, longValue, byteValue, boolValue);
            }
        }

        try (swaydb.memory.Map<Integer, MyData> db = swaydb.memory.Map
                .<Integer, MyData>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(new MyDataSerializer())
                .build()) {
            // db.put(1, new MyData("one", "two")).get
            MyData myData = new MyData("one", "two", 10L, (byte) 100, true);
            db.put(1, myData);
            // db.get(1).get
            MyData result = db.get(1);
            assertThat("result contains value", result, notNullValue());
            assertThat(result.key, equalTo("one"));
            assertThat(result.value, equalTo("two"));
            assertThat(result.longValue, equalTo(10L));
            assertThat(result.byteValue, equalTo((byte) 100));
            assertThat(result.boolValue, equalTo(true));
            // db.remove(1).get
            db.remove(1);
            MyData result2 = db.get(1);
            assertThat("Empty result", result2, nullValue());
            MyData myData2 = new MyData("one", "two", 10L, (byte) 100, false);
            db.put(1, myData2);
            MyData result3 = db.get(1);
            assertThat(result3.boolValue, equalTo(false));
        }
    }

    static class MyData implements Serializable {

        public String key;
        public String value;

        public MyData(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    @Test
    public void memoryMapIntApacheSerializer() {

        try (swaydb.memory.Map<Integer, MyData> db = swaydb.memory.Map
                .<Integer, MyData>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(new ApacheSerializer<>())
                .build()) {
            // db.put(1, new MyData("one", "two")).get
            MyData myData = new MyData("one", "two");
            db.put(1, myData);
            // db.get(1).get
            MyData result = db.get(1);
            assertThat("result contains value", result, notNullValue());
            assertThat(result.key, equalTo("one"));
            assertThat(result.value, equalTo("two"));
            // db.remove(1).get
            db.remove(1);
            MyData result2 = db.get(1);
            assertThat("Empty result", result2, nullValue());
        }
    }
}
