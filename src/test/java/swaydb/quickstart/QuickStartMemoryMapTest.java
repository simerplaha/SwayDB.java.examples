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
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import org.junit.Test;
import swaydb.data.slice.BytesReader;
import swaydb.data.slice.Slice$;
import swaydb.data.slice.Slice;
import swaydb.java.ApacheSerializer;

public class QuickStartMemoryMapTest {

    @SuppressWarnings("unchecked")
    @Test
    public void memoryMapIntString() {
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

            //write 100 key-values atomically
            IntStream.range(1, 100).forEach(index -> db.put(index, String.valueOf(index)));

            // Iteration: fetch all key-values withing range 10 to 90, update values
            // and atomically write updated key-values
            IntStream.rangeClosed(10, 90)
                    .mapToObj(item -> new AbstractMap.SimpleEntry<>(item, item + "_updated"))
                    .forEach(pair -> db.put(pair.getKey(), pair.getValue()));

            //assert the key-values were updated
            IntStream.rangeClosed(10, 90)
                    .mapToObj(item -> new AbstractMap.SimpleEntry<>(item, db.get(item)))
                    .forEach(pair -> assertThat(pair.getValue().endsWith("_updated"), equalTo(true)));
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
    public void memoryMapIntStringisContainsValue() {
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
    public void memoryMapIntStringisMightContain() {
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
    public void memoryMapIntStringisHead() {
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
    public void memoryMapIntStringisKeysHead() {
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
    public void memoryMapIntStringisKeysLast() {
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
    public void memoryMapIntStringisLast() {
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
    public void memoryMapIntStringisPutAll() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            Map<Integer, String> data = new LinkedHashMap<>();
            data.put(1, "one");
            db.putAll(data);
            assertThat(db.containsValue("one"), equalTo(true));
        }
    }

    @Test
    public void memoryMapIntStringisUpdateAll() {
        try (swaydb.memory.Map<Integer, String> db = swaydb.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "zerro");
            Map<Integer, String> data = new LinkedHashMap<>();
            data.put(1, "one");
            db.updateAll(data);
            assertThat(db.containsValue("one"), equalTo(true));
        }
    }

    @Test
    public void memoryMapIntStringisKeySet() {
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
    public void memoryMapIntStringisValues() {
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
    public void memoryMapIntStringisEntrySet() {
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
            await().atMost(1600, TimeUnit.MILLISECONDS).until((Callable<Boolean>) () -> {
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
            await().atMost(1200, TimeUnit.MILLISECONDS).until((Callable<Boolean>) () -> {
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
            await().atMost(1800, TimeUnit.MILLISECONDS).until((Callable<Boolean>) () -> {
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
            await().atMost(1800, TimeUnit.MILLISECONDS).until((Callable<Boolean>) () -> {
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

            public MyData(String key, String value) {
                this.key = key;
                this.value = value;
            }
        }

        class MyDataSerializer implements swaydb.serializers.Serializer<MyData> {

            @Override
            public Slice<Object> write(MyData data) {
                return Slice$.MODULE$.ByteSliceImplicits(
                        Slice$.MODULE$.ByteSliceImplicits(Slice$.MODULE$.create(data.key.length() + data.value.length(),
                                scala.reflect.ClassTag$.MODULE$.Any()))
                                .addString(data.key, StandardCharsets.UTF_8))
                        .addString(data.value, StandardCharsets.UTF_8);
            }

            @Override
            public MyData read(Slice<Object> data) {
                final BytesReader reader = Slice$.MODULE$.ByteSliceImplicits(data).createReader();
                return new MyData(reader.readString(3, StandardCharsets.UTF_8),
                        reader.readString(3, StandardCharsets.UTF_8));
            }
        }

        try (swaydb.memory.Map<Integer, MyData> db = swaydb.memory.Map
                .<Integer, MyData>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(new MyDataSerializer())
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
