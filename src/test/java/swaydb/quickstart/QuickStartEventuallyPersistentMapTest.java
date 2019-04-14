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

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import org.junit.BeforeClass;
import org.junit.Test;
import swaydb.base.TestBase;
import swaydb.data.config.MMAP;
import swaydb.data.config.RecoveryMode;
import swaydb.data.slice.Slice;
import swaydb.java.ApacheSerializer;
import swaydb.java.Apply;
import swaydb.java.BytesReader;

public class QuickStartEventuallyPersistentMapTest extends TestBase {
    
    @BeforeClass
    public static void beforeClass() throws IOException {
        deleteDirectoryWalkTree(Paths.get("disk2"));
        deleteDirectoryWalkTree(Paths.get("disk2builder"));
        deleteDirectoryWalkTree(Paths.get("disk2builderClear"));
        deleteDirectoryWalkTree(Paths.get("disk2builderContainsValue"));
        deleteDirectoryWalkTree(Paths.get("disk2builderMightContain"));
        deleteDirectoryWalkTree(Paths.get("disk2builderEntries"));
        deleteDirectoryWalkTree(Paths.get("disk2builderIsEmpty"));
        deleteDirectoryWalkTree(Paths.get("disk2builderKeySet"));
        deleteDirectoryWalkTree(Paths.get("disk2builderPutAll"));
        deleteDirectoryWalkTree(Paths.get("disk2builderSize"));
        deleteDirectoryWalkTree(Paths.get("disk2builderValues"));
        deleteDirectoryWalkTree(Paths.get("disk2putExpireAfter"));
        deleteDirectoryWalkTree(Paths.get("disk2putExpireAt"));
        deleteDirectoryWalkTree(Paths.get("disk2expireAfter"));
        deleteDirectoryWalkTree(Paths.get("disk2expireAt"));
        deleteDirectoryWalkTree(Paths.get("disk2update"));
        deleteDirectoryWalkTree(Paths.get("disk2asjava"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void persistentMapIntString() {
        // Create a eventually persistent database. If the directories do not exist, they will be created.
        // val db = eventually.persistent.Map[Int, String](dir = dir.resolve("disk2")).get
        try (swaydb.eventually.persistent.Map<Integer, String> db =
                swaydb.eventually.persistent.Map.<Integer, String>create(
                        Integer.class, String.class, Paths.get("disk2"))) {
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
            db.commit(
                    new swaydb.java.Prepare<Integer, String>().put(2, "two value"),
                    new swaydb.java.Prepare().remove(1)
            );
            assertThat(db.get(2), equalTo("two value"));
            assertThat(db.get(1), nullValue());
            //write 100 key-values atomically
            IntStream.range(1, 100).forEach(index ->
                    db.put(index, String.valueOf(index)));
            //Iteration: fetch all key-values withing range 10 to 90, update values and
            // atomically write updated key-values
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
    public void persistentMapIntStringClear() {
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2builderClear"))
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
    public void persistentMapIntStringSize() {  
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2builderSize"))
                        .withKeySerializer(Integer.class)
                        .withValueSerializer(String.class)
                        .build()) {
            db.put(1, "one");
            assertThat(db.size(), equalTo(1));
        }
    }

    @Test
    public void persistentMapIntStringIsEmpty() {  
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2builderIsEmpty"))
                        .withKeySerializer(Integer.class)
                        .withValueSerializer(String.class)
                        .build()) {
            db.put(1, "one");
            assertThat(db.isEmpty(), equalTo(false));
            assertThat(db.nonEmpty(), equalTo(true));
        }
    }

    @Test
    public void persistentMapIntStringContainsValue() {  
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2builderContainsValue"))
                        .withKeySerializer(Integer.class)
                        .withValueSerializer(String.class)
                        .build()) {
            db.put(1, "one");
            assertThat(db.containsValue("one"), equalTo(true));
        }
    }

    @Test
    public void persistentMapIntStringMightContain() {  
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2builderMightContain"))
                        .withKeySerializer(Integer.class)
                        .withValueSerializer(String.class)
                        .build()) {
            db.put(1, "one");
            assertThat(db.mightContain(1), equalTo(true));
        }
    }

    @Test
    public void persistentMapIntStringHead() {  
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2builderHead"))
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
    public void persistentMapIntStringLast() {  
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2builderLast"))
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
    public void persistentMapIntStringPutMap() {
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2builderPutAll"))
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
    public void persistentMapIntStringUpdateMap() {  
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2builderUpdateAll"))
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
    public void persistentMapIntStringKeySet() {
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                .<Integer, String>builder()
                .withDirecory(Paths.get("disk2builderKeySet"))
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            // db.put(1, "one").get
            db.put(1, "one");
            db.put(2, "two");
            // db.get(1).get
            Set<Integer> keys = db.keySet();
            assertThat(keys.toString(), equalTo("[1, 2]"));
        }
    }
    
    @Test
    public void persistentMapIntStringKeysHead() {  
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2builderKeysHead"))
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
    public void persistentMapIntStringKeysLast() {
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2builderKeysLast"))
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
    public void persistentMapIntStringValues() {
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                .<Integer, String>builder()
                .withDirecory(Paths.get("disk2builderValues"))
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            // db.put(1, "one").get
            db.put(1, "one");
            db.put(2, "two");
            // db.get(1).get
            Collection<String> keys = db.values();
            assertThat(keys.toString(), equalTo("[one, two]"));
        }
    }
    
    @Test
    public void persistentMapIntStringEntrySet() {
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2builderEntries"))
                        .withKeySerializer(Integer.class)
                        .withValueSerializer(String.class)
                        .build()) {
            // db.put(1, "one").get
            db.put(1, "one");
            db.put(2, "two");
            // db.get(1).get
            Set<Map.Entry<Integer, String>> entries = db.entrySet();
            assertThat(entries.toString(), equalTo("[1=one, 2=two]"));
        }
    }
    
    @Test
    public void persistentMapIntStringPutExpireAfter() {  
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2putExpireAfter"))
                        .withKeySerializer(Integer.class)
                        .withValueSerializer(String.class)
                        .build()) {
            db.put(1, "one", 100, TimeUnit.MILLISECONDS);
            assertThat(db.entrySet().toString(), equalTo("[1=one]"));
            await().atMost(1800, TimeUnit.MILLISECONDS).until((Callable<Boolean>) () -> {
                assertThat(db.get(1), nullValue());
                return true;
            });
        }
    }

    @Test
    public void persistentMapIntStringPutExpireAt() {  
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2putExpireAt"))
                        .withKeySerializer(Integer.class)
                        .withValueSerializer(String.class)
                        .build()) {
            LocalDateTime expireAt = LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100));
            db.put(1, "one", expireAt);
            assertThat(db.entrySet().toString(), equalTo("[1=one]"));
            await().atMost(1800, TimeUnit.MILLISECONDS).until((Callable<Boolean>) () -> {
                assertThat(db.get(1), nullValue());
                return true;
            });
        }
    }

    @Test
    public void persistentMapIntStringExpiration() {  
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2expiration"))
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
    public void persistentMapIntStringTimeLeft() {  
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2timeleft"))
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
    public void persistentMapIntStringKeySize() {
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2keysize"))
                        .withKeySerializer(Integer.class)
                        .withValueSerializer(String.class)
                        .build()) {
            db.put(1, "one");
            assertThat(db.keySize(1), equalTo(4));
        }
    }

    @Test
    public void persistentMapIntStringValueSize() {
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2valuesize"))
                        .withKeySerializer(Integer.class)
                        .withValueSerializer(String.class)
                        .build()) {
            db.put(1, "one");
            assertThat(db.valueSize("one"), equalTo(3));
        }
    }

    @Test
    public void persistentMapIntStringSizes() {
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2sizes"))
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
    public void persistentMapIntStringExpireAfter() {  
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2expireAfter"))
                        .withKeySerializer(Integer.class)
                        .withValueSerializer(String.class)
                        .build()) {
            db.put(1, "one");
            db.expire(1, 100, TimeUnit.MILLISECONDS);
            assertThat(db.entrySet().toString(), equalTo("[1=one]"));
            await().atMost(2800, TimeUnit.MILLISECONDS).until((Callable<Boolean>) () -> {
                assertThat(db.get(1), nullValue());
                return true;
            });
        }
    }

    @Test
    public void persistentMapIntStringExpireAt() {  
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2expireAt"))
                        .withKeySerializer(Integer.class)
                        .withValueSerializer(String.class)
                        .build()) {
            db.put(1, "one");
            db.expire(1, LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100)));
            assertThat(db.entrySet().toString(), equalTo("[1=one]"));
            await().atMost(1000, TimeUnit.MILLISECONDS).until((Callable<Boolean>) () -> {
                assertThat(db.get(1), nullValue());
                return true;
            });
        }
    }

    @Test
    public void persistentMapIntStringUpdate() {  
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2update"))
                        .withKeySerializer(Integer.class)
                        .withValueSerializer(String.class)
                        .build()) {
            db.put(1, "one");
            db.update(1, "one+1");
            assertThat(db.get(1), equalTo("one+1"));
        }
    }

    @Test
    public void persistentMapIntStringAsJava() {
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2asjava"))
                        .withKeySerializer(Integer.class)
                        .withValueSerializer(String.class)
                        .build()) {
            db.put(1, "one");
            assertThat(db.asJava().size(), equalTo(1));
        }
    }

    @Test
    public void persistentMapStringIntRegisterApplyFunctionUpdate() {
        try (swaydb.eventually.persistent.Map<String, Integer> likesMap = swaydb.eventually.persistent.Map
                .<String, Integer>builder()
                .withDirecory(Paths.get("disk2registerapplyfunction"))
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
    public void persistentMapIntStringFromBuilder() {
        try (swaydb.eventually.persistent.Map<Integer, String> db = swaydb.eventually.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk2builder"))
                        .withKeySerializer(Integer.class)
                        .withValueSerializer(String.class)
                        .withMaxOpenSegments(1000)
                        .withCacheSize(100000000)
                        .withMapSize(4000000)
                        .withMmapMaps(true)
                        .withRecoveryMode(RecoveryMode.ReportFailure$.MODULE$)
                        .withMmapAppendix(true)
                        .withMmapSegments(MMAP.WriteAndRead$.MODULE$)
                        .withSegmentSize(2000000)
                        .withAppendixFlushCheckpointSize(2000000)
                        .withOtherDirs(scala.collection.immutable.Nil$.MODULE$)
                        .withCacheCheckDelay(scala.concurrent.duration.FiniteDuration.apply(5, TimeUnit.SECONDS))
                        .withSegmentsOpenCheckDelay(
                                scala.concurrent.duration.FiniteDuration.apply(5, TimeUnit.SECONDS))
                        .withBloomFilterFalsePositiveRate(0.01)
                        .withCompressDuplicateValues(true)
                        .withDeleteSegmentsEventually(false)
                        .withLastLevelGroupingStrategy(scala.Option.empty())
                        .withAcceleration(swaydb.persistent.Map$.MODULE$.apply$default$18())
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
    public void persistentMapIntCustom() {

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
                return swaydb.java.Slice.create(4 + data.key.length() + 4 + data.value.length())
                        .addInt(data.key.length())
                        .addString(data.key)
                        .addInt(data.value.length())
                        .addString(data.value)
                        .close();
            }

            @Override
            public MyData read(Slice<Object> data) {
                final swaydb.java.BytesReader reader = BytesReader.create(data);
                int keyLength = reader.readInt();
                String key = reader.readString(keyLength);
                int valueLength = reader.readInt();
                String value = reader.readString(valueLength);
                return new MyData(key, value);
            }
        }

        try (swaydb.eventually.persistent.Map<Integer, MyData> db = swaydb.eventually.persistent.Map
                .<Integer, MyData>builder()
                .withDirecory(Paths.get("disk2custom"))
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
    public void persistentMapIntApacheSerializer() {

        try (swaydb.eventually.persistent.Map<Integer, MyData> db = swaydb.eventually.persistent.Map
                .<Integer, MyData>builder()
                .withDirecory(Paths.get("disk2apache"))
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
