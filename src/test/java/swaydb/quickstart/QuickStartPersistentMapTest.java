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

public class QuickStartPersistentMapTest extends TestBase {
    
    @BeforeClass
    public static void beforeClass() throws IOException {
        deleteDirectoryWalkTree(Paths.get("disk1"));
        deleteDirectoryWalkTree(Paths.get("disk1builder"));
        deleteDirectoryWalkTree(Paths.get("disk1builderClear"));
        deleteDirectoryWalkTree(Paths.get("disk1builderContainsValue"));
        deleteDirectoryWalkTree(Paths.get("disk1builderMightContain"));
        deleteDirectoryWalkTree(Paths.get("disk1builderEntries"));
        deleteDirectoryWalkTree(Paths.get("disk1builderIsEmpty"));
        deleteDirectoryWalkTree(Paths.get("disk1builderKeySet"));
        deleteDirectoryWalkTree(Paths.get("disk1builderPutAll"));
        deleteDirectoryWalkTree(Paths.get("disk1builderSize"));
        deleteDirectoryWalkTree(Paths.get("disk1builderValues"));
        deleteDirectoryWalkTree(Paths.get("disk1putExpireAfter"));
        deleteDirectoryWalkTree(Paths.get("disk1putExpireAt"));
        deleteDirectoryWalkTree(Paths.get("disk1expireAfter"));
        deleteDirectoryWalkTree(Paths.get("disk1expireAt"));
        deleteDirectoryWalkTree(Paths.get("disk1update"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void persistentMapIntString() {
        // Create a persistent database. If the directories do not exist, they will be created.
        // val db = persistent.Map[Int, String](dir = dir.resolve("disk1")).get
        try (swaydb.persistent.Map<Integer, String> db = swaydb.persistent.Map.<Integer, String>create(
                        Integer.class, String.class, Paths.get("disk1"))) {
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
        try (swaydb.persistent.Map<Integer, String> db = swaydb.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk1builderClear"))
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
        try (swaydb.persistent.Map<Integer, String> db = swaydb.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk1builderSize"))
                        .withKeySerializer(Integer.class)
                        .withValueSerializer(String.class)
                        .build()) {
            db.put(1, "one");
            assertThat(db.size(), equalTo(1));
        }
    }

    @Test
    public void persistentMapIntStringIsEmpty() {  
        try (swaydb.persistent.Map<Integer, String> db = swaydb.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk1builderIsEmpty"))
                        .withKeySerializer(Integer.class)
                        .withValueSerializer(String.class)
                        .build()) {
            db.put(1, "one");
            assertThat(db.isEmpty(), equalTo(false));
        }
    }

    @Test
    public void persistentMapIntStringContainsValue() {  
        try (swaydb.persistent.Map<Integer, String> db = swaydb.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk1builderContainsValue"))
                        .withKeySerializer(Integer.class)
                        .withValueSerializer(String.class)
                        .build()) {
            db.put(1, "one");
            assertThat(db.containsValue("one"), equalTo(true));
        }
    }

    @Test
    public void persistentMapIntStringMightContain() {  
        try (swaydb.persistent.Map<Integer, String> db = swaydb.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk1builderMightContain"))
                        .withKeySerializer(Integer.class)
                        .withValueSerializer(String.class)
                        .build()) {
            db.put(1, "one");
            assertThat(db.mightContain(1), equalTo(true));
        }
    }

    @Test
    public void persistentMapIntStringHead() {  
        try (swaydb.persistent.Map<Integer, String> db = swaydb.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk1builderHead"))
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
        try (swaydb.persistent.Map<Integer, String> db = swaydb.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk1builderLast"))
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
    public void persistentMapIntStringPutAll() {  
        try (swaydb.persistent.Map<Integer, String> db = swaydb.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk1builderPutAll"))
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
    public void persistentMapIntStringisUpdateAll() {  
        try (swaydb.persistent.Map<Integer, String> db = swaydb.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk1builderUpdateAll"))
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
    public void persistentMapIntStringKeySet() {
        try (swaydb.persistent.Map<Integer, String> db = swaydb.persistent.Map
                .<Integer, String>builder()
                .withDirecory(Paths.get("disk1builderKeySet"))
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
        try (swaydb.persistent.Map<Integer, String> db = swaydb.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk1builderKeysHead"))
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
        try (swaydb.persistent.Map<Integer, String> db = swaydb.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk1builderKeysLast"))
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
        try (swaydb.persistent.Map<Integer, String> db = swaydb.persistent.Map
                .<Integer, String>builder()
                .withDirecory(Paths.get("disk1builderValues"))
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
        try (swaydb.persistent.Map<Integer, String> db = swaydb.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk1builderEntries"))
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
        try (swaydb.persistent.Map<Integer, String> db = swaydb.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk1putExpireAfter"))
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
        try (swaydb.persistent.Map<Integer, String> db = swaydb.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk1putExpireAt"))
                        .withKeySerializer(Integer.class)
                        .withValueSerializer(String.class)
                        .build()) {
            LocalDateTime expireAt = LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100));
            db.put(1, "one", expireAt);
            assertThat(db.expiration(1).truncatedTo(ChronoUnit.SECONDS).toString(),
                    equalTo(expireAt.truncatedTo(ChronoUnit.SECONDS).toString()));
            assertThat(db.entrySet().toString(), equalTo("[1=one]"));
            await().atMost(1800, TimeUnit.MILLISECONDS).until((Callable<Boolean>) () -> {
                assertThat(db.get(1), nullValue());
                return true;
            });
        }
    }

    @Test
    public void persistentMapIntStringExpireAfter() {  
        try (swaydb.persistent.Map<Integer, String> db = swaydb.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk1expireAfter"))
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
    public void persistentMapIntStringExpireAt() {  
        try (swaydb.persistent.Map<Integer, String> db = swaydb.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk1expireAt"))
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
        try (swaydb.persistent.Map<Integer, String> db = swaydb.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk1update"))
                        .withKeySerializer(Integer.class)
                        .withValueSerializer(String.class)
                        .build()) {
            db.put(1, "one");
            db.update(1, "one+1");
            assertThat(db.get(1), equalTo("one+1"));
        }
    }

    @Test
    public void persistentMapIntStringFromBuilder() {
        try (swaydb.persistent.Map<Integer, String> db = swaydb.persistent.Map
                        .<Integer, String>builder()
                        .withDirecory(Paths.get("disk1builder"))
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
            // db.remove(1).get
            db.remove(1);
            String result2 = db.get(1);
            assertThat("Empty result", result2, nullValue());
        }
    }
    
}