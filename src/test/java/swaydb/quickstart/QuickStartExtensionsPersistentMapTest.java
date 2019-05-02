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
import java.util.concurrent.TimeUnit;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.BeforeClass;
import org.junit.Test;
import swaydb.base.TestBase;
import swaydb.data.config.MMAP;
import swaydb.data.config.RecoveryMode;

public class QuickStartExtensionsPersistentMapTest extends TestBase {

    @BeforeClass
    public static void beforeClass() throws IOException {
        deleteDirectoryWalkTreeStartsWith("disk5");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void persistentMapIntStringFrom() {
        // Create a persistent database. If the directories do not exist, they will be created.
        // val db = persistent.Map[Int, String](dir = dir.resolve("disk1From")).get
        try (swaydb.extensions.persistent.Map<Integer, String> db = swaydb.extensions.persistent.Map.create(
                Integer.class, String.class, Paths.get("disk5From"))) {
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
                    new swaydb.java.Prepare<Integer, String>().put(2, "two value"),
                    new swaydb.java.Prepare().remove(1)
            );

            assertThat(db.get(2), equalTo("two value"));
            assertThat(db.get(1), nullValue());
        }
    }
    
    @Test
    public void persistentMapIntStringClear() {
        // Create a persistent database        
        try (swaydb.extensions.persistent.Map<Integer, String> db = swaydb.extensions.persistent.Map
                .<Integer, String>builder()
                .withDir(Paths.get("disk5builderClear"))
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
    public void persistentMapIntStringSize() {
        try (swaydb.extensions.persistent.Map<Integer, String> db = swaydb.extensions.persistent.Map
                .<Integer, String>builder()
                .withDir(Paths.get("disk5builderSize"))
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
    public void persistentMapIntStringIsEmpty() {
        try (swaydb.extensions.persistent.Map<Integer, String> db = swaydb.extensions.persistent.Map
                .<Integer, String>builder()
                .withDir(Paths.get("disk5builderIsEmpty"))
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            assertThat(db.isEmpty(), equalTo(false));
            assertThat(db.nonEmpty(), equalTo(true));
        }
    }

    @Test
    public void persistentMapIntStringMightContain() {
        try (swaydb.extensions.persistent.Map<Integer, String> db = swaydb.extensions.persistent.Map
                .<Integer, String>builder()
                .withDir(Paths.get("disk5builderMightContain"))
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            db.put(1, "one");
            assertThat(db.mightContain(1), equalTo(true));
        }
    }
    
    @Test
    public void persistentMapIntStringHead() {
        try (swaydb.extensions.persistent.Map<Integer, String> db = swaydb.extensions.persistent.Map
                .<Integer, String>builder()
                .withDir(Paths.get("disk5builderHead"))
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
        try (swaydb.extensions.persistent.Map<Integer, String> db = swaydb.extensions.persistent.Map
                .<Integer, String>builder()
                .withDir(Paths.get("disk5builderLast"))
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
    public void persistentMapIntStringMaps() {
        try (swaydb.extensions.persistent.Map<String, String> rootMap = swaydb.extensions.persistent.Map
                .<String, String>builder()
                .withDir(Paths.get("disk5builderMaps"))
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
    public void persistentMapIntStringFromBuilder() {
        try (swaydb.extensions.persistent.Map<Integer, String> db = swaydb.extensions.persistent.Map
                        .<Integer, String>builder()                        
                        .withDir(Paths.get("disk5builder"))
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

}
