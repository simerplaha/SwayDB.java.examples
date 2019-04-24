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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import org.junit.Test;

public class QuickStartextensionsMemoryMapTest {

    @SuppressWarnings("unchecked")
    @Test
    public void memoryMapIntStringFrom() {
        // Create a memory database        
        // val db = memory.Map[Int, String]().get
        try (swaydb.extensions.memory.Map<Integer, String> db = swaydb.extensions.memory.Map.create(
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
                    new swaydb.java.Prepare<Integer, String>().put(2, "two value"),
                    new swaydb.java.Prepare().remove(1)
            );

            assertThat(db.get(2), equalTo("two value"));
            assertThat(db.get(1), nullValue());
        }
    }
    
    @Test
    public void memoryMapIntStringClear() {
        // Create a memory database        
        try (swaydb.extensions.memory.Map<Integer, String> db = swaydb.extensions.memory.Map
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
        try (swaydb.extensions.memory.Map<Integer, String> db = swaydb.extensions.memory.Map
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
        try (swaydb.extensions.memory.Map<Integer, String> db = swaydb.extensions.memory.Map
                .<Integer, String>builder()
                .withKeySerializer(Integer.class)
                .withValueSerializer(String.class)
                .build()) {
            assertThat(db.isEmpty(), equalTo(false));
            assertThat(db.nonEmpty(), equalTo(true));
        }
    }

}
