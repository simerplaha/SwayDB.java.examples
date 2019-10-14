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

import java.util.AbstractMap;
import java.util.stream.IntStream;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import org.junit.Test;
import swaydb.data.util.Functions;
import swaydb.java.MapIO;
import static swaydb.java.serializers.Default.intSerializer;
import static swaydb.java.serializers.Default.stringSerializer;


@SuppressWarnings({"checkstyle:JavadocMethod", "checkstyle:JavadocType"})
public class QuickStartMemoryMapTest {

    @SuppressWarnings("unchecked")
    @Test
    public void memoryMapIntStringFrom() {
        // Create a memory database
        // val db = memory.Map[Int, String]().get
        MapIO<Integer, String, Functions.Disabled> db =
            swaydb.java.memory.Map
            .config(intSerializer(), stringSerializer())
            .create()
            .get();

        // db.put(1, "one").get
        db.put(1, "one");
        // db.get(1).get
        String result = db.get(1).get().get();
        assertThat("result contains value", result, notNullValue());
//        assertThat("Key 1 is present", db.contains(1), equalTo(true));
        assertThat(result, equalTo("one"));
        // db.remove(1).get
        db.remove(1);
        String result2 = db.get(1).get().orElse(null);
        assertThat("Empty result", result2, nullValue());
        // db.put(1, "one value").get
        db.put(1, "one value");

//            db.commit(
//                    new swaydb.java.Prepare<Integer, String>().put(2, "two value"),
//                    new swaydb.java.Prepare().remove(1)
//            );
//
//            assertThat(db.get(2), equalTo("two value"));
//            assertThat(db.get(1), nullValue());

        // write 100 key-values atomically
//            db.put(IntStream.rangeClosed(1, 100)
//                    .mapToObj(index -> new AbstractMap.SimpleEntry<>(index, String.valueOf(index)))
//                    .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue())));

        // Iteration: fetch all key-values withing range 10 to 90, update values
        // and atomically write updated key-values
        db
                .from(10)
                .takeWhile(item -> item.key() <= 90)
                .map(item -> new AbstractMap.SimpleEntry<>(item.key(), item.value() + "_updated"))
                .materialize().forEach(integerStringEntry -> {
                    integerStringEntry.forEach((item) -> {
                        db.put(item.getKey(), item.getValue()).get();
                    });
                });
        // assert the key-values were updated
        IntStream.rangeClosed(10, 90)
                .mapToObj(item -> new AbstractMap.SimpleEntry<>(item, db.get(item)))
                .forEach(pair -> {
                    assertThat(pair.getValue().get().orElse("").endsWith("_updated"), equalTo(true));
                });
//            db.close();
    }
}
