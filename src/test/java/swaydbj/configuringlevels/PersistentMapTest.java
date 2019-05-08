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
package swaydbj.configuringlevels;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Option;
import scala.Predef$;
import scala.collection.Seq;
import scala.concurrent.ExecutionContext;
import scala.runtime.AbstractFunction1;
import swaydbj.base.TestBase;
import swaydb.data.accelerate.Accelerator;
import swaydb.data.accelerate.Level0Meter;
import swaydb.data.compaction.LevelMeter;
import swaydb.data.compaction.Throttle;
import swaydb.data.config.Dir;
import swaydb.data.config.MMAP;
import swaydb.data.config.SwayDBPersistentConfig;
import swaydb.data.order.KeyOrder;
import swaydbj.java.*;

public class PersistentMapTest extends TestBase {

    @BeforeClass
    public static void beforeClass() throws IOException {
        deleteDirectoryWalkTree(Paths.get("Disk1/myDB"));
        deleteDirectoryWalkTree(Paths.get("Disk2/myDB"));
        deleteDirectoryWalkTree(Paths.get("Disk3/myDB"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void persistentMapIntStringConfig() {
        SwayDBPersistentConfig config = ConfigWizard.addPersistentLevel0(
                StorageDoubleImplicits.mb(4.0),
                Paths.get("Disk1/myDB"),
                true,
                RecoveryMode.ReportFailure.get(),
                new AbstractFunction1<Level0Meter, Accelerator>() {
                    @Override
                    public Accelerator apply(Level0Meter level0Meter) {
                        return swaydbj.java.Accelerator.cruise(level0Meter);
                    }
                })
                .addMemoryLevel1(
                        StorageDoubleImplicits.mb(4.0),
                        false,
                        0.1,
                        true,
                        true,
                        (Option) scala.None$.MODULE$,
                        new AbstractFunction1<LevelMeter, Throttle>() {
                            @Override
                            public final Throttle apply(LevelMeter levelMeter) {
                                return levelMeter.levelSize()
                                        > StorageDoubleImplicits.gb(1.0)
                                        ? new swaydb.data.compaction.Throttle(
                                             Duration.zero(), 10)
                                        : new swaydb.data.compaction.Throttle(
                                             Duration.zero(), 0);
                            }
                    })
                .addPersistentLevel(Paths.get("Disk1/myDB"),
                        (Seq) Predef$.MODULE$.wrapRefArray((Object[]) new Dir[]{
                    swaydb.package$.MODULE$.pathStringToDir("Disk2/myDB"),
                    swaydb.package$.MODULE$.pathStringToDir("Disk3/myDB")}),
                        StorageDoubleImplicits.mb(4.0),
                        MMAP.WriteAndRead$.MODULE$, true, 0, true, 0, true, true, (Option) scala.None$.MODULE$,
                        new AbstractFunction1<LevelMeter, Throttle>() {
                            public static final long serialVersionUID = 0L;

                            public final Throttle apply(LevelMeter levelMeter) {
                                return levelMeter.segmentsCount() > 100 ? new swaydb.data.compaction.Throttle(
                                             Duration.zero(), 10)
                                        : new swaydb.data.compaction.Throttle(
                                             Duration.zero(), 0);
                            }
                        });
        KeyOrder ordering = (KeyOrder) swaydb.data.order.KeyOrder$.MODULE$.reverse();
        ExecutionContext ec = swaydb.SwayDB$.MODULE$.defaultExecutionContext();

        try (swaydbj.persistent.Map<Integer, String> db = new swaydbj.persistent.Map<>(
              (swaydb.Map<Integer, String, swaydb.data.IO>)
                    swaydb.SwayDB$.MODULE$.apply(config, 1000, StorageDoubleImplicits.gb(1.0),
                          Duration.of(5, TimeUnit.SECONDS),
                          Duration.of(5, TimeUnit.SECONDS),
                          Serializer.classToType(Integer.class),
                          Serializer.classToType(String.class),
                          ordering, ec).get())) {
            db.put(1, "one");
            // db.get(1).get
            String result = db.get(1);
            assertThat(result, equalTo("one"));
            // db.remove(1).get
            db.remove(1);
            String result2 = db.get(1);
            assertThat("Empty result", result2, nullValue());
        }
    }
}
