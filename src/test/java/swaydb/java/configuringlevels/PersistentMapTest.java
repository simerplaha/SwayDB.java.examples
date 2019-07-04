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
package swaydb.java.configuringlevels;

import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.ExecutionContext;
import scala.concurrent.duration.FiniteDuration;
import scala.runtime.AbstractFunction1;
import swaydb.base.TestBase;
import swaydb.data.accelerate.Accelerator;
import swaydb.data.accelerate.LevelZeroMeter;
import swaydb.data.config.LevelZeroPersistentConfig;
import swaydb.data.order.KeyOrder;
import swaydb.java.ConfigWizard;
import swaydb.java.Duration;
import swaydb.java.RecoveryMode;
import swaydb.java.StorageDoubleImplicits;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

@SuppressWarnings({"checkstyle:JavadocMethod", "checkstyle:JavadocType"})
public class PersistentMapTest extends TestBase {

    @BeforeClass
    public static void beforeClass() throws IOException {
        deleteDirectoryWalkTree(addTarget(Paths.get("Disk1/myDB")));
        deleteDirectoryWalkTree(addTarget(Paths.get("Disk2/myDB")));
        deleteDirectoryWalkTree(addTarget(Paths.get("Disk3/myDB")));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void persistentMapIntStringConfig() {
        LevelZeroPersistentConfig config = ConfigWizard.addPersistentLevel0(
                StorageDoubleImplicits.mb(4.0),
                addTarget(Paths.get("Disk1/myDB")),
                true,
                null,
                RecoveryMode.ReportFailure.get(),
                new AbstractFunction1<LevelZeroMeter, Accelerator>() {
                    @Override
                    public Accelerator apply(LevelZeroMeter level0Meter) {
                        return swaydb.java.Accelerator.cruise(level0Meter);
                    }
                },
                new AbstractFunction1<LevelZeroMeter, FiniteDuration>() {
                    @Override
                    public FiniteDuration apply(LevelZeroMeter level0Meter) {
                        return Duration.of(5, TimeUnit.SECONDS);
                    }
                });
//                .addMemoryLevel1(
//                        StorageDoubleImplicits.mb(4.0),
//                        false,
//                      false,
//                        0.1,
//                        true,
//                        true,
//                        (Option) scala.None$.MODULE$
//                        )
//                .addPersistentLevel1(addTarget(Paths.get("Disk1/myDB")),
//                        (Seq) Predef$.MODULE$.wrapRefArray((Object[]) new Dir[]{
//                    swaydb.package$.MODULE$.pathStringToDir("target/Disk2/myDB"),
//                    swaydb.package$.MODULE$.pathStringToDir("target/Disk3/myDB")}),
//                        StorageDoubleImplicits.mb(4.0),
//                        MMAP.WriteAndRead$.MODULE$, true, 0, true, 0, true, true, (Option) scala.None$.MODULE$,
//                        new AbstractFunction1<LevelMeter, Throttle>() {
//                            public static final long serialVersionUID = 0L;
//
//                            public final Throttle apply(LevelMeter levelMeter) {
//                                return levelMeter.segmentsCount() > 100 ? new swaydb.data.compaction.Throttle(
//                                             Duration.zero(), 10)
//                                        : new swaydb.data.compaction.Throttle(
//                                             Duration.zero(), 0);
//                            }
//                        });
        KeyOrder ordering = swaydb.data.order.KeyOrder$.MODULE$.lexicographic();
        ExecutionContext ec = swaydb.SwayDB$.MODULE$.defaultExecutionContext();

//        try (swaydb.java.persistent.Map<Integer, String> db = new swaydb.java.persistent.Map<>(
//              (swaydb.Map<Integer, String, swaydb.data.IO>)
//                    swaydb.SwayDB$.MODULE$.apply(config, 1000, StorageDoubleImplicits.gb(1.0),
//                          Duration.of(5, TimeUnit.SECONDS),
//                          Duration.of(5, TimeUnit.SECONDS),
//                          Serializer.classToType(Integer.class),
//                          Serializer.classToType(String.class),
//                          ordering, ec).get())) {
//            db.put(1, "one");
//            // db.get(1).get
//            String result = db.get(1);
//            assertThat(result, equalTo("one"));
//            // db.remove(1).get
//            db.remove(1);
//            String result2 = db.get(1);
//            assertThat("Empty result", result2, nullValue());
//        }
    }
}
