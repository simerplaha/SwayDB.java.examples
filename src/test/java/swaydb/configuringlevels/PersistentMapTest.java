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
package swaydb.configuringlevels;

import java.nio.file.Paths;
import org.junit.Test;
import scala.Option;
import scala.runtime.AbstractFunction1;
import swaydb.data.accelerate.Accelerator;
import swaydb.data.accelerate.Level0Meter;
import swaydb.data.compaction.LevelMeter;
import swaydb.data.compaction.Throttle;
import swaydb.data.config.RecoveryMode;
import swaydb.data.config.SwayDBPersistentConfig;
import swaydb.java.ConfigWizard;

public class PersistentMapTest {

    @SuppressWarnings("unchecked")
    @Test
    public void persistentMapIntStringConfig() {
        SwayDBPersistentConfig config = ConfigWizard.addPersistentLevel0(
                swaydb.package$.MODULE$.StorageDoubleImplicits(4.0).mb(),
                Paths.get("Disk1/myDB"),
                true,
                RecoveryMode.ReportFailure$.MODULE$,
                new AbstractFunction1<Level0Meter, Accelerator>() {
                    @Override
                    public Accelerator apply(Level0Meter level0Meter) {
                        return swaydb.data.accelerate.Accelerator$.MODULE$.cruise(level0Meter);
                    }
                })
                .addMemoryLevel1(
                        swaydb.package$.MODULE$.StorageDoubleImplicits(4.0).mb(),
                        false,
                        0.1,
                        true,
                        true,
                        (Option) scala.None$.MODULE$,
                        new AbstractFunction1<LevelMeter, Throttle>(){
                            @Override
                            public final Throttle apply(LevelMeter levelMeter) {
                                return levelMeter.levelSize()
                                        > swaydb.package$.MODULE$.StorageDoubleImplicits(1.0).gb()
                                        ? new swaydb.data.compaction.Throttle(
                                                scala.concurrent.duration.Duration$.MODULE$.Zero(), 10)
                                        : new swaydb.data.compaction.Throttle(
                                                scala.concurrent.duration.Duration$.MODULE$.Zero(), 0);
                            }
                        });
    }
}
