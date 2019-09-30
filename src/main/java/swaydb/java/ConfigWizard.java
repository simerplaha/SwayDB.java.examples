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
package swaydb.java;

import scala.concurrent.duration.FiniteDuration;
import scala.runtime.AbstractFunction1;
import swaydb.data.accelerate.Accelerator;
import swaydb.data.accelerate.LevelZeroMeter;
import swaydb.data.compaction.CompactionExecutionContext;
import swaydb.data.config.ConfigWizard$;
import swaydb.data.config.LevelZeroPersistentConfig;
import swaydb.data.config.RecoveryMode;

import java.nio.file.Path;

/**
 * The ConfigWizard wrapper.
 */
public class ConfigWizard {

    /**
     * Returns the LevelZeroPersistentConfig object.
     * @param mapSize the mapSize
     * @param directory the directory
     * @param mmap the mmap
     * @param compactionExecutionContext the compactionExecutionContext
     * @param recoveryMode the recoveryMode
     * @param acceleration the acceleration
     * @param throttle the throttle
     *
     * @return the LevelZeroPersistentConfig object
     */
    public static LevelZeroPersistentConfig addPersistentLevel0(int mapSize, Path directory,
            boolean mmap, CompactionExecutionContext.Create compactionExecutionContext, RecoveryMode recoveryMode,
          AbstractFunction1<LevelZeroMeter, Accelerator> acceleration,
          AbstractFunction1<LevelZeroMeter, FiniteDuration> throttle) {
        return ConfigWizard$.MODULE$.addPersistentLevel0(directory, mapSize, mmap, recoveryMode,
              compactionExecutionContext, acceleration, throttle);
    }
}
