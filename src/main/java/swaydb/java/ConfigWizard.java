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

import java.nio.file.Path;
import scala.runtime.AbstractFunction1;
import swaydb.data.accelerate.Accelerator;
import swaydb.data.accelerate.Level0Meter;
import swaydb.data.config.ConfigWizard$;
import swaydb.data.config.LevelZeroPersistentConfig;
import swaydb.data.config.RecoveryMode;

public class ConfigWizard {

    public static LevelZeroPersistentConfig addPersistentLevel0(int mapSize, Path directory,
            boolean mmap, RecoveryMode recoveryMode, AbstractFunction1<Level0Meter, Accelerator> acceleration) {
        return ConfigWizard$.MODULE$.addPersistentLevel0(mapSize, directory, mmap, recoveryMode, acceleration);
    }
}
