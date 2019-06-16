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

import scala.Predef$;
import scala.collection.Seq;
import swaydb.data.config.Dir;

import java.nio.file.Path;

/**
 * The Dirs wrapper.
 */
public class Dirs {

    /**
     * Creates the dir sequence.
     * @param paths the paths
     *
     * @return the dir sequence
     */
    public static Seq<Dir> create(Path... paths) {
        Dir[] dirs = new Dir[paths.length];
        int index = 0;
        for (Path path : paths) {
            dirs[index] = swaydb.package$.MODULE$.pathStringToDir(path.toFile().getAbsolutePath());
            index += 1;
        }
        return Predef$.MODULE$.wrapRefArray(dirs);
    }
}
