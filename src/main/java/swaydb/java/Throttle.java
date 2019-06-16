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

import scala.Function1;
import scala.runtime.AbstractFunction1;
import swaydb.data.compaction.LevelMeter;

import java.util.function.Function;

/**
 * The Throttle function wrapper.
 */
public class Throttle {

    /**
     * Creates the scala throttle function.
     * @param function the throttle function
     *
     * @return the scala throttle function
     */
    public static Function1<LevelMeter, swaydb.data.compaction.Throttle> create(
            Function<LevelMeter, swaydb.data.compaction.Throttle> function) {
        return new AbstractFunction1<LevelMeter, swaydb.data.compaction.Throttle>() {
            @Override
            public final swaydb.data.compaction.Throttle apply(LevelMeter levelMeter) {
                return function.apply(levelMeter);
            }
        };
    }
}
