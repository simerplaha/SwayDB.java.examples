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

import java.util.concurrent.TimeUnit;

/**
 * The Duration wrapper.
 */
public class Duration {
    /**
     * Returns the FiniteDuration object.
     * @param length the length
     * @param unit the unit
     *
     * @return the FiniteDuration object
     */
    public static FiniteDuration of(long length, TimeUnit unit) {
        return scala.concurrent.duration.Duration$.MODULE$.apply(length, unit);
    }

    /**
     * Returns the FiniteDuration zero object.
     *
     * @return the FiniteDuration zero object
     */
    public static FiniteDuration zero() {
        return scala.concurrent.duration.Duration$.MODULE$.Zero();
    }
}
