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

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import scala.concurrent.duration.FiniteDuration;

/**
 * The Apply wrapper.
 */
public class Apply {

    /**
     * Returns the Map object for update.
     * @param <V> the type of the value element
     * @param value the value
     *
     * @return the Map object for update
     */
    public static <V> swaydb.Apply.Map<V> update(V value) {
        return swaydb.Apply.Update$.MODULE$.apply(value);
    }

    /**
     * Returns the Map object for expire.
     * @param <V> the type of the value element
     * @param expireAt the expireAt
     *
     * @return the Map object for expire
     */
    @SuppressWarnings("unchecked")
    public static <V> swaydb.Apply.Map<V> expire(LocalDateTime expireAt) {
        int expireAtNano = Duration.between(LocalDateTime.now(), expireAt).getNano();
        return (swaydb.Apply.Map<V>) swaydb.Apply.Expire$.MODULE$.apply(
                FiniteDuration.create(expireAtNano, TimeUnit.NANOSECONDS).fromNow());
    }

    /**
     * Returns the Map object for remove.
     * @param <V> the type of the value element
     *
     * @return the Map object for remove
     */
    @SuppressWarnings("unchecked")
    public static <V> swaydb.Apply.Map<V> remove() {
        return (swaydb.Apply.Map<V>) swaydb.Apply.Remove$.MODULE$;
    }

    /**
     * Returns the Map object for nothing.
     * @param <V> the type of the value element
     *
     * @return the Map object for nothing
     */
    @SuppressWarnings("unchecked")
    public static <V> swaydb.Apply.Map<V> nothing() {
        return (swaydb.Apply.Map<V>) swaydb.Apply.Nothing$.MODULE$;
    }
}
