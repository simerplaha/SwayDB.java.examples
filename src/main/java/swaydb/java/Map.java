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

import java.util.function.Function;
import swaydb.data.accelerate.Level0Meter;

public interface Map<K, V> {

    V put(K key, V value);
    
    int size();
    
    boolean isEmpty();
    
    V get(K key);
    
    V remove(K key);
    
    K registerFunction(K functionId, Function<V, swaydb.Apply.Map<V>> function);
    
    void applyFunction(K key, K functionId);
    
    @SuppressWarnings("unchecked")
    Level0Meter commit(swaydb.Prepare<K, V>... prepares);
}
