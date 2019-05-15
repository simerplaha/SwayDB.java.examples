package quickstart;

import java.util.AbstractMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

public class QuickStartTest {

    @Test
    public void quickStart() {
        // Create a memory database
        swaydb.java.Map<Integer, String> map = swaydb.java.memory.Map.create(Integer.class, String.class);

        map.put(1, "one");
        map.get(1);
        map.remove(1);

        // write 100 key-values atomically
        map.put(IntStream.rangeClosed(1, 100)
            .mapToObj(index -> new AbstractMap.SimpleEntry<>(index, String.valueOf(index)))
            .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue())));

        // Iteration: fetch all key-values withing range 10 to 90,
        // update values and atomically write updated key-values
        map
            .from(10)
            .takeWhile(item -> item.getKey() <= 90)
            .map(item -> new AbstractMap.SimpleEntry<>(item.getKey(), item.getValue() + "_updated"))
            .materialize().foreach(map::put);
    }
}
